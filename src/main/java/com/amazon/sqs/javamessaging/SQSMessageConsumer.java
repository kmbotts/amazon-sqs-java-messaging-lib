/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A client uses a MessageConsumer object to receive messages from a
 * destination. A MessageConsumer object is created by passing a Destination
 * object to a message-consumer creation method supplied by a session.
 * <p>
 * This message consumer does not support message selectors
 * <p>
 * A client may either synchronously receive a message consumer's messages or
 * have the consumer asynchronously deliver them as they arrive via registering
 * a MessageListener object.
 * <p>
 * The message consumer creates a background thread to prefetch the messages to
 * improve the <code>receive</code> turn-around times.
 */
public class SQSMessageConsumer<SQS_CLIENT extends AmazonSQS> implements QueueReceiver {
    private static final Log LOG = LogFactory.getLog(SQSMessageConsumer.class);
    public static final int PREFETCH_EXECUTOR_GRACEFUL_SHUTDOWN_TIME = 30;

    protected volatile boolean closed = false;

    private final SQSQueueDestination sqsDestination;

    private final AbstractSession<SQS_CLIENT> parentSQSSession;

    private final SQSSessionCallbackScheduler<SQS_CLIENT> sqsSessionRunnable;

    /**
     * Executor for prefetch thread.
     */
    private final ExecutorService prefetchExecutor;

    /**
     * Prefetch Runnable. This include keeping internal message buffer filled and call MessageListener if set.
     */
    private final SQSMessageConsumerPrefetch<SQS_CLIENT> sqsMessageConsumerPrefetch;

    SQSMessageConsumer(AbstractConnection<SQS_CLIENT> parentSQSConnection,
                       AbstractSession<SQS_CLIENT> parentSQSSession,
                       SQSSessionCallbackScheduler<SQS_CLIENT> sqsSessionRunnable,
                       SQSQueueDestination destination,
                       Acknowledger acknowledger,
                       NegativeAcknowledger<SQS_CLIENT> negativeAcknowledger,
                       ThreadFactory threadFactory) {

        this(parentSQSConnection, parentSQSSession,
                sqsSessionRunnable, destination,
                acknowledger, negativeAcknowledger, threadFactory,
                new SQSMessageConsumerPrefetch<>(sqsSessionRunnable, acknowledger, negativeAcknowledger, destination,
                        parentSQSConnection.getWrappedAmazonSQSClient(),
                        parentSQSConnection.getNumberOfMessagesToPrefetch()));

    }

    SQSMessageConsumer(AbstractConnection<SQS_CLIENT> parentSQSConnection,
                       AbstractSession<SQS_CLIENT> parentSQSSession,
                       SQSSessionCallbackScheduler<SQS_CLIENT> sqsSessionRunnable,
                       SQSQueueDestination destination,
                       Acknowledger acknowledger,
                       NegativeAcknowledger<SQS_CLIENT> negativeAcknowledger,
                       ThreadFactory threadFactory,
                       SQSMessageConsumerPrefetch<SQS_CLIENT> sqsMessageConsumerPrefetch) {

        this.parentSQSSession = parentSQSSession;
        this.sqsDestination = destination;
        this.sqsSessionRunnable = sqsSessionRunnable;
        this.sqsMessageConsumerPrefetch = sqsMessageConsumerPrefetch;
        this.sqsMessageConsumerPrefetch.setMessageConsumer(this);

        prefetchExecutor = Executors.newSingleThreadExecutor(threadFactory);
        prefetchExecutor.execute(sqsMessageConsumerPrefetch);
    }


    /**
     * Gets the queue destination associated with this queue receiver, where the
     * messages are delivered from.
     *
     * @return a queue destination
     */
    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) sqsDestination;
    }

    /**
     * Gets the message consumer's MessageListener.
     *
     * @return a message listener
     */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        return sqsMessageConsumerPrefetch.getMessageListener();
    }

    /**
     * Sets the message consumer's MessageListener.
     *
     * @param listener a message listener to use for asynchronous message delivery
     * @throws JMSException If the message consumer is closed
     */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.sqsMessageConsumerPrefetch.setMessageListener(listener);
    }

    /**
     * This call blocks indefinitely until a message is produced or until this
     * message consumer is closed. When ConnectionState is stopped receive is
     * paused.
     *
     * @return the next message produced for this message consumer, or null if
     * this message consumer is closed during the receive call
     * @throws JMSException On internal error
     */
    @Override
    public Message receive() throws JMSException {
        checkClosed();
        return sqsMessageConsumerPrefetch.receive();
    }

    /**
     * This call blocks until a message arrives, the timeout expires, or this
     * message consumer is closed. A timeout of zero never expires, and the call
     * blocks indefinitely.
     *
     * @param timeout the timeout value (in milliseconds)
     * @return the next message produced for this message consumer, or null if
     * the timeout expires or this message consumer is closed during the receive call
     * @throws JMSException On internal error
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        return sqsMessageConsumerPrefetch.receive(timeout);
    }

    /**
     * Receives the next message if one is immediately available.
     *
     * @return the next message produced for this message consumer, or null if
     * no message is available
     * @throws JMSException On internal error
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        return sqsMessageConsumerPrefetch.receiveNoWait();
    }

    /**
     * Closes the message consumer.
     * <p>
     * This will not return until receives and/or message listeners in progress
     * have completed. A blocked message consumer receive call returns null when
     * this consumer is closed.
     * <p>
     * Since consumer prefetch threads use SQS long-poll feature with 20 seconds
     * timeout, closing each consumer prefetch thread can take up to 20 seconds,
     * which in-turn will impact the time on consumer close.
     * <p>
     * This method may be called from a message listener's onMessage method on
     * its own consumer. After this method returns the onMessage method will be
     * allowed to complete normally, and the callback scheduler thread will be
     * closing the message consumer.
     *
     * @throws JMSException On internal error.
     */
    @Override
    public void close() throws JMSException {
        if (closed) {
            return;
        }

        if (parentSQSSession.isActiveCallbackSessionThread()) {
            sqsSessionRunnable.setConsumerCloseAfterCallback(this);
            return;
        }

        doClose();
    }

    void doClose() {
        if (closed) {
            return;
        }

        sqsMessageConsumerPrefetch.close();

        parentSQSSession.removeConsumer(this);

        try {
            if (!prefetchExecutor.isShutdown()) {
                LOG.info("Shutting down " + SQSSession.CONSUMER_PREFETCH_EXECUTOR_NAME + " executor");
                /** Shut down executor. */
                prefetchExecutor.shutdown();
            }

            parentSQSSession.waitForConsumerCallbackToComplete(this);

            if (!prefetchExecutor.awaitTermination(PREFETCH_EXECUTOR_GRACEFUL_SHUTDOWN_TIME, TimeUnit.SECONDS)) {

                LOG.warn("Can't terminate executor service " + SQSSession.CONSUMER_PREFETCH_EXECUTOR_NAME +
                        " after " + PREFETCH_EXECUTOR_GRACEFUL_SHUTDOWN_TIME +
                        " seconds, some running threads will be shutdown immediately");
                prefetchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while closing the consumer.", e);
        }

        closed = true;
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * This method is not supported.
     */
    @Override
    public String getMessageSelector() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * This stops the prefetching
     */
    protected void stopPrefetch() {
        if (!closed) {
            sqsMessageConsumerPrefetch.stop();
        }
    }

    /**
     * This starts the prefetching
     */
    protected void startPrefetch() {
        if (!closed) {
            sqsMessageConsumerPrefetch.start();
        }
    }

    private void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
    }

    List<SQSMessageIdentifier> purgePrefetchedMessagesWithGroups(Set<String> affectedGroups) throws JMSException {
        return sqsMessageConsumerPrefetch.purgePrefetchedMessagesWithGroups(affectedGroups);
    }
}
