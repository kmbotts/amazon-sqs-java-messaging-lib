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
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageListener;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Used internally to prefetch messages to internal buffer on a background
 * thread for better <code>receive</code> turn around times.
 * <p>
 * Each message consumer creates one prefetch thread.
 * <p>
 * This runs until the message consumer is closed and in-progress SQS
 * <code>receiveMessage</code> call returns.
 * <p>
 * Uses SQS <code>receiveMessage</code> with long-poll wait time of 20 seconds.
 * <p>
 * Add re-tries on top of <code>AmazonSQSClient</code> re-tries on SQS calls.
 */
public class SQSMessageConsumerPrefetch implements Runnable, PrefetchManager {

    private static final Log LOG = LogFactory.getLog(SQSMessageConsumerPrefetch.class);

    protected static final int WAIT_TIME_SECONDS = 20;

    protected static final String ALL = "All";

    private final AbstractSQSClientWrapper sqsClientWrapper;

    private final String queueUrl;

    /**
     * Maximum number messages, which MessageConsumer can prefetch
     */
    private final int numberOfMessagesToPrefetch;

    private final SQSQueueDestination sqsDestination;

    /**
     * Internal buffer of Messages. The size of queue is MIN_BATCH by default
     * and it can be changed by user.
     */
    protected final Deque<FetchedMessage> messageQueue;

    private final Acknowledger acknowledger;

    private final NegativeAcknowledger negativeAcknowledger;

    private volatile MessageListener messageListener;

    private AbstractMessageConsumer messageConsumer;

    private final SQSSessionCallbackScheduler callbackScheduler;

    /**
     * Counter on how many messages are prefetched into internal messageQueue.
     */
    protected int messagesPrefetched = 0;

    /**
     * Counter on how many messages have been explicitly requested.
     * TODO: Consider renaming this class and several other variables now that
     * this logic factors in message requests as well as prefetching.
     */
    protected int messagesRequested = 0;

    /**
     * States of the prefetch thread
     */
    protected volatile boolean closed = false;

    protected volatile boolean running = false;

    /**
     * Controls the number of retry attempts to the SQS
     */
    protected int retriesAttempted = 0;

    private final Object stateLock = new Object();

    /**
     * AWS SQS SDK with default backup strategy already re-tries 3 times
     * exponentially. This backoff is on top of that to let the prefetch thread
     * backoff after SDK completes re-tries with a max delay of 2 seconds and
     * 25ms delayInterval.
     */
    protected ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy(25, 25, 2000);

    SQSMessageConsumerPrefetch(SQSSessionCallbackScheduler callbackScheduler,
                               Acknowledger acknowledger,
                               NegativeAcknowledger negativeAcknowledger,
                               SQSQueueDestination sqsDestination,
                               AbstractSQSClientWrapper sqsClientWrapper,
                               int numberOfMessagesToPrefetch) {

        this.sqsClientWrapper = sqsClientWrapper;
        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;

        this.acknowledger = acknowledger;
        this.negativeAcknowledger = negativeAcknowledger;
        this.queueUrl = sqsDestination.getQueueUrl();
        this.sqsDestination = sqsDestination;
        this.callbackScheduler = callbackScheduler;
        this.messageQueue = new ArrayDeque<>(numberOfMessagesToPrefetch);
    }

    MessageListener getMessageListener() {
        return messageListener;
    }

    void setMessageConsumer(AbstractMessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public AbstractMessageConsumer getMessageConsumer() {
        return messageConsumer;
    }

    /**
     * Sets the message listener.
     * <p>
     * If message listener is set, the existing messages on the internal buffer
     * will be pushed to session callback scheduler.
     * <p>
     * If message lister is set to null, then the messages on the internal
     * buffer of session callback scheduler will be negative acknowledged, which
     * will be handled by the session callback scheduler thread itself.
     */
    protected void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        if (messageListener == null || isClosed()) {
            return;
        }
        synchronized (stateLock) {
            if (!running || isClosed()) {
                return;
            }

            List<FetchedMessage> allPrefetchedMessages = new ArrayList<>(messageQueue);
            callbackScheduler.scheduleCallBacks(messageListener, allPrefetchedMessages);
            messageQueue.clear();

            // This will request the first message if necessary.
            // TODO: This may overfetch if setMessageListener is being called multiple
            // times, as the session callback scheduler may already have entries for this consumer.
            messageListenerReady();
        }
    }

    /**
     * Determine the number of messages we should attempt to fetch from SQS.
     * Returns the difference between the number of messages needed (either for
     * prefetching or by request) and the number currently fetched.
     */
    private int numberOfMessagesToFetch() {
        int numberOfMessagesNeeded = Math.max(numberOfMessagesToPrefetch, messagesRequested);
        return Math.max(numberOfMessagesNeeded - messagesPrefetched, 0);
    }

    /**
     * Runs until the message consumer is closed and in-progress SQS
     * <code>receiveMessage</code> call returns.
     * <p>
     * This blocks if configured number of prefetched messages are already
     * received or connection has not started yet.
     * <p>
     * After consumer is closed, all the messages inside internal buffer will
     * be negatively acknowledged.
     */
    @Override
    public void run() {
        while (true) {
            int prefetchBatchSize;
            boolean nackQueueMessages = false;
            List<Message> messages = null;
            try {
                if (isClosed()) {
                    break;
                }

                synchronized (stateLock) {
                    waitForStart();
                    waitForPrefetch();
                    prefetchBatchSize = Math.min(numberOfMessagesToFetch(), SQSMessagingClientConstants.MAX_BATCH);
                }

                if (!isClosed()) {
                    messages = getMessagesWithBackoff(prefetchBatchSize);
                }

                if (messages != null && !messages.isEmpty()) {
                    processReceivedMessages(messages);
                }
            } catch (InterruptedException e) {
                nackQueueMessages = true;
                break;
            } catch (Throwable e) {
                LOG.error("Unexpected exception when prefetch messages:", e);
                nackQueueMessages = true;
                throw new RuntimeException(e);
            } finally {
                if (isClosed() || nackQueueMessages) {
                    nackQueueMessages();
                }
            }
        }
    }

    /**
     * Call <code>receiveMessage</code> with the given wait time.
     */
    protected List<Message> getMessages(int batchSize, int waitTimeSeconds) throws JMSException {

        assert batchSize > 0;

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(batchSize)
                .withAttributeNames(ALL)
                .withMessageAttributeNames(ALL)
                .withWaitTimeSeconds(waitTimeSeconds);
        //if the receive request is for FIFO queue, provide a unique receive request attempt it, so that
        //failed calls retried by SDK will claim the same messages
        if (sqsDestination.isFifo()) {
            receiveMessageRequest.withReceiveRequestAttemptId(UUID.randomUUID().toString());
        }
        ReceiveMessageResult receivedMessageResult = sqsClientWrapper.receiveMessage(receiveMessageRequest);
        return receivedMessageResult.getMessages();
    }

    /**
     * Converts the received message to JMS message, and pushes to messages to
     * either callback scheduler for asynchronous message delivery or to
     * internal buffers for synchronous message delivery. Messages that was not
     * converted to JMS message will be immediately negative acknowledged.
     */
    protected void processReceivedMessages(List<Message> messages) {
        List<String> nackMessages = new ArrayList<>();
        List<FetchedMessage> fetchedMessages = new ArrayList<>();
        for (Message message : messages) {
            try {
                javax.jms.Message jmsMessage = convertToJMSMessage(message);
                fetchedMessages.add(new FetchedMessage(this, jmsMessage));
            } catch (JMSException e) {
                LOG.warn("Caught exception while converting received messages", e);
                nackMessages.add(message.getReceiptHandle());
            }
        }

        synchronized (stateLock) {
            if (messageListener != null) {
                callbackScheduler.scheduleCallBacks(messageListener, fetchedMessages);
            } else {
                messageQueue.addAll(fetchedMessages);
            }

            messagesPrefetched += fetchedMessages.size();
            notifyStateChange();
        }

        // Nack any messages that cannot be serialized to JMSMessage.
        try {
            negativeAcknowledger.action(queueUrl, nackMessages);
        } catch (JMSException e) {
            LOG.warn("Caught exception while nacking received messages", e);
        }
    }

    protected List<Message> getMessagesWithBackoff(int batchSize) throws InterruptedException {
        try {
            List<Message> result = getMessages(batchSize, WAIT_TIME_SECONDS);
            retriesAttempted = 0;
            return result;
        } catch (JMSException e) {
            LOG.warn("Encountered exception during receive in ConsumerPrefetch thread", e);
            try {
                sleep(backoffStrategy.delayBeforeNextRetry(retriesAttempted++));
                return Collections.emptyList();
            } catch (InterruptedException ex) {
                LOG.warn("Interrupted while retrying on receive", ex);
                throw ex;
            }
        }
    }

    protected void waitForPrefetch() throws InterruptedException {
        synchronized (stateLock) {
            while (numberOfMessagesToFetch() <= 0 && !isClosed()) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting on prefetch", e);
                    /* For interruption, we do not nack the messages */
                    throw e;
                }
            }
        }
    }

    /**
     * Convert the return SQS message into JMS message
     *
     * @param message SQS message to convert
     * @return Converted JMS message
     * @throws JMSException exception
     */
    protected javax.jms.Message convertToJMSMessage(Message message) throws JMSException {
        MessageAttributeValue messageTypeAttribute = message.getMessageAttributes()
                .get(SQSMessage.JMS_SQS_MESSAGE_TYPE);
        javax.jms.Message jmsMessage;
        if (messageTypeAttribute == null) {
            jmsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
        } else {
            String messageType = messageTypeAttribute.getStringValue();
            if (SQSMessage.BYTE_MESSAGE_TYPE.equals(messageType)) {
                try {
                    jmsMessage = new SQSBytesMessage(acknowledger, queueUrl, message);
                } catch (JMSException e) {
                    LOG.warn("MessageReceiptHandle - " + message.getReceiptHandle() +
                            "cannot be serialized to BytesMessage", e);
                    throw e;
                }
            } else if (SQSMessage.OBJECT_MESSAGE_TYPE.equals(messageType)) {
                jmsMessage = new SQSObjectMessage(acknowledger, queueUrl, message);
            } else if (SQSMessage.TEXT_MESSAGE_TYPE.equals(messageType)) {
                jmsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
            } else {
                throw new JMSException("Not a supported JMS message type");
            }
        }

        jmsMessage.setJMSDestination(sqsDestination);

        MessageAttributeValue replyToQueueNameAttribute = message.getMessageAttributes().get(
                SQSMessage.JMS_SQS_REPLY_TO_QUEUE_NAME);
        MessageAttributeValue replyToQueueUrlAttribute = message.getMessageAttributes().get(
                SQSMessage.JMS_SQS_REPLY_TO_QUEUE_URL);
        if (replyToQueueNameAttribute != null && replyToQueueUrlAttribute != null) {
            String replyToQueueUrl = replyToQueueUrlAttribute.getStringValue();
            String replyToQueueName = replyToQueueNameAttribute.getStringValue();
            Destination replyToQueue = new SQSQueueDestination(replyToQueueName, replyToQueueUrl);
            jmsMessage.setJMSReplyTo(replyToQueue);
        }

        MessageAttributeValue correlationIdAttribute = message.getMessageAttributes().get(
                SQSMessage.JMS_SQS_CORRELATION_ID);
        if (correlationIdAttribute != null) {
            jmsMessage.setJMSCorrelationID(correlationIdAttribute.getStringValue());
        }

        return jmsMessage;
    }

    protected void nackQueueMessages() {
        // Also nack messages already in the messageQueue
        synchronized (stateLock) {
            try {
                negativeAcknowledger.bulkAction(messageQueue, queueUrl);
            } catch (JMSException e) {
                LOG.warn("Caught exception while nacking queued messages", e);
            } finally {
                notifyStateChange();
            }
        }
    }

    protected void waitForStart() throws InterruptedException {
        synchronized (stateLock) {
            while (!running && !isClosed()) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting on consumer start", e);
                    throw e;
                }
            }
        }
    }

    @Override
    public void messageDispatched() {
        synchronized (stateLock) {
            messagesPrefetched--;
            messagesRequested--;
            if (numberOfMessagesToFetch() > 0) {
                notifyStateChange();
            }
        }
    }

    @Override
    public void messageListenerReady() {
        synchronized (stateLock) {
            // messagesRequested may still be more than zero if there were pending receive()
            // calls when the message listener was set.
            if (messagesRequested <= 0 && !isClosed() && messageListener != null) {
                requestMessage();
            }
        }
    }

    void requestMessage() {
        synchronized (stateLock) {
            messagesRequested++;
            notifyStateChange();
        }
    }

    private void unrequestMessage() {
        synchronized (stateLock) {
            messagesRequested--;
            notifyStateChange();
        }
    }

    javax.jms.Message receive() throws JMSException {
        return receive(0);
    }

    javax.jms.Message receive(long timeout) throws JMSException {
        if (cannotDeliver()) {
            return null;
        }

        if (timeout < 0) {
            timeout = 0;
        }

        FetchedMessage fetchedMessage = null;
        synchronized (stateLock) {
            requestMessage();
            try {
                // If message exists in queue poll.
                if (messageQueue.isEmpty()) {
                    long startTime = System.currentTimeMillis();

                    long waitTime = 0;
                    while (messageQueue.isEmpty() && !isClosed() &&
                            (timeout == 0 || (waitTime = getWaitTime(timeout, startTime)) > 0)) {
                        try {
                            stateLock.wait(waitTime);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }
                    if (messageQueue.isEmpty() || isClosed()) {
                        return null;
                    }
                }
                fetchedMessage = messageQueue.pollFirst();
            } finally {
                if (fetchedMessage == null) {
                    unrequestMessage();
                }
            }
        }
        return messageHandler(fetchedMessage);
    }

    private long getWaitTime(long timeout, long startTime) {
        return timeout - (System.currentTimeMillis() - startTime);
    }

    protected void notifyStateChange() {
        synchronized (stateLock) {
            stateLock.notifyAll();
        }
    }

    javax.jms.Message receiveNoWait() throws JMSException {
        if (cannotDeliver()) {
            return null;
        }

        FetchedMessage fetchedMessage;
        synchronized (stateLock) {
            if (messageQueue.isEmpty() && numberOfMessagesToPrefetch == 0) {
                List<Message> messages = getMessages(1, 0);
                if (messages != null && !messages.isEmpty()) {
                    processReceivedMessages(messages);
                }
            }
            fetchedMessage = messageQueue.pollFirst();
        }
        if (fetchedMessage != null) {
            requestMessage();
        }
        return messageHandler(fetchedMessage);
    }

    void start() {
        if (isClosed() || running) {
            return;
        }
        synchronized (stateLock) {
            running = true;
            notifyStateChange();
        }
    }

    void stop() {
        if (isClosed() || !running) {
            return;
        }
        synchronized (stateLock) {
            running = false;
            notifyStateChange();
        }
    }

    void close() {
        if (isClosed()) {
            return;
        }
        synchronized (stateLock) {
            closed = true;
            notifyStateChange();
            messageListener = null;
        }
    }

    /**
     * Helper that notifies PrefetchThread that message is dispatched and AutoAcknowledge
     */
    private javax.jms.Message messageHandler(FetchedMessage fetchedMessage) throws JMSException {
        if (fetchedMessage == null) {
            return null;
        }
        javax.jms.Message message = fetchedMessage.getMessage();

        // Notify PrefetchThread that message is dispatched
        this.messageDispatched();
        acknowledger.notifyMessageReceived((SQSMessage) message);
        return message;
    }

    private boolean cannotDeliver() throws JMSException {
        if (!running) {
            return true;
        }

        if (isClosed()) {
            throw new JMSException("Cannot receive messages when the consumer is closed");
        }

        if (messageListener != null) {
            throw new JMSException("Cannot receive messages synchronously after a message listener is set");
        }
        return false;
    }

    /**
     * Sleeps for the configured time.
     */
    protected void sleep(long sleepTimeMillis) throws InterruptedException {
        Thread.sleep(sleepTimeMillis);
    }

    protected boolean isClosed() {
        return closed;
    }

    List<SQSMessageIdentifier> purgePrefetchedMessagesWithGroups(Set<String> affectedGroups) throws JMSException {
        List<SQSMessageIdentifier> purgedMessages = new ArrayList<>();
        synchronized (stateLock) {
            //let's walk over the prefetched messages
            Iterator<FetchedMessage> managerIterator = messageQueue.iterator();
            while (managerIterator.hasNext()) {
                FetchedMessage fetchedMessage = managerIterator.next();
                SQSMessage prefetchedMessage = (SQSMessage) fetchedMessage.getMessage();
                SQSMessageIdentifier messageIdentifier = SQSMessageIdentifier.fromSQSMessage(prefetchedMessage);

                //is the prefetch entry for one of the affected group ids?
                if (affectedGroups.contains(messageIdentifier.getGroupId())) {
                    //we will purge this prefetched message
                    purgedMessages.add(messageIdentifier);
                    //remove from prefetch queue
                    managerIterator.remove();
                    //we are done with it and can prefetch more messages
                    this.messagesPrefetched--;
                }
            }

            notifyStateChange();
        }

        return purgedMessages;
    }
}
