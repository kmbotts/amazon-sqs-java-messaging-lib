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


import com.amazonaws.services.sqs.model.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test the SQSSessionCallbackSchedulerTest class
 */
public class SQSSessionCallbackSchedulerTest {

    private static final String QUEUE_URL_PREFIX = "QueueUrl";
    private static final String QUEUE_URL_1 = "QueueUrl1";
    private static final String QUEUE_URL_2 = "QueueUrl2";

    private SQSSession sqsSession;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private SQSConnection sqsConnection;
    private AmazonSQSMessagingClientWrapper sqsClient;
    private ArrayDeque<CallbackEntry> callbackQueue;
    private Acknowledger acknowledger;
    private AbstractMessageConsumer consumer;

    @Before
    public void setup() {

        sqsClient = Mockito.mock(AmazonSQSMessagingClientWrapper.class);

        sqsConnection = Mockito.mock(SQSConnection.class);
        Mockito.when(sqsConnection.getSqsClientWrapper())
                .thenReturn(sqsClient);

        sqsSession = Mockito.mock(SQSSession.class);
        Mockito.when(sqsSession.getConnectionDelegate())
                .thenReturn(sqsConnection);

        negativeAcknowledger = Mockito.mock(NegativeAcknowledger.class);

        callbackQueue = Mockito.mock(ArrayDeque.class);

        acknowledger = Mockito.mock(Acknowledger.class);

        consumer = Mockito.mock(SQSMessageConsumer.class);

        sqsSessionRunnable = Mockito.spy(new SQSSessionCallbackScheduler(
                sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE),
                acknowledger,
                negativeAcknowledger,
                callbackQueue));
    }

    /**
     * Test nack queue messages when the queue is empty
     */
    @Test
    public void testNackQueueMessageWhenEmpty() throws JMSException {

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Nack the messages
         */
        sqsSessionRunnable.nackQueuedMessages();

        /*
         * Verify results
         */
        Mockito.verify(negativeAcknowledger, Mockito.never()).bulkAction(Matchers.anyList(), Matchers.anyInt());
    }

    /**
     * Test nack queue messages does not propagate a JMS exception
     */
    @Test
    public void testNackQueueMessageAcknowledgerThrowJMSException() throws JMSException {

        MessageListener msgListener = Mockito.mock(MessageListener.class);

        SQSMessage sqsMessage = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage.getReceiptHandle())
                .thenReturn("r1");
        Mockito.when(sqsMessage.getSQSMessageID())
                .thenReturn("messageId1");
        Mockito.when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        FetchedMessage msgManager = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager.getMessage())
                .thenReturn(sqsMessage);

        Mockito.when(msgManager.getPrefetchManager())
                .thenReturn(Mockito.mock(PrefetchManager.class));

        CallbackEntry entry1 = new CallbackEntry(msgListener, msgManager);

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(true);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        Mockito.doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).bulkAction(Matchers.anyList(), Matchers.anyInt());

        /*
         * Nack the messages, no exception expected
         */
        sqsSessionRunnable.nackQueuedMessages();
    }

    /**
     * Test nack queue messages does propagate Errors
     */
    @Test
    public void testNackQueueMessageAcknowledgerThrowError() throws JMSException {

        MessageListener msgListener = Mockito.mock(MessageListener.class);

        SQSMessage sqsMessage = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage.getReceiptHandle())
                .thenReturn("r2");
        Mockito.when(sqsMessage.getSQSMessageID())
                .thenReturn("messageId2");
        Mockito.when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_2);

        FetchedMessage msgManager = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager.getMessage())
                .thenReturn(sqsMessage);

        Mockito.when(msgManager.getPrefetchManager())
                .thenReturn(Mockito.mock(PrefetchManager.class));


        CallbackEntry entry1 = new CallbackEntry(msgListener, msgManager);

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(true);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        Mockito.doThrow(new Error("error"))
                .when(negativeAcknowledger).bulkAction(Matchers.anyList(), Matchers.anyInt());

        /*
         * Nack the messages, exception expected
         */
        try {
            sqsSessionRunnable.nackQueuedMessages();
            junit.framework.Assert.fail();
        } catch (Error e) {
            // expected error
        }
    }

    /**
     * Test nack Queue Message
     */
    @Test
    public void testNackQueueMessage() throws JMSException {

        /*
         * Set up mocks
         */
        MessageListener msgListener = Mockito.mock(MessageListener.class);

        SQSMessage sqsMessage1 = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        Mockito.when(sqsMessage1.getSQSMessageID())
                .thenReturn("messageId1");
        Mockito.when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        FetchedMessage msgManager1 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);

        Mockito.when(msgManager1.getPrefetchManager())
                .thenReturn(Mockito.mock(PrefetchManager.class));

        SQSMessage sqsMessage2 = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage2.getReceiptHandle())
                .thenReturn("r2");
        Mockito.when(sqsMessage2.getSQSMessageID())
                .thenReturn("messageId2");
        Mockito.when(sqsMessage2.getQueueUrl())
                .thenReturn(QUEUE_URL_2);

        FetchedMessage msgManager2 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager2.getMessage())
                .thenReturn(sqsMessage2);

        Mockito.when(msgManager2.getPrefetchManager())
                .thenReturn(Mockito.mock(PrefetchManager.class));

        CallbackEntry entry1 = new CallbackEntry(msgListener, msgManager1);
        CallbackEntry entry2 = new CallbackEntry(msgListener, msgManager2);

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        List<SQSMessageIdentifier> nackMessageIdentifiers = new ArrayList<SQSMessageIdentifier>();
        nackMessageIdentifiers.add(SQSMessageIdentifier.builder()
                .queueUrl(QUEUE_URL_1)
                .receiptHandle("r1")
                .SQSMessageID("messageId1")
                .build());
        nackMessageIdentifiers.add(SQSMessageIdentifier.builder()
                .queueUrl(QUEUE_URL_2)
                .receiptHandle("r2")
                .SQSMessageID("messageId2")
                .build());

        /*
         * Nack the messages
         */
        sqsSessionRunnable.nackQueuedMessages();

        /*
         * Verify results
         */
        Mockito.verify(negativeAcknowledger).bulkAction(Matchers.eq(nackMessageIdentifiers), Matchers.eq(2));
    }

    /**
     * Test starting callback does not propagate Interrupted Exception
     */
    @Test
    public void testStartingCallbackThrowJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        Mockito.doThrow(new JMSException("closed"))
                .when(sqsSession).startingCallback(consumer);

        Mockito.doNothing()
                .when(sqsSessionRunnable).nackQueuedMessages();

        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        FetchedMessage msgManager1 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager1.getMessage())
                .thenReturn(Mockito.mock(SQSMessage.class));
        Mockito.when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        CallbackEntry entry1 = new CallbackEntry(null, msgManager1);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        /*
         * Nack the messages, exit the loop
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        Mockito.verify(sqsSession).startingCallback(consumer);
        Mockito.verify(sqsSessionRunnable).nackQueuedMessages();
        Mockito.verify(sqsSession, Mockito.never()).finishedCallback();
    }

    /**
     * Test callback run execution when call back entry message listener is empty
     */
    @Test
    public void testCallbackQueueEntryMessageListenerEmpty() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        Mockito.doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(Matchers.any(SQSMessageConsumer.class));

        FetchedMessage msgManager1 = createMessageManager(1);
        FetchedMessage msgManager2 = createMessageManager(2);

        CallbackEntry entry1 = new CallbackEntry(null, msgManager1);
        CallbackEntry entry2 = new CallbackEntry(null, msgManager2);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(true);

        // Setup ConsumerCloseAfterCallback
        SQSMessageConsumer messageConsumer = Mockito.mock(SQSMessageConsumer.class);
        sqsSessionRunnable.setConsumerCloseAfterCallback(messageConsumer);

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        Mockito.verify(sqsSession, Mockito.times(2)).startingCallback(consumer);
        Mockito.verify(sqsSessionRunnable).nackQueuedMessages();

        // Verify that we nack the message
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(negativeAcknowledger, Mockito.times(2)).bulkAction(captor.capture(), Matchers.eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>) allCaptured.get(0);
        Assert.assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        Assert.assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>) allCaptured.get(1);
        Assert.assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        Assert.assertEquals("r2", captured.get(0).getReceiptHandle());

        // Verify do close is called on set ConsumerCloseAfterCallback
        Mockito.verify(messageConsumer).doClose();

        Mockito.verify(sqsSession).finishedCallback();
    }

    /**
     * Test callback run execution when message ack throws a JMS exception
     */
    @Test
    public void testCallbackQueueEntryMessageAckThrowsJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        Mockito.doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        Mockito.when(sqsMessage1.getSQSMessageID())
                .thenReturn("messageId1");
        Mockito.when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        FetchedMessage msgManager1 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        Mockito.when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        // Throw an exception when try to acknowledge the message
        Mockito.doThrow(new JMSException("Exception"))
                .when(sqsMessage1).acknowledge();

        MessageListener msgListener = Mockito.mock(MessageListener.class);
        CallbackEntry entry1 = new CallbackEntry(msgListener, msgManager1);

        FetchedMessage msgManager2 = createMessageManager(2);
        CallbackEntry entry2 = new CallbackEntry(msgListener, msgManager2);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        Mockito.verify(sqsSession, Mockito.times(2)).startingCallback(consumer);
        Mockito.verify(sqsSessionRunnable).nackQueuedMessages();

        Mockito.verify(sqsMessage1).acknowledge();
        // Verify that we nack the message
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(negativeAcknowledger, Mockito.times(2)).bulkAction(captor.capture(), Matchers.eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>) allCaptured.get(0);
        Assert.assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        Assert.assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>) allCaptured.get(1);
        Assert.assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        Assert.assertEquals("r2", captured.get(0).getReceiptHandle());

        Mockito.verify(sqsSession).finishedCallback();
    }


    /**
     * Test callback run execution when message nack throws a JMS exception
     */
    @Test
    public void testCallbackQueueEntryMessageNAckThrowsJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        Mockito.doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        Mockito.when(sqsMessage1.getSQSMessageID())
                .thenReturn("messageId1");
        Mockito.when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        FetchedMessage msgManager1 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        Mockito.when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        // Set message listener as null to force a nack
        CallbackEntry entry1 = new CallbackEntry(null, msgManager1);

        FetchedMessage msgManager2 = createMessageManager(2);
        CallbackEntry entry2 = new CallbackEntry(null, msgManager2);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);
        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(true);

        // Throw an exception when try to negative acknowledge the message
        Mockito.doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).action(QUEUE_URL_1, Collections.singletonList("r1"));

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        Mockito.verify(sqsSession, Mockito.times(2)).startingCallback(consumer);
        Mockito.verify(sqsSessionRunnable).nackQueuedMessages();

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(negativeAcknowledger, Mockito.times(2)).bulkAction(captor.capture(), Matchers.eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>) allCaptured.get(0);
        Assert.assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        Assert.assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>) allCaptured.get(1);
        Assert.assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        Assert.assertEquals("r2", captured.get(0).getReceiptHandle());

        Mockito.verify(sqsSession).finishedCallback();
    }

    /**
     * Test schedule callback
     */
    @Test
    public void testScheduleCallBack() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        sqsSessionRunnable = Mockito.spy(new SQSSessionCallbackScheduler(
                sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE),
                acknowledger,
                negativeAcknowledger,
                null));

        MessageListener msgListener = Mockito.mock(MessageListener.class);
        FetchedMessage msgManager = Mockito.mock(FetchedMessage.class);
        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.scheduleCallBacks(msgListener, Collections.singletonList(msgManager));

        Assert.assertEquals(1, sqsSessionRunnable.getCallbackQueue().size());

        CallbackEntry entry = sqsSessionRunnable.getCallbackQueue().pollFirst();

        Assert.assertEquals(msgListener, entry.getMessageListener());
        Assert.assertEquals(msgManager, entry.getFetchedMessage());
    }

    /**
     * Test that no auto ack messages occurs when client acknowledge is set
     */
    @Test
    public void testMessageNotAckWithClientAckMode() throws JMSException, InterruptedException {

        /**
         * Set up mocks
         */
        sqsSessionRunnable = Mockito.spy(new SQSSessionCallbackScheduler(sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE),
                acknowledger, negativeAcknowledger, callbackQueue));

        Mockito.doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        Mockito.when(sqsMessage1.getSQSMessageID())
                .thenReturn("messageId1");
        Mockito.when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        FetchedMessage msgManager1 = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        Mockito.when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        MessageListener msgListener = Mockito.mock(MessageListener.class);
        CallbackEntry entry1 = new CallbackEntry(msgListener, msgManager1);

        Mockito.when(callbackQueue.pollFirst())
                .thenReturn(entry1);
        Mockito.when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Start the callback
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        Mockito.verify(sqsSession, Mockito.times(2)).startingCallback(consumer);
        Mockito.verify(sqsSessionRunnable).nackQueuedMessages();

        // Verify that do not ack the message
        Mockito.verify(sqsMessage1, Mockito.never()).acknowledge();
        Mockito.verify(negativeAcknowledger, Mockito.never()).action(QUEUE_URL_1, Collections.singletonList("r1"));
        Mockito.verify(sqsSession).finishedCallback();
    }

    /**
     * Test that no auto ack messages occurs when client acknowledge is set
     */
    @Test
    public void testWhenListenerThrowsWhenAutoAckThenCallbackQueuePurgedFromMessagesWithSameQueueAndGroup() throws JMSException, InterruptedException {

        /**
         * Set up mocks
         */
        sqsSessionRunnable = Mockito.spy(new SQSSessionCallbackScheduler(
                sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE),
                acknowledger,
                negativeAcknowledger,
                null));

        MessageListener messageListener = Mockito.mock(MessageListener.class);
        Mockito.doThrow(RuntimeException.class)
                .when(messageListener).onMessage(Matchers.any(javax.jms.Message.class));

        List<FetchedMessage> messages = new ArrayList<FetchedMessage>();
        messages.add(createFifoMessageManager("queue1", "group1", "message1", "handle1"));
        messages.add(createFifoMessageManager("queue1", "group1", "message2", "handle2"));
        messages.add(createFifoMessageManager("queue2", "group1", "message3", "handle3"));
        messages.add(createFifoMessageManager("queue1", "group2", "message4", "handle4"));
        messages.add(createFifoMessageManager("queue1", "group1", "message5", "handle5"));
        messages.add(createFifoMessageManager("queue2", "group2", "message6", "handle6"));
        sqsSessionRunnable.scheduleCallBacks(messageListener, messages);

        Mockito.doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        sqsSessionRunnable.run();

        ArgumentCaptor<List> messageIdentifierListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Integer> indexOfMessageCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.verify(negativeAcknowledger, Mockito.times(3)).bulkAction(messageIdentifierListCaptor.capture(), indexOfMessageCaptor.capture());
        List<SQSMessageIdentifier> nackedMessages = messageIdentifierListCaptor.getAllValues().get(0);
        int nackedMessagesSize = indexOfMessageCaptor.getAllValues().get(0).intValue();

        //failing to process 'message1' should nack all messages for queue1 and group1, that is 'message1', 'message2' and 'message5'
        Assert.assertEquals(3, nackedMessagesSize);
        Assert.assertEquals("message1", nackedMessages.get(0).getSQSMessageID());
        Assert.assertEquals("message2", nackedMessages.get(1).getSQSMessageID());
        Assert.assertEquals("message5", nackedMessages.get(2).getSQSMessageID());
    }

    private FetchedMessage createFifoMessageManager(String queueUrl, String groupId, String messageId, String receiptHandle) throws JMSException {
        Message message = new Message();
        message.setBody("body");
        message.setMessageId(messageId);
        message.setReceiptHandle(receiptHandle);
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, "728374687246872364");
        attributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, messageId);
        attributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId);
        attributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "0");
        message.setAttributes(attributes);
        SQSMessage sqsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);
        FetchedMessage msgManager = new FetchedMessage(prefetchManager, sqsMessage);
        return msgManager;
    }

    private FetchedMessage createMessageManager(int index) {
        SQSMessage sqsMessage = Mockito.mock(SQSMessage.class);
        Mockito.when(sqsMessage.getReceiptHandle())
                .thenReturn("r" + index);
        Mockito.when(sqsMessage.getSQSMessageID())
                .thenReturn("messageId" + index);
        Mockito.when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_PREFIX + index);

        PrefetchManager prefetchManager = Mockito.mock(PrefetchManager.class);
        Mockito.when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        FetchedMessage msgManager = Mockito.mock(FetchedMessage.class);
        Mockito.when(msgManager.getMessage())
                .thenReturn(sqsMessage);
        Mockito.when(msgManager.getPrefetchManager())
                .thenReturn(prefetchManager);
        return msgManager;
    }
}
