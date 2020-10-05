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


import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.jms.JMSException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Test the SQSMessageProducerTest class
 */
public class SQSMessageProducerFifoTest {

    public static final String QUEUE_URL = "QueueUrl.fifo";
    public static final String QUEUE_NAME = "QueueName.fifo";
    public static final String MESSAGE_ID = "MessageId";
    public static final String SEQ_NUMBER = "10010101231012312354534";
    public static final String SEQ_NUMBER_2 = "10010101231012312354535";
    public static final String GROUP_ID = "G1";
    public static final String DEDUP_ID = "D1";

    private SQSMessageProducer producer;
    private SQSQueueDestination destination;
    private AmazonSQSMessagingClientWrapper amazonSQSClient;
    private Contracts.Acknowledger acknowledger;

    @Before
    public void setup() throws JMSException {

        amazonSQSClient = Mockito.mock(AmazonSQSMessagingClientWrapper.class);

        acknowledger = Mockito.mock(Contracts.Acknowledger.class);

        SQSSession sqsSession = Mockito.mock(SQSSession.class);
        destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        producer = Mockito.spy(new SQSMessageProducer(amazonSQSClient, sqsSession, destination));
    }

    /**
     * Test propertyToMessageAttribute with empty messages of different type
     */
    @Test
    public void testPropertyToMessageAttributeWithEmpty() throws JMSException {

        /*
         * Test Empty text message default attribute
         */
        SQSMessage sqsText = new SQSTextMessage();
        Map<String, MessageAttributeValue> messageAttributeText = producer.propertyToMessageAttribute(sqsText);

        Assert.assertEquals(0, messageAttributeText.size());

        /*
         * Test Empty object message default attribute
         */
        SQSMessage sqsObject = new SQSObjectMessage();
        Map<String, MessageAttributeValue> messageAttributeObject = producer.propertyToMessageAttribute(sqsObject);

        Assert.assertEquals(0, messageAttributeObject.size());

        /*
         * Test Empty byte message default attribute
         */
        SQSMessage sqsByte = new SQSBytesMessage();
        Map<String, MessageAttributeValue> messageAttributeByte = producer.propertyToMessageAttribute(sqsByte);

        Assert.assertEquals(0, messageAttributeByte.size());
    }

    /**
     * Test propertyToMessageAttribute with messages of different type
     */
    @Test
    public void testPropertyToMessageAttribute() throws JMSException {

        internalTestPropertyToMessageAttribute(new SQSTextMessage());

        internalTestPropertyToMessageAttribute(new SQSObjectMessage());

        internalTestPropertyToMessageAttribute(new SQSBytesMessage());
    }

    public void internalTestPropertyToMessageAttribute(SQSMessage sqsText) throws JMSException {

        /*
         * Setup JMS message property
         */
        String booleanProperty = "BooleanProperty";
        String byteProperty = "ByteProperty";
        String shortProperty = "ShortProperty";
        String intProperty = "IntProperty";
        String longProperty = "LongProperty";
        String floatProperty = "FloatProperty";
        String doubleProperty = "DoubleProperty";
        String stringProperty = "StringProperty";
        String objectProperty = "ObjectProperty";

        sqsText.setBooleanProperty(booleanProperty, true);
        sqsText.setByteProperty(byteProperty, (byte) 1);
        sqsText.setShortProperty(shortProperty, (short) 2);
        sqsText.setIntProperty(intProperty, 3);
        sqsText.setLongProperty(longProperty, 4L);
        sqsText.setFloatProperty(floatProperty, (float) 5.0);
        sqsText.setDoubleProperty(doubleProperty, 6.0);
        sqsText.setStringProperty(stringProperty, "seven");
        sqsText.setObjectProperty(objectProperty, new Integer(8));

        MessageAttributeValue messageAttributeValueBoolean = new MessageAttributeValue();
        messageAttributeValueBoolean.setDataType("String.Boolean");
        messageAttributeValueBoolean.setStringValue("true");

        MessageAttributeValue messageAttributeValueByte = new MessageAttributeValue();
        messageAttributeValueByte.setDataType("Number.byte");
        messageAttributeValueByte.setStringValue("1");

        MessageAttributeValue messageAttributeValueShort = new MessageAttributeValue();
        messageAttributeValueShort.setDataType("Number.short");
        messageAttributeValueShort.setStringValue("2");

        MessageAttributeValue messageAttributeValueInt = new MessageAttributeValue();
        messageAttributeValueInt.setDataType("Number.int");
        messageAttributeValueInt.setStringValue("3");

        MessageAttributeValue messageAttributeValueLong = new MessageAttributeValue();
        messageAttributeValueLong.setDataType("Number.long");
        messageAttributeValueLong.setStringValue("4");

        MessageAttributeValue messageAttributeValueFloat = new MessageAttributeValue();
        messageAttributeValueFloat.setDataType("Number.float");
        messageAttributeValueFloat.setStringValue("5.0");

        MessageAttributeValue messageAttributeValueDouble = new MessageAttributeValue();
        messageAttributeValueDouble.setDataType("Number.double");
        messageAttributeValueDouble.setStringValue("6.0");

        MessageAttributeValue messageAttributeValueString = new MessageAttributeValue();
        messageAttributeValueString.setDataType("String");
        messageAttributeValueString.setStringValue("seven");

        MessageAttributeValue messageAttributeValueObject = new MessageAttributeValue();
        messageAttributeValueObject.setDataType("Number.int");
        messageAttributeValueObject.setStringValue("8");

        /*
         * Convert property to sqs message attribute
         */
        Map<String, MessageAttributeValue> messageAttribute = producer.propertyToMessageAttribute(sqsText);

        /*
         * Verify results
         */
        Assert.assertEquals(messageAttributeValueBoolean, messageAttribute.get(booleanProperty));
        Assert.assertEquals(messageAttributeValueByte, messageAttribute.get(byteProperty));
        Assert.assertEquals(messageAttributeValueShort, messageAttribute.get(shortProperty));
        Assert.assertEquals(messageAttributeValueInt, messageAttribute.get(intProperty));
        Assert.assertEquals(messageAttributeValueLong, messageAttribute.get(longProperty));
        Assert.assertEquals(messageAttributeValueFloat, messageAttribute.get(floatProperty));
        Assert.assertEquals(messageAttributeValueDouble, messageAttribute.get(doubleProperty));
        Assert.assertEquals(messageAttributeValueString, messageAttribute.get(stringProperty));
        Assert.assertEquals(messageAttributeValueObject, messageAttribute.get(objectProperty));

    }

    /**
     * Test sendMessageInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessage() throws JMSException {

        String messageBody = "MyText1";
        SQSTextMessage msg = Mockito.spy(new SQSTextMessage(messageBody));
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendMessageInternal(destination, msg, null);

        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.TEXT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER);
    }

    /**
     * Test sendMessageInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessageFromReceivedMessage() throws JMSException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.TEXT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(PropertyType.STRING.getType());
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody("MessageBody");

        SQSTextMessage msg = Mockito.spy(new SQSTextMessage(acknowledger, QUEUE_URL, message));
        msg.setWritePermissionsForProperties(true);

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendMessageInternal(destination, msg, null);

        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, "MessageBody", SQSMessage.TEXT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    /**
     * Test sendMessageInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessage() throws JMSException {

        HashSet<String> set = new HashSet<String>();
        set.add("data1");

        SQSObjectMessage msg = Mockito.spy(new SQSObjectMessage(set));
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);
        String msgBody = msg.getMessageBody();

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendMessageInternal(destination, msg, null);

        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, msgBody, SQSMessage.OBJECT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER);
    }

    /**
     * Test sendMessageInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessageFromReceivedMessage() throws JMSException, IOException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.OBJECT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(PropertyType.STRING.getType());
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

        // Encode an object to byte array
        Integer integer = new Integer("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        String messageBody = Base64.encodeAsString(array.toByteArray());
        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody(messageBody);

        SQSObjectMessage msg = Mockito.spy(new SQSObjectMessage(acknowledger, QUEUE_URL, message));
        msg.setWritePermissionsForProperties(true);

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendMessageInternal(destination, msg, null);

        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.OBJECT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    /**
     * Test sendMessageInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessage() throws JMSException {

        SQSBytesMessage msg = Mockito.spy(new SQSBytesMessage());
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);
        msg.writeByte((byte) 0);
        msg.reset();

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendMessageInternal(destination, msg, null);

        String messageBody = "AA==";
        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.BYTE_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));

        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER);
    }

    /**
     * Test sendMessageInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessageFromReceivedMessage() throws JMSException, IOException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(PropertyType.STRING.getType());
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

        byte[] byteArray = new byte[]{1, 0, 'a', 65};
        String messageBody = Base64.encodeAsString(byteArray);
        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody(messageBody);

        SQSBytesMessage msg = Mockito.spy(new SQSBytesMessage(acknowledger, QUEUE_URL, message));
        msg.setWritePermissionsForProperties(true);

        Mockito.when(amazonSQSClient.sendMessage(Matchers.any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendMessageInternal(destination, msg, null);

        Mockito.verify(amazonSQSClient).sendMessage(Matchers.argThat(new SendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.BYTE_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        Mockito.verify(msg).setJMSDestination(destination);
        Mockito.verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        Mockito.verify(msg).setSQSMessageID(MESSAGE_ID);
        Mockito.verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    private static class SendMessageRequestMatcher extends ArgumentMatcher<SendMessageRequest> {

        private final String queueUrl;
        private final String messagesBody;
        private final String messageType;
        private final String groupId;
        private final String deduplicationId;

        private SendMessageRequestMatcher(String queueUrl, String messagesBody, String messageType, String groupId, String deduplicationId) {
            this.queueUrl = queueUrl;
            this.messagesBody = messagesBody;
            this.messageType = messageType;
            this.groupId = groupId;
            this.deduplicationId = deduplicationId;
        }

        @Override
        public boolean matches(Object argument) {
            if (!(argument instanceof SendMessageRequest)) {
                return false;
            }

            SendMessageRequest request = (SendMessageRequest) argument;
            Assert.assertEquals(queueUrl, request.getQueueUrl());
            Assert.assertEquals(messagesBody, request.getMessageBody());
            String messageType = request.getMessageAttributes().get(SQSMessage.JMS_SQS_MESSAGE_TYPE).getStringValue();
            Assert.assertEquals(this.messageType, messageType);
            Assert.assertEquals(this.groupId, request.getMessageGroupId());
            Assert.assertEquals(this.deduplicationId, request.getMessageDeduplicationId());
            return true;
        }
    }
}
