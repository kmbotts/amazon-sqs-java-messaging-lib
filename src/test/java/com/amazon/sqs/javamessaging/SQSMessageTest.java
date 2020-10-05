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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.QueueSession;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test the SQSMessageTest class
 */
public class SQSMessageTest {
    private QueueSession mockSQSSession;
    final String myTrueBoolean = "myTrueBoolean";
    final String myFalseBoolean = "myFalseBoolean";
    final String myInteger = "myInteger";
    final String myDouble = "myDouble";
    final String myFloat = "myFloat";
    final String myLong = "myLong";
    final String myShort = "myShort";
    final String myByte = "myByte";
    final String myString = "myString";
    final String myCustomString = "myCustomString";
    final String myNumber = "myNumber";

    @Before
    public void setup() {
        mockSQSSession = Mockito.mock(QueueSession.class);
    }

    /**
     * Test setting SQSMessage property
     */
    @Test
    public void testProperty() throws JMSException {
        Mockito.when(mockSQSSession.createMessage()).thenReturn(new TestSQSMessage());
        Message message = mockSQSSession.createMessage();

        message.setBooleanProperty("myTrueBoolean", true);
        message.setBooleanProperty("myFalseBoolean", false);
        message.setIntProperty("myInteger", 100);
        message.setDoubleProperty("myDouble", 2.1768);
        message.setFloatProperty("myFloat", 3.1457f);
        message.setLongProperty("myLong", 1290772974281L);
        message.setShortProperty("myShort", (short) 123);
        message.setByteProperty("myByteProperty", (byte) 'a');
        message.setStringProperty("myString", "StringValue");
        message.setStringProperty("myNumber", "500");

        Assert.assertTrue(message.propertyExists("myTrueBoolean"));
        Assert.assertEquals(message.getObjectProperty("myTrueBoolean"), true);
        Assert.assertTrue(message.getBooleanProperty("myTrueBoolean"));

        Assert.assertTrue(message.propertyExists("myFalseBoolean"));
        Assert.assertEquals(message.getObjectProperty("myFalseBoolean"), false);
        Assert.assertFalse(message.getBooleanProperty("myFalseBoolean"));

        Assert.assertTrue(message.propertyExists("myInteger"));
        Assert.assertEquals(message.getObjectProperty("myInteger"), 100);
        Assert.assertEquals(message.getIntProperty("myInteger"), 100);

        Assert.assertTrue(message.propertyExists("myDouble"));
        Assert.assertEquals(message.getObjectProperty("myDouble"), 2.1768);
        Assert.assertEquals(message.getDoubleProperty("myDouble"), 2.1768, 0.0d);

        Assert.assertTrue(message.propertyExists("myFloat"));
        Assert.assertEquals(message.getObjectProperty("myFloat"), 3.1457f);
        Assert.assertEquals(message.getFloatProperty("myFloat"), 3.1457f, 0.0f);

        Assert.assertTrue(message.propertyExists("myLong"));
        Assert.assertEquals(message.getObjectProperty("myLong"), 1290772974281L);
        Assert.assertEquals(message.getLongProperty("myLong"), 1290772974281L);

        Assert.assertTrue(message.propertyExists("myShort"));
        Assert.assertEquals(message.getObjectProperty("myShort"), (short) 123);
        Assert.assertEquals(message.getShortProperty("myShort"), (short) 123);

        Assert.assertTrue(message.propertyExists("myByteProperty"));
        Assert.assertEquals(message.getObjectProperty("myByteProperty"), (byte) 'a');
        Assert.assertEquals(message.getByteProperty("myByteProperty"), (byte) 'a');

        Assert.assertTrue(message.propertyExists("myString"));
        Assert.assertEquals(message.getObjectProperty("myString"), "StringValue");
        Assert.assertEquals(message.getStringProperty("myString"), "StringValue");

        Assert.assertTrue(message.propertyExists("myNumber"));
        Assert.assertEquals(message.getObjectProperty("myNumber"), "500");
        Assert.assertEquals(message.getStringProperty("myNumber"), "500");
        Assert.assertEquals(message.getLongProperty("myNumber"), 500L);
        Assert.assertEquals(message.getFloatProperty("myNumber"), 500f, 0.0f);
        Assert.assertEquals(message.getShortProperty("myNumber"), (short) 500);
        Assert.assertEquals(message.getDoubleProperty("myNumber"), 500d, 0.0d);
        Assert.assertEquals(message.getIntProperty("myNumber"), 500);

        // Validate property names
        Set<String> propertyNamesSet = new HashSet<String>(Arrays.asList(
                "myTrueBoolean",
                "myFalseBoolean",
                "myInteger",
                "myDouble",
                "myFloat",
                "myLong",
                "myShort",
                "myByteProperty",
                "myNumber",
                "myString"));

        Enumeration<String> propertyNames = message.getPropertyNames();
        int counter = 0;
        while (propertyNames.hasMoreElements()) {
            Assert.assertTrue(propertyNamesSet.contains(propertyNames.nextElement()));
            counter++;
        }
        Assert.assertEquals(propertyNamesSet.size(), counter);

        message.clearProperties();
        Assert.assertFalse(message.propertyExists("myTrueBoolean"));
        Assert.assertFalse(message.propertyExists("myInteger"));
        Assert.assertFalse(message.propertyExists("myDouble"));
        Assert.assertFalse(message.propertyExists("myFloat"));
        Assert.assertFalse(message.propertyExists("myLong"));
        Assert.assertFalse(message.propertyExists("myShort"));
        Assert.assertFalse(message.propertyExists("myByteProperty"));
        Assert.assertFalse(message.propertyExists("myString"));
        Assert.assertFalse(message.propertyExists("myNumber"));

        propertyNames = message.getPropertyNames();
        Assert.assertFalse(propertyNames.hasMoreElements());
    }

    /**
     * Test check property write permissions
     */
    @Test
    public void testCheckPropertyWritePermissions() throws JMSException {
        SQSMessage msg = new TestSQSMessage();

        msg.checkBodyWritePermissions();

        msg.setWritePermissionsForBody(false);

        try {
            msg.checkBodyWritePermissions();
        } catch (MessageNotWriteableException exception) {
            Assert.assertEquals("Message body is not writable", exception.getMessage());
        }

        msg.checkPropertyWritePermissions();

        msg.setWritePermissionsForProperties(false);

        try {
            msg.checkPropertyWritePermissions();
        } catch (MessageNotWriteableException exception) {
            Assert.assertEquals("Message properties are not writable", exception.getMessage());
        }
    }

    /**
     * Test set object property
     */
    @Test
    public void testSetObjectProperty() throws JMSException {
        SQSMessage msg = Mockito.spy(new TestSQSMessage());

        try {
            msg.setObjectProperty(null, 1);
            Assert.fail();
        } catch (IllegalArgumentException exception) {
            Assert.assertEquals("Property name can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("", 1);
            Assert.fail();
        } catch (IllegalArgumentException exception) {
            Assert.assertEquals("Property name can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("Property", null);
        } catch (MessageFormatException exception) {
            Assert.fail();
        }

        try {
            msg.setObjectProperty("Property", "");
        } catch (IllegalArgumentException exception) {
            Assert.fail();
        }

        try {
            msg.setObjectProperty("Property", new HashSet<String>());
            Assert.fail();
        } catch (MessageFormatException exception) {
            Assert.assertEquals("Value of property with name Property has incorrect type java.util.HashSet.",
                    exception.getMessage());
        }

        msg.setWritePermissionsForProperties(false);
        try {
            msg.setObjectProperty("Property", "1");
            Assert.fail();
        } catch (MessageNotWriteableException exception) {
            Assert.assertEquals("Message properties are not writable", exception.getMessage());
        }

        msg.setWritePermissionsForProperties(true);
        msg.setObjectProperty("Property", "1");

        Assert.assertEquals("1", msg.getJMSMessagePropertyValue("Property").getValue());
    }

    /**
     * Test using SQS message attribute during SQS Message constructing
     */
    @Test
    public void testSQSMessageAttributeToProperty() throws JMSException {

        Contracts.Acknowledger ack = Mockito.mock(Contracts.Acknowledger.class);

        Map<String, String> systemAttributes = new HashMap<>();
        systemAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "100");

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        messageAttributes.put(myTrueBoolean, new MessageAttributeValue()
                .withDataType(PropertyType.BOOLEAN.getType())
                .withStringValue("true"));

        messageAttributes.put(myFalseBoolean, new MessageAttributeValue()
                .withDataType(PropertyType.BOOLEAN.getType())
                .withStringValue("0"));

        messageAttributes.put(myInteger, new MessageAttributeValue()
                .withDataType(PropertyType.INT.getType())
                .withStringValue("100"));

        messageAttributes.put(myDouble, new MessageAttributeValue()
                .withDataType(PropertyType.DOUBLE.getType())
                .withStringValue("2.1768"));

        messageAttributes.put(myFloat, new MessageAttributeValue()
                .withDataType(PropertyType.FLOAT.getType())
                .withStringValue("3.1457"));

        messageAttributes.put(myLong, new MessageAttributeValue()
                .withDataType(PropertyType.LONG.getType())
                .withStringValue("1290772974281"));

        messageAttributes.put(myShort, new MessageAttributeValue()
                .withDataType(PropertyType.SHORT.getType())
                .withStringValue("123"));

        messageAttributes.put(myByte, new MessageAttributeValue()
                .withDataType(PropertyType.BYTE.getType())
                .withStringValue("1"));

        messageAttributes.put(myString, new MessageAttributeValue()
                .withDataType(PropertyType.STRING.getType())
                .withStringValue("StringValue"));

        messageAttributes.put(myCustomString, new MessageAttributeValue()
                .withDataType("prop.custom")
                .withStringValue("['one', 'two']"));

        messageAttributes.put(myNumber, new MessageAttributeValue()
                .withDataType("Number")
                .withStringValue("500"));

        com.amazonaws.services.sqs.model.Message sqsMessage = new com.amazonaws.services.sqs.model.Message()
                .withMessageAttributes(messageAttributes)
                .withAttributes(systemAttributes)
                .withMessageId("messageId")
                .withReceiptHandle("ReceiptHandle");

        SQSMessage message = new TestSQSMessage(ack, "QueueUrl", sqsMessage);

        Assert.assertTrue(message.propertyExists(myTrueBoolean));
        Assert.assertEquals(message.getObjectProperty(myTrueBoolean), true);
        Assert.assertTrue(message.getBooleanProperty(myTrueBoolean));

        Assert.assertTrue(message.propertyExists(myFalseBoolean));
        Assert.assertEquals(message.getObjectProperty(myFalseBoolean), false);
        Assert.assertFalse(message.getBooleanProperty(myFalseBoolean));

        Assert.assertTrue(message.propertyExists(myInteger));
        Assert.assertEquals(message.getObjectProperty(myInteger), 100);
        Assert.assertEquals(message.getIntProperty(myInteger), 100);

        Assert.assertTrue(message.propertyExists(myDouble));
        Assert.assertEquals(message.getObjectProperty(myDouble), 2.1768);
        Assert.assertEquals(message.getDoubleProperty(myDouble), 2.1768, 0.0d);

        Assert.assertTrue(message.propertyExists(myFloat));
        Assert.assertEquals(message.getObjectProperty(myFloat), 3.1457f);
        Assert.assertEquals(message.getFloatProperty(myFloat), 3.1457f, 0.0f);

        Assert.assertTrue(message.propertyExists(myLong));
        Assert.assertEquals(message.getObjectProperty(myLong), 1290772974281L);
        Assert.assertEquals(message.getLongProperty(myLong), 1290772974281L);

        Assert.assertTrue(message.propertyExists(myShort));
        Assert.assertEquals(message.getObjectProperty(myShort), (short) 123);
        Assert.assertEquals(message.getShortProperty(myShort), (short) 123);

        Assert.assertTrue(message.propertyExists(myByte));
        Assert.assertEquals(message.getObjectProperty(myByte), (byte) 1);
        Assert.assertEquals(message.getByteProperty(myByte), (byte) 1);

        Assert.assertTrue(message.propertyExists(myString));
        Assert.assertEquals(message.getObjectProperty(myString), "StringValue");
        Assert.assertEquals(message.getStringProperty(myString), "StringValue");

        Assert.assertTrue(message.propertyExists(myCustomString));
        Assert.assertEquals(message.getObjectProperty(myCustomString), "['one', 'two']");
        Assert.assertEquals(message.getStringProperty(myCustomString), "['one', 'two']");

        Assert.assertTrue(message.propertyExists(myNumber));
        Assert.assertEquals(message.getObjectProperty(myNumber), "500");
        Assert.assertEquals(message.getStringProperty(myNumber), "500");
        Assert.assertEquals(message.getIntProperty(myNumber), 500);
        Assert.assertEquals(message.getShortProperty(myNumber), (short) 500);
        Assert.assertEquals(message.getLongProperty(myNumber), 500L);
        Assert.assertEquals(message.getFloatProperty(myNumber), 500f, 0.0f);
        Assert.assertEquals(message.getDoubleProperty(myNumber), 500d, 0.0d);

        Collection<String> props = new HashSet<>();
        Enumeration<String> propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
            String next = propertyNames.nextElement();
            Assert.assertTrue(message.propertyExists(next));
            props.add(next);
        }

        message.clearProperties();
        for (String prop : props) {
            Assert.assertFalse(message.propertyExists(prop));
        }

        propertyNames = message.getPropertyNames();
        Assert.assertFalse(propertyNames.hasMoreElements());
    }

    static class TestSQSMessage extends SQSMessage {
        public TestSQSMessage() {
        }

        public TestSQSMessage(Contracts.Acknowledger acknowledger, String queueUrl, com.amazonaws.services.sqs.model.Message sqsMessage) throws JMSException {
            super(acknowledger, queueUrl, sqsMessage);
        }

        @Override
        public void clearBody() throws JMSException {
            super.clearBody();
        }
    }
}
