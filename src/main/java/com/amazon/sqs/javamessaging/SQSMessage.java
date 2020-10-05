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
import com.amazonaws.util.StringUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The SQSMessage is the root class of all SQS JMS messages and implements JMS
 * Message interface.
 * <p>
 * Not all message headers are supported at this time:
 * <ul>
 * <li><code>JMSMessageID</code> is always assigned as SQS provided message id.</li>
 * <li><code>JMSRedelivered</code> is set to true if SQS delivers the message
 * more than once. This not necessarily mean that the user received message more
 * than once, but rather SQS attempted to deliver it more than once. Due to
 * prefetching used in {@link SQSMessageConsumerPrefetch}, this can be set to
 * true although user never received the message. This is set based on SQS
 * ApproximateReceiveCount attribute</li>
 * <li><code>JMSDestination</code></li> is the destination object which message
 * is sent to and received from.
 * </ul>
 * </P>
 * <p>
 * JMSXDeliveryCount reserved property is supported and set based on the
 * approximate receive count observed on the SQS side.
 */
abstract class SQSMessage implements Message {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    // Define constant message types.
    static final String BYTE_MESSAGE_TYPE = "byte";
    static final String OBJECT_MESSAGE_TYPE = "object";
    static final String TEXT_MESSAGE_TYPE = "text";
    static final String MAP_MESSAGE_TYPE = "map";

    static final String JMS_SQS_MESSAGE_TYPE = "JMS_SQSMessageType";
    static final String JMS_SQS_REPLY_TO_QUEUE_NAME = "JMS_SQSReplyToQueueName";
    static final String JMS_SQS_REPLY_TO_QUEUE_URL = "JMS_SQSReplyToQueueURL";
    static final String JMS_SQS_CORRELATION_ID = "JMS_SQSCorrelationID";

    //region JMS Properties
    private int JMSDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int JMSPriority = Message.DEFAULT_PRIORITY;
    private long JMSTimestamp;
    private boolean JMSRedelivered;
    private String JMSCorrelationID;
    private long JMSExpiration = Message.DEFAULT_TIME_TO_LIVE;
    private String JMSMessageID;
    private String JMSType;
    private Destination JMSReplyTo;
    private Destination JMSDestination;

    private final Map<String, JMSMessagePropertyValue> properties = new HashMap<>();
    //endregion

    @Getter(value = AccessLevel.PACKAGE)
    @Setter(value = AccessLevel.PACKAGE)
    private boolean writePermissionsForProperties;

    @Getter(value = AccessLevel.PACKAGE)
    @Setter(value = AccessLevel.PACKAGE)
    private boolean writePermissionsForBody;

    /**
     * Function for acknowledging message.
     */
    private Acknowledger acknowledger;

    /**
     * Original SQS Message ID.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private String SQSMessageID;

    /**
     * QueueUrl the message came from.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private String queueUrl;

    /**
     * Original SQS Message receipt handle.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private String receiptHandle;

    /**
     * This is called at the receiver side to create a
     * JMS message from the SQS message received.
     */
    SQSMessage(Acknowledger acknowledger,
               String queueUrl,
               com.amazonaws.services.sqs.model.Message sqsMessage) throws JMSException {
        this.acknowledger = acknowledger;
        this.queueUrl = queueUrl;
        this.receiptHandle = sqsMessage.getReceiptHandle();
        setSQSMessageID(sqsMessage.getMessageId());
        Map<String, String> systemAttributes = sqsMessage.getAttributes();
        int receiveCount = Integer.parseInt(systemAttributes.get(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT));

        /*
          JMSXDeliveryCount is set based on SQS ApproximateReceiveCount
          attribute.
         */
        properties.put(SQSMessagingClientConstants.JMSX_DELIVERY_COUNT, PropertyType.INT.createJMSMessagePropertyValue(receiveCount));

        if (receiveCount > 1) {
            setJMSRedelivered(true);
        }

        if (sqsMessage.getMessageAttributes() != null) {
            addMessageAttributes(sqsMessage);
        }

        // map the SequenceNumber, MessageGroupId and MessageDeduplicationId to JMS specific properties
        mapSystemAttributeToJmsMessageProperty(systemAttributes, SQSMessagingClientConstants.SEQUENCE_NUMBER, SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER);
        mapSystemAttributeToJmsMessageProperty(systemAttributes, SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID);
        mapSystemAttributeToJmsMessageProperty(systemAttributes, SQSMessagingClientConstants.MESSAGE_GROUP_ID, SQSMessagingClientConstants.JMSX_GROUP_ID);

        setWritePermissionsForBody(false);
        setWritePermissionsForProperties(false);
    }

    /**
     * Create new empty Message to send. SQSMessage cannot be sent without any
     * payload. One of SQSTextMessage, SQSObjectMessage, or SQSBytesMessage
     * should be used to add payload.
     */
    SQSMessage() {
        setWritePermissionsForBody(true);
        setWritePermissionsForProperties(true);
    }

    //region JMS Member Variable Getter/Setter

    /**
     * Gets the message ID.
     * <p>
     * The JMSMessageID header field contains a value that uniquely identifies
     * each message sent by a provider. It is set to SQS messageId with the
     * prefix 'ID:'.
     *
     * @return the ID of the message.
     */
    @Override
    public String getJMSMessageID() throws JMSException {
        return JMSMessageID;
    }

    /**
     * Sets the message ID. It should have prefix 'ID:'.
     * <p>
     * Set when a message is sent. This method can be used to change the value
     * for a message that has been received.
     *
     * @param id The ID of the message.
     */
    @Override
    public void setJMSMessageID(String id) throws JMSException {
        this.JMSMessageID = id;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return JMSTimestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        this.JMSTimestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return JMSCorrelationID != null ? JMSCorrelationID.getBytes(DEFAULT_CHARSET) : null;
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        this.JMSCorrelationID = correlationID != null ? new String(correlationID, DEFAULT_CHARSET) : null;
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        this.JMSCorrelationID = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return JMSCorrelationID;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return JMSReplyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        if (replyTo != null && !(replyTo instanceof SQSQueueDestination)) {
            throw new IllegalArgumentException("The replyTo Destination must be a SQSQueueDestination");
        }
        this.JMSReplyTo = (SQSQueueDestination) replyTo;
    }

    /**
     * Gets the Destination object for this message.
     * <p>
     * The JMSDestination header field contains the destination to which the
     * message is being sent.
     * <p>
     * When a message is sent, this field is ignored. After completion of the
     * send or publish method, the field holds the destination specified by the
     * method.
     * <p>
     * When a message is received, its JMSDestination value must be equivalent
     * to the value assigned when it was sent.
     *
     * @return The destination of this message.
     */
    @Override
    public Destination getJMSDestination() throws JMSException {
        return JMSDestination;
    }

    /**
     * Sets the Destination object for this message.
     * <p>
     * Set when a message is sent. This method can be used to change the value
     * for a message that has been received.
     *
     * @param destination The destination for this message.
     */
    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.JMSDestination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return JMSDeliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        this.JMSDeliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return JMSRedelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.JMSRedelivered = redelivered;
    }

    @Override
    public String getJMSType() throws JMSException {
        return JMSType;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        this.JMSType = type;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return JMSExpiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        this.JMSExpiration = expiration;
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return JMSPriority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        this.JMSPriority = priority;
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        throw JMSExceptionUtil.UnsupportedMethod().get();
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        throw JMSExceptionUtil.UnsupportedMethod().get();
    }
    //endregion

    //region Get Property Methods
    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.BOOLEAN.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toBoolean(prop.getValue());
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.BYTE.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toByte(prop.getValue());
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.SHORT.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toShort(prop.getValue());
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.INT.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toInt(prop.getValue());
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.LONG.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toLong(prop.getValue());
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.FLOAT.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toFloat(prop.getValue());
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.DOUBLE.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toDouble(prop.getValue());
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        JMSMessagePropertyValue prop = propertyExists(name)
                ? properties.get(name)
                : PropertyType.STRING.createJMSMessagePropertyValue(null);
        return prop.getPropertyType().toString(prop.getValue());
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        JMSMessagePropertyValue propertyValue = getJMSMessagePropertyValue(name);
        if (propertyValue != null) {
            return propertyValue.getValue();
        }
        return null;
    }
    //endregion

    //region Set Property Methods
    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.BOOLEAN);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.BYTE);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.SHORT);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.INT);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.LONG);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.FLOAT);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.DOUBLE);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectPropertyInternal(name, value, PropertyType.STRING);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        PropertyType propertyType = PropertyType.findValidPropertyValueType(name, value);
        setObjectPropertyInternal(name, value, propertyType);
    }

    private void setObjectPropertyInternal(String name, Object value, PropertyType propertyType) throws JMSException {
        handleNullPropertyName(name);
        checkPropertyWritePermissions();
        properties.put(name, propertyType.createJMSMessagePropertyValue(value));
    }
    //endregion

    //region Other Property Methods
    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
        setWritePermissionsForProperties(true);
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        handleNullPropertyName(name);
        return properties.containsKey(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return new PropertyEnum(properties.keySet().iterator());
    }
    //endregion

    //region JMS Other

    /**
     * <p>
     * Acknowledges message(s).
     * <p>
     * A client may individually acknowledge each message as it is consumed, or
     * it may choose to acknowledge multiple messages based on acknowledge mode,
     * which in turn might might acknowledge all messages consumed by the
     * session.
     * <p>
     * Messages that have been received but not acknowledged may be redelivered.
     * <p>
     * If the session is closed, messages cannot be acknowledged.
     * <p>
     * If only the consumer is closed, messages can still be acknowledged.
     *
     * @throws JMSException          On Internal error
     * @throws IllegalStateException If this method is called on a closed session.
     * @see AcknowledgeMode
     */
    @Override
    public void acknowledge() throws JMSException {
        if (acknowledger != null) {
            acknowledger.acknowledge(this);
        }
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return true;
    }
    //endregion

    //region Internal SQS Methods
    void mapSystemAttributeToJmsMessageProperty(Map<String, String> systemAttributes, String systemAttributeName, String jmsMessagePropertyName) throws JMSException {
        String systemAttributeValue = systemAttributes.get(systemAttributeName);
        if (systemAttributeValue != null) {
            properties.put(jmsMessagePropertyName, PropertyType.STRING.createJMSMessagePropertyValue(systemAttributeValue));
        }
    }

    void addMessageAttributes(com.amazonaws.services.sqs.model.Message sqsMessage) throws JMSException {
        for (Entry<String, MessageAttributeValue> entry : sqsMessage.getMessageAttributes().entrySet()) {
            properties.put(entry.getKey(), new JMSMessagePropertyValue(
                    entry.getValue().getStringValue(), entry.getValue().getDataType()));
        }
    }

    void checkPropertyWritePermissions() throws JMSException {
        if (!isWritePermissionsForProperties()) {
            throw new MessageNotWriteableException("Message properties are not writable");
        }
    }

    void checkBodyWritePermissions() throws JMSException {
        if (!isWritePermissionsForBody()) {
            throw new MessageNotWriteableException("Message body is not writable");
        }
    }

    /**
     * Get SQS Message Group Id (applicable for FIFO queues, available also as JMS property 'JMSXGroupId')
     *
     * @throws JMSException exception
     */
    String getSQSMessageGroupId() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID);
    }

    /**
     * Get SQS Message Deduplication Id (applicable for FIFO queues, available also as JMS property 'JMS_SQS_DeduplicationId')
     *
     * @throws JMSException exception
     */
    String getSQSMessageDeduplicationId() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID);
    }

    /**
     * Get SQS Message Sequence Number (applicable for FIFO queues, available also as JMS property 'JMS_SQS_SequenceNumber')
     *
     * @throws JMSException exception
     */
    String getSQSMessageSequenceNumber() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER);
    }

    /**
     * Set SQS Message Id, used on send.
     *
     * @param SQSMessageID messageId assigned by SQS during send.
     */
    void setSQSMessageID(String SQSMessageID) throws JMSException {
        this.SQSMessageID = SQSMessageID;
        this.setJMSMessageID(String.format(SQSMessagingClientConstants.MESSAGE_ID_FORMAT, SQSMessageID));
    }

    private void handleNullPropertyName(String name) {
        if (StringUtils.isNullOrEmpty(name))
            throw new IllegalArgumentException("Property name can not be null or empty.");
    }

    /**
     * Returns the property value with message attribute to object property
     * conversions took place.
     * <p>
     *
     * @param name The name of the property to get.
     * @return <code>JMSMessagePropertyValue</code> with object value and
     * corresponding SQS message attribute type and message attribute
     * string value.
     * @throws JMSException On internal error.
     */
    JMSMessagePropertyValue getJMSMessagePropertyValue(String name) throws JMSException {
        return properties.get(name);
    }

    /**
     * This method sets the JMS_SQS_SEQUENCE_NUMBER property on the message. It is exposed explicitly here, so that
     * it can be invoked even on read-only message object obtained through receing a message.
     * This support the use case of send a received message by using the same JMSMessage object.
     *
     * @param sequenceNumber Sequence number to set. If null or empty, the stored sequence number will be removed.
     * @throws JMSException exception
     */
    void setSequenceNumber(String sequenceNumber) throws JMSException {
        if (sequenceNumber == null || sequenceNumber.isEmpty()) {
            properties.remove(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER);
        } else {
            properties.put(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER, PropertyType.STRING.createJMSMessagePropertyValue(sequenceNumber));
        }
    }

    boolean isEmpty() {
        return false;
    }
    //endregion

}
