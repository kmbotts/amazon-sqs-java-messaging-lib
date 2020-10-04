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
import com.amazonaws.util.Base64;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * An ObjectMessage object is used to send a message that contains a Java
 * serializable object.
 * <p>
 * It inherits from the Message interface and adds a body containing a single
 * reference to an object. Only Serializable Java objects can be used.
 * <p>
 * When a client receives an ObjectMessage, it is in read-only mode. If a client
 * attempts to write to the message at this point, a
 * MessageNotWriteableException is thrown. If clearBody is called, the message
 * can now be both read from and written to.
 */
class SQSObjectMessage extends SQSMessage implements ObjectMessage {
    private static final Log LOG = LogFactory.getLog(SQSObjectMessage.class);

    /**
     * Serialized message body
     */
    private String body;

    /**
     * Convert received SQSMessage into ObjectMessage
     */
    SQSObjectMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        body = sqsMessage.getBody();
    }

    /**
     * Create new empty ObjectMessage to send.
     */
    public SQSObjectMessage() throws JMSException {
        super();
    }

    /**
     * Create new ObjectMessage with payload to send.
     */
    public SQSObjectMessage(Serializable payload) throws JMSException {
        super();
        body = serialize(payload);
    }

    /**
     * Sets the <code>Serializable</code> containing this message's body
     *
     * @param payload The <code>Serializable</code> containing the message's body
     * @throws javax.jms.MessageNotWriteableException If the message is in read-only mode.
     * @throws MessageFormatException                 If object serialization fails.
     */
    @Override
    public void setObject(Serializable payload) throws JMSException {
        checkBodyWritePermissions();
        body = serialize(payload);
    }

    /**
     * Gets the <code>Serializable</code> containing this message's body
     *
     * @throws MessageFormatException If object deserialization fails.
     */
    @Override
    public Serializable getObject() throws JMSException {
        return deserialize(body);
    }

    /**
     * Sets the message body to write mode, and sets the object body to null
     */
    @Override
    public void clearBody() throws JMSException {
        body = null;
        setBodyWritePermissions(true);
    }

    /**
     * Deserialize the <code>String</code> into <code>Serializable</code>
     * object.
     */
    protected static Serializable deserialize(String serialized) throws JMSException {
        if (serialized == null) {
            return null;
        }
        Serializable deserializedObject;
        ObjectInputStream objectInputStream = null;
        try {
            byte[] bytes = Base64.decode(serialized);
            objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            deserializedObject = (Serializable) objectInputStream.readObject();
        } catch (IOException e) {
            LOG.error("IOException: Message cannot be written", e);
            throw convertExceptionToMessageFormatException(e);
        } catch (Exception e) {
            LOG.error("Unexpected exception: ", e);
            throw convertExceptionToMessageFormatException(e);
        } finally {
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
        return deserializedObject;
    }

    /**
     * Serialize the <code>Serializable</code> object to <code>String</code>.
     */
    protected static String serialize(Serializable serializable) throws JMSException {
        if (serializable == null) {
            return null;
        }
        String serializedString;
        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(bytesOut);
            objectOutputStream.writeObject(serializable);
            objectOutputStream.flush();
            serializedString = Base64.encodeAsString(bytesOut.toByteArray());
        } catch (IOException e) {
            LOG.error("IOException: cannot serialize objectMessage", e);
            throw convertExceptionToMessageFormatException(e);
        } finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
        return serializedString;
    }

    public String getMessageBody() {
        return body;
    }

    void setMessageBody(String body) {
        this.body = body;
    }

    @Override
    protected boolean isEmpty() {
        return StringUtils.isNullOrEmpty(body);
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        try {
            if (isBodyAssignableTo(c)) {
                Object obj = getObject();
                return obj == null ? null : c.cast(obj);
            }
            throw new MessageFormatException("Message is not assignable to class: " + c.getName());

        } catch (JMSException e) {
            throw new MessageFormatException("Object deserialization failed", e.getErrorCode());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return isEmpty()
                || c.isAssignableFrom(getObject().getClass())
                || c.isAssignableFrom(Serializable.class);
    }
}
