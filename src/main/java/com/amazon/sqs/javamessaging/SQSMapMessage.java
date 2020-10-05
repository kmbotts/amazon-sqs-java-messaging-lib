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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;
import javax.jms.MapMessage;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("RedundantThrows")
class SQSMapMessage extends SQSMessage implements MapMessage {
    private static final Log LOG = LogFactory.getLog(SQSMapMessage.class);

    /**
     * Text of the message. Assume this is safe from SQS invalid characters.
     */
    private final ConcurrentHashMap<String, Object> map;

    /**
     * Convert received SQSMessage into TextMessage.
     */
    SQSMapMessage(Contracts.Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        map = (ConcurrentHashMap<String, Object>) SQSMessageUtil.deserialize(sqsMessage.getBody());
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return false;
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return 0;
    }

    @Override
    public short getShort(String name) throws JMSException {
        return 0;
    }

    @Override
    public char getChar(String name) throws JMSException {
        return 0;
    }

    @Override
    public int getInt(String name) throws JMSException {
        return 0;
    }

    @Override
    public long getLong(String name) throws JMSException {
        return 0;
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return 0;
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return 0;
    }

    @Override
    public String getString(String name) throws JMSException {
        return null;
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return new byte[0];
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return null;
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return map.keys();
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {

    }

    @Override
    public void setByte(String name, byte value) throws JMSException {

    }

    @Override
    public void setShort(String name, short value) throws JMSException {

    }

    @Override
    public void setChar(String name, char value) throws JMSException {

    }

    @Override
    public void setInt(String name, int value) throws JMSException {

    }

    @Override
    public void setLong(String name, long value) throws JMSException {

    }

    @Override
    public void setFloat(String name, float value) throws JMSException {

    }

    @Override
    public void setDouble(String name, double value) throws JMSException {

    }

    @Override
    public void setString(String name, String value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {

    }

    @Override
    public void setObject(String name, Object value) throws JMSException {

    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return false;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        map.clear();
    }
}
