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
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

/**
 * This class borrows from <code>ActiveMQStreamMessage</code>, which is also
 * licensed under Apache2.0. Its methods are based largely on those found in
 * <code>java.io.DataInputStream</code> and
 * <code>java.io.DataOutputStream</code>.
 */
@SuppressWarnings("unchecked")
class SQSBytesMessage extends SQSMessage implements BytesMessage {
    private static final Log LOG = LogFactory.getLog(SQSBytesMessage.class);

    private byte[] bytes;

    private ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

    @Setter(value = AccessLevel.PACKAGE)
    private DataInputStream dataIn;

    @Setter(value = AccessLevel.PACKAGE)
    private DataOutputStream dataOut = new DataOutputStream(bytesOut);

    /**
     * Convert received SQSMessage into BytesMessage.
     */
    SQSBytesMessage(Contracts.Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        try {
            /* Bytes is set by the reset() */
            dataOut.write(Base64.decode(sqsMessage.getBody()));
            /* Makes it read-only */
            reset();

        } catch (IOException e) {
            LOG.error("IOException: Message cannot be written", e);
            throw JMSExceptionUtil.convertExceptionToJMSException(e);

        } catch (Exception e) {
            LOG.error("Unexpected exception: ", e);
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    /**
     * Create new empty BytesMessage to send.
     */
    SQSBytesMessage() {
        super();
    }

    @Override
    public long getBodyLength() throws JMSException {
        checkBodyReadPermissions();
        return bytes.length;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readBoolean();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readUnsignedByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readUnsignedShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readChar();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readInt();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readLong();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readFloat();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readDouble();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        checkBodyReadPermissions();
        try {
            return dataIn.readUTF();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        if (length < 0) {
            throw new IndexOutOfBoundsException("Length bytes to read can't be smaller than 0 but was " + length);
        }
        checkBodyReadPermissions();
        try {
            /*
              Almost copy of readFully implementation except that EOFException
              is not thrown if the stream is at the end of file and no byte is
              available
             */
            int n = 0;
            while (n < length) {
                int count = dataIn.read(value, n, length - n);
                if (count < 0) {
                    break;
                }
                n += count;
            }
            /*
              JMS specification mentions that the next read of the stream
              returns -1 if the previous read consumed the byte stream and
              there are no more bytes left to be read from the stream
             */
            if (n == 0 && length > 0) {
                n = -1;
            }
            return n;

        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    byte[] readBytesInternal() {
        if (bytes == null) {
            return bytesOut.toByteArray();
        } else {
            return Arrays.copyOf(bytes, bytes.length);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeByte(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeShort(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeChar(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeInt(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeLong(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeFloat(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeDouble(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.writeUTF(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.write(value);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkBodyWritePermissions();
        try {
            dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw JMSExceptionUtil.convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException("Cannot write null value of object");
        }
        if (value instanceof Boolean) {
            writeBoolean((Boolean) value);
        } else if (value instanceof Character) {
            writeChar((Character) value);
        } else if (value instanceof Byte) {
            writeByte((Byte) value);
        } else if (value instanceof Short) {
            writeShort((Short) value);
        } else if (value instanceof Integer) {
            writeInt((Integer) value);
        } else if (value instanceof Long) {
            writeLong((Long) value);
        } else if (value instanceof Float) {
            writeFloat((Float) value);
        } else if (value instanceof Double) {
            writeDouble((Double) value);
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type: " + value.getClass());
        }
    }

    @Override
    public void reset() throws JMSException {
        setWritePermissionsForBody(false);

        if (dataOut != null) {
            bytes = bytesOut.toByteArray();
            dataOut = null;
            bytesOut = null;
        }
        dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        bytes = null;
        dataIn = null;
        bytesOut = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(bytesOut);
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        if (isBodyAssignableTo(c)) {
            return isEmpty() ? null : c.cast(bytes);
        }
        throw new MessageFormatException("Message is not assignable to class: " + c.getName());
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return isEmpty()
                || c.isAssignableFrom(byte[].class)
                || c.isAssignableFrom(Object.class);
    }

    @Override
    protected boolean isEmpty() {
        return bytes == null || bytes.length == 0;
    }
}
