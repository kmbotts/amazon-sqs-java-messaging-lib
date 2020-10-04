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
import com.amazonaws.util.StringUtils;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.TextMessage;

/**
 * A TextMessage object is used to send a message body containing a
 * java.lang.String. It inherits from the Message interface and adds a text
 * message body. SQS does not accept empty or null message body
 * <p>
 * When a client receives a TextMessage, it is in read-only mode. If a client
 * attempts to write to the message at this point, a
 * MessageNotWriteableException is thrown. If clearBody is called, the message
 * can now be both read from and written to.
 */
@SuppressWarnings("RedundantThrows")
class SQSTextMessage extends SQSMessage implements TextMessage {

    /**
     * Text of the message. Assume this is safe from SQS invalid characters.
     */
    private String text;

    /**
     * Convert received SQSMessage into TextMessage.
     */
    SQSTextMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        this.text = sqsMessage.getBody();
    }

    /**
     * Create new empty TextMessage to send.
     */
    public SQSTextMessage() throws JMSException {
        super();
    }

    /**
     * Create new TextMessage with payload to send.
     */
    public SQSTextMessage(String payload) throws JMSException {
        super();
        this.text = payload;
    }

    /**
     * Sets the text containing this message's body.
     *
     * @param string The <code>String</code> containing the message's body
     * @throws javax.jms.MessageNotWriteableException If the message is in read-only mode.
     */
    @Override
    public void setText(String string) throws JMSException {
        checkBodyWritePermissions();
        this.text = string;
    }

    /**
     * Gets the text containing this message's body.
     *
     * @return The <code>String</code> containing the message's body
     */
    @Override
    public String getText() throws JMSException {
        return text;
    }

    /**
     * Sets the message body to write mode, and sets the text to null
     */
    @Override
    public void clearBody() throws JMSException {
        text = null;
        setBodyWritePermissions(true);
    }

    @Override
    protected boolean isEmpty() {
        return StringUtils.isNullOrEmpty(text);
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        if (isBodyAssignableTo(c))
            return isEmpty() ? null : c.cast(getText());

        throw new MessageFormatException("Message is not assignable to class: " + c.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return isEmpty() || c.isAssignableFrom(String.class);
    }
}
