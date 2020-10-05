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

import com.amazonaws.services.sqs.model.DeleteMessageRequest;

import javax.jms.JMSException;

import java.util.Collections;
import java.util.List;

/**
 * Used by session to automatically acknowledge a client's receipt of a message
 * either when the session has successfully returned from a call to receive or
 * when the message listener the session has called to process the message
 * successfully returns.
 */
class AutoAcknowledger implements Contracts.Acknowledger {

    private final AbstractSQSClientWrapper sqsClientWrapper;
    private final AbstractSession session;

    AutoAcknowledger(AbstractSQSClientWrapper sqsClientWrapper, AbstractSession session) {
        this.sqsClientWrapper = sqsClientWrapper;
        this.session = session;
    }

    /**
     * Acknowledges the consumed message via calling <code>deleteMessage</code>
     */
    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();
        sqsClientWrapper.deleteMessage(new DeleteMessageRequest(
                message.getQueueUrl(), message.getReceiptHandle()));
    }

    /**
     * When notify message is received, it will acknowledge the message.
     */
    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        acknowledge(message);
    }

    /**
     * AutoAcknowledge doesn't need to do anything in this method. Return an
     * empty list.
     */
    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return Collections.emptyList();
    }

    /**
     * AutoAcknowledge doesn't need to do anything in this method.
     */
    @Override
    public void forgetUnAckMessages() {
    }
}
