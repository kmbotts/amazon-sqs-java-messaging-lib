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

import com.amazon.sqs.javamessaging.acknowledge.SendMessageAsyncHandler;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;

/**
 * A client uses a MessageProducer object to send messages to a queue
 * destination. A MessageProducer object is created by passing a Destination
 * object to a message-producer creation method supplied by a session.
 * <p>
 * A client also has the option of creating a message producer without supplying
 * a queue destination. In this case, a destination must be provided with every send
 * operation.
 * <p>
 */
public class SQSAsyncMessageProducer extends AbstractMessageProducer {
    private static final Log LOG = LogFactory.getLog(SQSAsyncMessageProducer.class);

    SQSAsyncMessageProducer(AbstractSQSClientWrapper sqsAsyncMessagingClientWrapper,
                            AbstractSession parentSQSSession,
                            Destination destination) throws JMSException {
        super(sqsAsyncMessagingClientWrapper, parentSQSSession, destination);
    }

    @Override
    protected AmazonSQSAsyncMessagingClientWrapper getSqsClientWrapper() {
        return (AmazonSQSAsyncMessagingClientWrapper) super.getSqsClientWrapper();
    }

    @Override
    protected void sendMessageInternal(Queue queue, Message message, CompletionListener listener) throws JMSException {
        checkClosed();
        SQSQueueDestination sqsQueueDestination = checkInvalidDestination(queue);
        SQSMessage sqsMessage = checkMessageFormat(message);

        SendMessageRequest sendMessageRequest = getSendMessageRequest(sqsQueueDestination, message);

        getSqsClientWrapper().sendMessageAsync(sendMessageRequest, new SendMessageAsyncHandler(sqsMessage, listener));
    }
}
