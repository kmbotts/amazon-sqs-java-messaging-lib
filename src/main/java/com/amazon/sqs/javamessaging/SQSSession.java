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

import lombok.Builder;

import javax.jms.Destination;
import javax.jms.JMSException;

import java.util.Set;

/**
 * A session serves several purposes:
 * <ul>
 * <li>It is a factory for its message producers and consumers.</li>
 * <li>It provides a way to create Queue objects for those clients that need to
 * dynamically manipulate provider-specific destination names.</li>
 * <li>It retains messages it consumes until they have been acknowledged.</li>
 * <li>It serializes execution of message listeners registered with its message
 * consumers.</li>
 * </ul>
 * <p>
 * Not safe for concurrent use.
 * <p>
 * This session object does not support:
 * <ul>
 * <li>(Temporary)Topic</li>
 * <li>Temporary Queue</li>
 * <li>Browser</li>
 * <li>MapMessage</li>
 * <li>StreamMessage</li>
 * <li>MessageSelector</li>
 * <li>Transactions</li>
 * </ul>
 */
class SQSSession extends AbstractSession {

    @Builder
    SQSSession(AbstractConnection connection,
               AcknowledgeMode acknowledgeMode,
               Set<AbstractMessageConsumer> messageConsumers,
               Set<AbstractMessageProducer> messageProducers) throws JMSException {

        super(connection, acknowledgeMode, messageConsumers, messageProducers);
    }

    @Override
    protected AbstractMessageProducer createMessageProducer(AbstractSQSClientWrapper sqsClientWrapper,
                                                            Destination destination) {
        return SQSMessageProducer.builder()
                .sqsClientWrapper(sqsClientWrapper)
                .destination(destination)
                .session(this)
                .build();
    }
}
