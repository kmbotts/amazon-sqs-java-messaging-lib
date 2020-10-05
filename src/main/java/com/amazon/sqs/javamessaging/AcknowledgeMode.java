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

import javax.jms.JMSException;

/**
 * <p>
 * Specifies the different possible modes of acknowledgment:
 * <ul>
 * <li>In ACK_AUTO mode, every time the user receives a message, it is
 * acknowledged automatically. This is implemented by consumer and does not need
 * any further processing.</li>
 * <li>In ACK_RANGE mode, all messages received before that message including
 * that one are acknowledged.</li>
 * <li>In ACK_UNORDERED mode, messages can be acknowledged in any order one at a
 * time. Acknowledging the consumed message does not affect other message
 * acknowledges.</li>
 * </ul>
 */
enum AcknowledgeMode {
    ACK_AUTO, ACK_UNORDERED, ACK_RANGE;

    private int originalAcknowledgeMode;

    /**
     * Sets the acknowledge mode.
     */
    AcknowledgeMode withOriginalAcknowledgeMode(int originalAcknowledgeMode) {
        this.originalAcknowledgeMode = originalAcknowledgeMode;
        return this;
    }

    /**
     * Returns the acknowledge mode.
     */
    int getOriginalAcknowledgeMode() {
        return originalAcknowledgeMode;
    }

    /**
     * Creates the acknowledger associated with the session, which will be used
     * to acknowledge the delivered messages on consumers of the session.
     *
     * @param sqsClientWrapper the SQS client to delete messages
     * @param session          the associated session for the acknowledger
     * @throws JMSException If invalid acknowledge mode is used.
     */
    Contracts.Acknowledger createAcknowledger(AbstractSQSClientWrapper sqsClientWrapper, AbstractSession session) throws JMSException {
        switch (this) {
            case ACK_AUTO:
                return new AutoAcknowledger(sqsClientWrapper, session);
            case ACK_RANGE:
                return new RangedAcknowledger(sqsClientWrapper, session);
            case ACK_UNORDERED:
                return new UnorderedAcknowledger(sqsClientWrapper, session);
            default:
                throw new JMSException(this + " - AcknowledgeMode does not exist");
        }
    }
}
