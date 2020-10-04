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

import lombok.AccessLevel;
import lombok.Builder;

/**
 * This is a logical connection entity, which encapsulates the logic to create
 * sessions.
 * <p>
 * Supports concurrent use, but the session objects it creates do no support
 * concurrent use.
 * <p>
 * The authentication does not take place with the creation of connection. It
 * takes place when the <code>amazonSQSClient</code> is used to call any SQS
 * API.
 * <p>
 * The physical connections are handled by the underlying
 * <code>amazonSQSClient</code>.
 * <p>
 * A JMS client typically creates a connection, one or more sessions, and a
 * number of message producers and consumers. When a connection is created, it
 * is in stopped mode. That means that no messages are being delivered, but
 * message producer can send messages while a connection is stopped.
 * <p>
 * Although the connection can be started immediately, it is typical to leave
 * the connection in stopped mode until setup is complete (that is, until all
 * message consumers have been created). At that point, the client calls the
 * connection's <code>start</code> method, and messages begin arriving at the
 * connection's consumers. This setup convention minimizes any client confusion
 * that may result from asynchronous message delivery while the client is still
 * in the process of setting itself up.
 * <p>
 * A connection can be started immediately, and the setup can be done
 * afterwards. Clients that do this must be prepared to handle asynchronous
 * message delivery while they are still in the process of setting up.
 * <p>
 * Transacted sessions are not supported.
 * <p>
 * Exception listener on connection is not supported.
 */
class SQSConnection extends AbstractConnection {

    @Builder(access = AccessLevel.PACKAGE)
    SQSConnection(AmazonSQSMessagingClientWrapper clientWrapper,
                  ProviderConfiguration providerConfiguration) {
        super(clientWrapper, providerConfiguration);
    }
}
