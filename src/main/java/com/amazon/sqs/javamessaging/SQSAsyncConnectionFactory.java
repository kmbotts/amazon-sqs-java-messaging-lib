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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;

import javax.jms.JMSException;
import javax.jms.QueueConnection;

/**
 * A ConnectionFactory object encapsulates a set of connection configuration
 * parameters for <code>AmazonSQSClient</code> as well as setting
 * <code>numberOfMessagesToPrefetch</code>.
 * <p>
 * The <code>numberOfMessagesToPrefetch</code> parameter is used to size of the
 * prefetched messages, which can be tuned based on the application workload. It
 * helps in returning messages from internal buffers(if there is any) instead of
 * waiting for the SQS <code>receiveMessage</code> call to return.
 * <p>
 * If more physical connections than the default maximum value (that is 50 as of
 * today) are needed on the connection pool,
 * {@link ClientConfiguration} needs to be configured.
 * <p>
 * None of the <code>createConnection</code> methods set-up the physical
 * connection to SQS, so validity of credentials are not checked with those
 * methods.
 */

public class SQSAsyncConnectionFactory extends AbstractConnectionFactory {

    private final AmazonSQSAsync amazonSQSAsync;

    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration) {
        this(providerConfiguration, AmazonSQSAsyncClientBuilder.standard());
    }

    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration, AmazonSQSAsyncClientBuilder builder) {
        this(providerConfiguration, builder.build());
    }

    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration, AmazonSQSAsync amazonSQSAsync) {
        super(providerConfiguration);
        this.amazonSQSAsync = amazonSQSAsync;
    }

    @Override
    protected QueueConnection createConnection(AWSCredentialsProvider awsCredentialsProvider) throws JMSException {
        AmazonSQSAsyncMessagingClientWrapper clientWrapper = new AmazonSQSAsyncMessagingClientWrapper(amazonSQSAsync, awsCredentialsProvider);
        return new SQSAsyncConnection(clientWrapper, getProviderConfiguration());
    }
}
