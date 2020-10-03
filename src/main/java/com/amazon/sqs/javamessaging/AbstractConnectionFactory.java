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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import lombok.AccessLevel;
import lombok.Getter;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

import java.util.function.Supplier;

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

public abstract class AbstractConnectionFactory<SQS_CLIENT extends AmazonSQS> implements QueueConnectionFactory {
    @Getter(value = AccessLevel.PROTECTED)
    private final ProviderConfiguration providerConfiguration;
    private final Supplier<SQS_CLIENT> sqsClientSupplier;

    protected AbstractConnectionFactory(ProviderConfiguration providerConfiguration,
                                        AwsSyncClientBuilder<AmazonSQSClientBuilder, SQS_CLIENT> builder) {
        this(providerConfiguration, builder.build());
    }

    /*
     * Creates a SQSConnectionFactory that uses the provided AmazonSQS client connection.
     * Every SQSConnection will use the same provided AmazonSQS client.
     */
    protected AbstractConnectionFactory(ProviderConfiguration providerConfiguration, final SQS_CLIENT client) {
        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (client == null) {
            throw new IllegalArgumentException("AmazonSQS client cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.sqsClientSupplier = () -> client;
    }

    protected abstract QueueConnection createConnection(SQS_CLIENT amazonSQS,
                                                        AWSCredentialsProvider awsCredentialsProvider) throws JMSException;

    //region QueueConnectionFactory Methods
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        try {
            SQS_CLIENT amazonSQS = sqsClientSupplier.get();
            return createConnection(amazonSQS, null);
        } catch (RuntimeException e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    @Override
    public QueueConnection createQueueConnection(String awsAccessKeyId,
                                                 String awsSecretKey) throws JMSException {

        AWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        try {
            SQS_CLIENT amazonSQS = sqsClientSupplier.get();
            return createConnection(amazonSQS, awsCredentialsProvider);
        } catch (Exception e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }
    //endregion

    //region ConnectionFactory Methods
    @Override
    public Connection createConnection() throws JMSException {
        return createQueueConnection();
    }

    @Override
    public Connection createConnection(String awsAccessKeyId, String awsSecretKey) throws JMSException {
        return createQueueConnection(awsAccessKeyId, awsSecretKey);
    }

    //region Unsupported Methods
    /**
     * This method is not supported.
     */
    @Override
    public JMSContext createContext() {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * This method is not supported.
     */
    @Override
    public JMSContext createContext(String userName, String password) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * This method is not supported.
     */
    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * This method is not supported.
     */
    @Override
    public JMSContext createContext(int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }
    //endregion
    //endregion
}
