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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.junit.Test;

import javax.jms.JMSException;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;


public class SQSConnectionFactoryTest {

    @Test
    public void canCreateFactoryWithDefaultProviderSettings() throws JMSException {
        SQSConnectionFactory factory = new SQSConnectionFactory(ProviderConfiguration.DEFAULT);
        //cannot actually attempt to create a connection because the default client builder depends on environment settings or instance configuration to be present
        //which we cannot guarantee on the builder fleet
    }

    @Test
    public void canCreateFactoryWithCustomClient() throws JMSException {
        AmazonSQS client = mock(AmazonSQS.class);
        SQSConnectionFactory factory = new SQSConnectionFactory(ProviderConfiguration.DEFAULT, client);
        SQSConnection connection = (SQSConnection) factory.createConnection();
        connection.close();
    }

    @Test
    public void factoryWithCustomClientWillUseTheSameClient() throws JMSException {
        AmazonSQS client = mock(AmazonSQS.class);
        SQSConnectionFactory factory = new SQSConnectionFactory(ProviderConfiguration.DEFAULT, client);
        SQSConnection connection1 = (SQSConnection) factory.createConnection();
        SQSConnection connection2 = (SQSConnection) factory.createConnection();

        assertSame(client, connection1.getSqsClientWrapper().getClient());
        assertSame(client, connection2.getSqsClientWrapper().getClient());
        assertSame(connection1.getSqsClientWrapper().getClient(), connection2.getSqsClientWrapper().getClient());

        connection1.close();
        connection2.close();
    }

    @Test
    public void canCreateFactoryWithCustomBuilder() throws JMSException {
        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1);
        SQSConnectionFactory factory = new SQSConnectionFactory(ProviderConfiguration.DEFAULT, clientBuilder);
        SQSConnection connection = (SQSConnection) factory.createConnection();
        connection.close();
    }

    @Test
    public void factoryWithCustomBuilderWillCreateNewClient() throws JMSException {
        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1);
        SQSConnectionFactory factory = new SQSConnectionFactory(ProviderConfiguration.DEFAULT, clientBuilder);
        SQSConnection connection1 = (SQSConnection) factory.createConnection();
        SQSConnection connection2 = (SQSConnection) factory.createConnection();

        assertNotSame(connection1.getSqsClientWrapper().getClient(), connection2.getSqsClientWrapper().getClient());

        connection1.close();
        connection2.close();
    }
}
