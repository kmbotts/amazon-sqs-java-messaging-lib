package com.amazon.sqs.javamessaging;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractSQSClientWrapper {
    private static final Log LOG = LogFactory.getLog(AmazonSQSMessagingClientWrapper.class);

    private static final Set<String> SECURITY_EXCEPTION_ERROR_CODES;

    /*
      List of exceptions that can classified as security. These exceptions
      are not thrown during connection-set-up rather after the service
      calls of the <code>AmazonSQSClient</code>.
     */
    static {
        SECURITY_EXCEPTION_ERROR_CODES = new HashSet<>();
        SECURITY_EXCEPTION_ERROR_CODES.add("MissingClientTokenId");
        SECURITY_EXCEPTION_ERROR_CODES.add("InvalidClientTokenId");
        SECURITY_EXCEPTION_ERROR_CODES.add("MissingAuthenticationToken");
        SECURITY_EXCEPTION_ERROR_CODES.add("AccessDenied");
    }

    private final AWSCredentialsProvider credentialsProvider;

    protected AbstractSQSClientWrapper() {
        this(null);
    }

    protected AbstractSQSClientWrapper(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    /**
     * If one uses any other AWS SDK operations other than explicitly listed
     * here, the exceptions thrown by those operations will not be wrapped as
     * <code>JMSException</code>.
     *
     * @return amazonSQSClient
     */
    public abstract AmazonSQS getClient();

    public void shutdown() {
        getClient().shutdown();
    }

    /**
     * Calls <code>deleteMessage</code> and wraps <code>AmazonClientException</code>. This is used to
     * acknowledge single messages, so that they can be deleted from SQS queue.
     *
     * @param deleteMessageRequest Container for the necessary parameters to execute the
     *                             deleteMessage service method on AmazonSQS.
     * @throws JMSException exception
     */
    public void deleteMessage(DeleteMessageRequest deleteMessageRequest) throws JMSException {
        try {
            prepareRequest(deleteMessageRequest);
            getClient().deleteMessage(deleteMessageRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "deleteMessage");
        }
    }

    /**
     * Calls <code>deleteMessageBatch</code> and wraps
     * <code>AmazonClientException</code>. This is used to acknowledge multiple
     * messages on client_acknowledge mode, so that they can be deleted from SQS
     * queue.
     *
     * @param deleteMessageBatchRequest Container for the necessary parameters to execute the
     *                                  deleteMessageBatch service method on AmazonSQS. This is the
     *                                  batch version of deleteMessage. Max batch size is 10.
     * @return The response from the deleteMessageBatch service method, as
     * returned by AmazonSQS
     * @throws JMSException exception
     */
    @SuppressWarnings("UnusedReturnValue")
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws JMSException {
        try {
            prepareRequest(deleteMessageBatchRequest);
            return getClient().deleteMessageBatch(deleteMessageBatchRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "deleteMessageBatch");
        }
    }

    /**
     * Calls <code>sendMessage</code> and wraps
     * <code>AmazonClientException</code>.
     *
     * @param sendMessageRequest Container for the necessary parameters to execute the
     *                           sendMessage service method on AmazonSQS.
     * @return The response from the sendMessage service method, as returned by
     * AmazonSQS
     * @throws JMSException exception
     */
    public SendMessageResult sendMessage(SendMessageRequest sendMessageRequest) throws JMSException {
        try {
            prepareRequest(sendMessageRequest);
            return getClient().sendMessage(sendMessageRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "sendMessage");
        }
    }

    /**
     * Check if the requested queue exists. This function calls
     * <code>GetQueueUrl</code> for the given queue name, returning true on
     * success, false if it gets <code>QueueDoesNotExistException</code>.
     *
     * @param queueName the queue to check
     * @return true if the queue exists, false if it doesn't.
     * @throws JMSException exception
     */
    public boolean queueExists(String queueName) throws JMSException {
        try {
            getClient().getQueueUrl(prepareRequest(new GetQueueUrlRequest(queueName)));
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (AmazonClientException e) {
            throw handleException(e, "getQueueUrl");
        }
    }

    /**
     * Check if the requested queue exists. This function calls
     * <code>GetQueueUrl</code> for the given queue name with the given owner
     * accountId, returning true on success, false if it gets
     * <code>QueueDoesNotExistException</code>.
     *
     * @param queueName           the queue to check
     * @param queueOwnerAccountId The AWS accountId of the account that created the queue
     * @return true if the queue exists, false if it doesn't.
     * @throws JMSException exception
     */
    public boolean queueExists(String queueName, String queueOwnerAccountId) throws JMSException {
        try {
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
            getQueueUrlRequest.setQueueOwnerAWSAccountId(queueOwnerAccountId);
            prepareRequest(getQueueUrlRequest);
            getClient().getQueueUrl(getQueueUrlRequest);
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (AmazonClientException e) {
            throw handleException(e, "getQueueUrl");
        }
    }

    /**
     * Gets the queueUrl of a queue given a queue name.
     *
     * @param queueName queue name
     * @return The response from the GetQueueUrl service method, as returned by
     * AmazonSQS, which will include queue`s URL
     * @throws JMSException exception
     */
    public GetQueueUrlResult getQueueUrl(String queueName) throws JMSException {
        return getQueueUrl(new GetQueueUrlRequest(queueName));
    }

    /**
     * Gets the queueUrl of a queue given a queue name owned by the provided accountId.
     *
     * @param queueName           queue name
     * @param queueOwnerAccountId The AWS accountId of the account that created the queue
     * @return The response from the GetQueueUrl service method, as returned by
     * AmazonSQS, which will include queue`s URL
     * @throws JMSException exception
     */
    public GetQueueUrlResult getQueueUrl(String queueName, String queueOwnerAccountId) throws JMSException {
        return getQueueUrl(new GetQueueUrlRequest(queueName).withQueueOwnerAWSAccountId(queueOwnerAccountId));
    }

    /**
     * Calls <code>getQueueUrl</code> and wraps <code>AmazonClientException</code>
     *
     * @param getQueueUrlRequest Container for the necessary parameters to execute the
     *                           getQueueUrl service method on AmazonSQS.
     * @return The response from the GetQueueUrl service method, as returned by
     * AmazonSQS, which will include queue`s URL
     * @throws JMSException exception
     */
    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws JMSException {
        try {
            prepareRequest(getQueueUrlRequest);
            return getClient().getQueueUrl(getQueueUrlRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "getQueueUrl");
        }
    }

    /**
     * Calls <code>createQueue</code> to create the queue with the default queue attributes,
     * and wraps <code>AmazonClientException</code>
     *
     * @param queueName queue name
     * @return The response from the createQueue service method, as returned by
     * AmazonSQS. This call creates a new queue, or returns the URL of
     * an existing one.
     * @throws JMSException exception
     */
    @SuppressWarnings("UnusedReturnValue")
    public CreateQueueResult createQueue(String queueName) throws JMSException {
        return createQueue(new CreateQueueRequest(queueName));
    }

    /**
     * Calls <code>createQueue</code> to create the queue with the provided queue attributes
     * if any, and wraps <code>AmazonClientException</code>
     *
     * @param createQueueRequest Container for the necessary parameters to execute the
     *                           createQueue service method on AmazonSQS.
     * @return The response from the createQueue service method, as returned by
     * AmazonSQS. This call creates a new queue, or returns the URL of
     * an existing one.
     * @throws JMSException exception
     */
    public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest) throws JMSException {
        try {
            prepareRequest(createQueueRequest);
            return getClient().createQueue(createQueueRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "createQueue");
        }
    }

    /**
     * Calls <code>receiveMessage</code> and wraps <code>AmazonClientException</code>. Used by
     * {@link SQSMessageConsumerPrefetch} to receive up to minimum of
     * (<code>numberOfMessagesToPrefetch</code>,10) messages from SQS queue into consumer
     * prefetch buffers.
     *
     * @param receiveMessageRequest Container for the necessary parameters to execute the
     *                              receiveMessage service method on AmazonSQS.
     * @return The response from the ReceiveMessage service method, as returned
     * by AmazonSQS.
     * @throws JMSException exception
     */
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws JMSException {
        try {
            prepareRequest(receiveMessageRequest);
            return getClient().receiveMessage(receiveMessageRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "receiveMessage");
        }
    }

    /**
     * Calls <code>changeMessageVisibility</code> and wraps <code>AmazonClientException</code>. This is
     * used to for negative acknowledge of a single message, so that messages can be received again without any delay.
     *
     * @param changeMessageVisibilityRequest Container for the necessary parameters to execute the
     *                                       changeMessageVisibility service method on AmazonSQS.
     * @throws JMSException exception
     */
    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws JMSException {
        try {
            prepareRequest(changeMessageVisibilityRequest);
            getClient().changeMessageVisibility(changeMessageVisibilityRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "changeMessageVisibility");
        }
    }

    /**
     * Calls <code>changeMessageVisibilityBatch</code> and wraps <code>AmazonClientException</code>. This is
     * used to for negative acknowledge of messages in batch, so that messages
     * can be received again without any delay.
     *
     * @param changeMessageVisibilityBatchRequest Container for the necessary parameters to execute the
     *                                            changeMessageVisibilityBatch service method on AmazonSQS.
     * @return The response from the changeMessageVisibilityBatch service
     * method, as returned by AmazonSQS.
     * @throws JMSException exception
     */
    @SuppressWarnings("UnusedReturnValue")
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
            throws JMSException {
        try {
            prepareRequest(changeMessageVisibilityBatchRequest);
            return getClient().changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "changeMessageVisibilityBatch");
        }
    }

    /**
     * Create generic error message for <code>AmazonServiceException</code>. Message include
     * Action, RequestId, HTTPStatusCode, and AmazonErrorCode.
     */
    private String logAndGetAmazonServiceException(AmazonServiceException ase, String action) {
        String errorMessage = "AmazonServiceException: " + action + ". RequestId: " + ase.getRequestId() +
                "\nHTTPStatusCode: " + ase.getStatusCode() + " AmazonErrorCode: " +
                ase.getErrorCode();
        LOG.error(errorMessage, ase);
        return errorMessage;
    }

    /**
     * Create generic error message for <code>AmazonClientException</code>. Message include
     * Action.
     */
    private String logAndGetAmazonClientException(AmazonClientException ace, String action) {
        String errorMessage = "AmazonClientException: " + action + ".";
        LOG.error(errorMessage, ace);
        return errorMessage;
    }

    protected JMSException handleException(AmazonClientException e, String operationName) {
        JMSException jmsException;
        if (e instanceof AmazonServiceException) {
            AmazonServiceException se = (AmazonServiceException) e;

            if (e instanceof QueueDoesNotExistException) {
                jmsException = new InvalidDestinationException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            } else if (isJMSSecurityException(se)) {
                jmsException = new JMSSecurityException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            } else {
                jmsException = new JMSException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            }

        } else {
            jmsException = new JMSException(logAndGetAmazonClientException(e, operationName));
        }
        jmsException.initCause(e);
        return jmsException;
    }

    private boolean isJMSSecurityException(AmazonServiceException e) {
        return SECURITY_EXCEPTION_ERROR_CODES.contains(e.getErrorCode());
    }

    protected <T extends AmazonWebServiceRequest> T prepareRequest(T request) {
        request.getRequestClientOptions().appendUserAgent(SQSMessagingClientConstants.APPENDED_USER_AGENT_HEADER_VERSION);
        if (credentialsProvider != null) {
            request.setRequestCredentialsProvider(credentialsProvider);
        }
        return request;
    }
}
