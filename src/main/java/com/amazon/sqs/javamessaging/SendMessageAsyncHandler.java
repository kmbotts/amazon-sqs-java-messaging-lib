package com.amazon.sqs.javamessaging;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import javax.jms.CompletionListener;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import java.util.Optional;

class SendMessageAsyncHandler implements AsyncHandler<SendMessageRequest, SendMessageResult> {

    private static final CompletionListener NO_OP = new CompletionListener() {
        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    };

    private final SQSMessage message;
    private final CompletionListener completionListener;

    SendMessageAsyncHandler(SQSMessage message, CompletionListener completionListener) {
        this.message = message;
        this.completionListener = Optional.ofNullable(completionListener).orElse(NO_OP);
    }

    @Override
    public void onError(Exception exception) {
        try {
            message.reset();
            completionListener.onException(message, exception);
        } catch (JMSException e) {
            throw new JMSRuntimeException("Unexpected Error occurred", e.getErrorCode(), e);
        }
    }

    @Override
    public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult) {
        try {
            String messageId = sendMessageResult.getMessageId();
            message.setSQSMessageID(messageId);

            if (sendMessageResult.getSequenceNumber() != null) {
                message.setSequenceNumber(sendMessageResult.getSequenceNumber());
            }
            message.reset();
            completionListener.onCompletion(message);
        } catch (JMSException e) {
            onError(e);
        }
    }
}
