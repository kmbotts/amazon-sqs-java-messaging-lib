package com.amazon.sqs.javamessaging.acknowledge;

import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import javax.jms.CompletionListener;
import javax.jms.JMSException;
import javax.jms.Message;

import java.util.Optional;

public class SendMessageAsyncHandler implements AsyncHandler<SendMessageRequest, SendMessageResult> {

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

    public SendMessageAsyncHandler(Message message, CompletionListener completionListener) {
        this.message = (SQSMessage) message;
        this.completionListener = Optional.ofNullable(completionListener).orElse(NO_OP);
    }

    @Override
    public void onError(Exception exception) {
        completionListener.onException(message, exception);
    }

    @Override
    public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult) {
        try {
            String messageId = sendMessageResult.getMessageId();
            message.setSQSMessageID(messageId);

            if (sendMessageResult.getSequenceNumber() != null) {
                message.setSequenceNumber(sendMessageResult.getSequenceNumber());
            }
            completionListener.onCompletion(message);
        } catch (JMSException e) {
            onError(e);
        }
    }
}
