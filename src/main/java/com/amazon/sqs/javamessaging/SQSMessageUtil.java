package com.amazon.sqs.javamessaging;

import com.amazonaws.util.Base64;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SQSMessageUtil {
    private static final Log LOG = LogFactory.getLog(SQSMessageUtil.class);

    /**
     * Deserialize the <code>String</code> into <code>Serializable</code>
     * object.
     */
    static Serializable deserialize(String serialized) throws JMSException {
        if (StringUtils.isNullOrEmpty(serialized)) {
            return null;
        }

        Serializable deserializedObject;
        try {
            byte[] bytes = Base64.decode(serialized);

            try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                deserializedObject = (Serializable) objectInputStream.readObject();

            } catch (IOException e) {
                LOG.error("IOException: Message cannot be written", e);
                throw JMSExceptionUtil.convertExceptionToMessageFormatException(e);
            }

        } catch (Exception e) {
            LOG.error("Unexpected exception: ", e);
            throw JMSExceptionUtil.convertExceptionToMessageFormatException(e);
        }

        return deserializedObject;
    }

    /**
     * Serialize the <code>Serializable</code> object to <code>String</code>.
     */
    static String serialize(Serializable serializable) throws JMSException {
        if (serializable == null) {
            return null;
        }

        String serializedString;

        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(bytesOut)) {

            objectOutputStream.writeObject(serializable);
            objectOutputStream.flush();
            serializedString = Base64.encodeAsString(bytesOut.toByteArray());

        } catch (IOException e) {
            LOG.error("IOException: cannot serialize objectMessage", e);
            throw JMSExceptionUtil.convertExceptionToMessageFormatException(e);
        }

        return serializedString;
    }
}
