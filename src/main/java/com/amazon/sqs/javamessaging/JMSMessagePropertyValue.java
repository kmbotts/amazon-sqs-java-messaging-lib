package com.amazon.sqs.javamessaging;

import lombok.Data;

import javax.jms.JMSException;

import java.util.Objects;

/**
 * This class is used fulfill object value, corresponding SQS message
 * attribute type and message attribute string value.
 */
@Data
class JMSMessagePropertyValue {

    private final PropertyType propertyType;

    private final Object value;

    private final String stringMessageAttributeValue;

    JMSMessagePropertyValue(String stringValue, String propertyType) throws JMSException {
        this.propertyType = PropertyType.fromType(propertyType);
        this.value = this.propertyType.fromString(stringValue);
        this.stringMessageAttributeValue = stringValue;
    }

    JMSMessagePropertyValue(Object value, PropertyType propertyType) {
        this.propertyType = propertyType;
        this.value = value;
        this.stringMessageAttributeValue = Objects.toString(value, null);
    }
}
