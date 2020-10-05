package com.amazon.sqs.javamessaging;

import lombok.AccessLevel;
import lombok.Getter;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Callable;

enum PropertyType {
    BOOLEAN("String.Boolean", Boolean.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toBoolean(value), this);
        }

        @Override
        boolean toBoolean(Object value) throws JMSException {
            return convertInternal(() -> Boolean.parseBoolean(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toBoolean(value) || "1".equalsIgnoreCase(value);
        }
    },
    BYTE("Number.byte", Byte.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toByte(value), this);
        }

        @Override
        byte toByte(Object value) throws JMSException {
            return convertInternal(() -> Byte.parseByte(toString(value)));
        }

        @Override
        short toShort(Object value) throws JMSException {
            return convertInternal(() -> Short.parseShort(toString(value)));
        }

        @Override
        int toInt(Object value) throws JMSException {
            return convertInternal(() -> Integer.parseInt(toString(value)));
        }

        @Override
        long toLong(Object value) throws JMSException {
            return convertInternal(() -> Long.parseLong(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toByte(value);
        }
    },
    SHORT("Number.short", Short.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toShort(value), this);
        }

        @Override
        short toShort(Object value) throws JMSException {
            return convertInternal(() -> Short.parseShort(toString(value)));
        }

        @Override
        int toInt(Object value) throws JMSException {
            return convertInternal(() -> Integer.parseInt(toString(value)));
        }

        @Override
        long toLong(Object value) throws JMSException {
            return convertInternal(() -> Long.parseLong(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toShort(value);
        }
    },
    INT("Number.int", Integer.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toInt(value), this);
        }

        @Override
        int toInt(Object value) throws JMSException {
            return convertInternal(() -> Integer.parseInt(toString(value)));
        }

        @Override
        long toLong(Object value) throws JMSException {
            return convertInternal(() -> Long.parseLong(toString(value)));
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toInt(value);
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }
    },
    LONG("Number.long", Long.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toLong(value), this);
        }

        @Override
        long toLong(Object value) throws JMSException {
            return convertInternal(() -> Long.parseLong(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toLong(value);
        }
    },
    FLOAT("Number.float", Float.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toFloat(value), this);
        }

        @Override
        float toFloat(Object value) throws JMSException {
            return convertInternal(() -> Float.parseFloat(toString(value)));
        }

        @Override
        double toDouble(Object value) throws JMSException {
            return convertInternal(() -> Double.parseDouble(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toFloat(value);
        }
    },
    DOUBLE("Number.double", Double.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException {
            return new JMSMessagePropertyValue(toDouble(value), this);
        }

        @Override
        double toDouble(Object value) throws JMSException {
            return convertInternal(() -> Double.valueOf(toString(value)));
        }

        @Override
        String toString(Object value) {
            return String.valueOf(value);
        }

        @Override
        Object fromString(String value) throws JMSException {
            return toDouble(value);
        }
    },
    STRING("String", String.class) {
        @Override
        JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) {
            return new JMSMessagePropertyValue(toString(value), this);
        }

        @Override
        boolean toBoolean(Object value) throws JMSException {
            return convertInternal(() -> Boolean.parseBoolean(toString(value)));
        }

        @Override
        byte toByte(Object value) throws JMSException {
            return convertInternal(() -> Byte.parseByte(toString(value)));
        }

        @Override
        short toShort(Object value) throws JMSException {
            return convertInternal(() -> Short.parseShort(toString(value)));
        }

        @Override
        int toInt(Object value) throws JMSException {
            return convertInternal(() -> Integer.parseInt(toString(value)));
        }

        @Override
        long toLong(Object value) throws JMSException {
            return convertInternal(() -> Long.parseLong(toString(value)));
        }

        @Override
        float toFloat(Object value) throws JMSException {
            return convertInternal(() -> Float.parseFloat(toString(value)));
        }

        @Override
        double toDouble(Object value) throws JMSException {
            return convertInternal(() -> Double.parseDouble(toString(value)));
        }

        @Override
        Object fromString(String value) {
            return toString(value);
        }
    };

    @Getter(value = AccessLevel.PACKAGE)
    private final String type;
    private final Class<?> typeClass;

    PropertyType(String type, Class<?> typeClass) {
        this.type = type;
        this.typeClass = typeClass;
    }

    static PropertyType fromType(String type) {
        return Arrays.stream(values())
                .filter(p -> p.getType().equalsIgnoreCase(type))
                .findFirst()
                .orElse(STRING);
    }

    static PropertyType findValidPropertyValueType(String name, Object value) throws MessageFormatException {
        Class<?> temp = value == null ? String.class : value.getClass();
        return Arrays.stream(values())
                .filter(p -> p.typeClass.equals(temp))
                .findFirst()
                .orElseThrow(() -> new MessageFormatException("Value of property with name " + name + " has incorrect type " + temp.getName() + "."));
    }

    abstract JMSMessagePropertyValue createJMSMessagePropertyValue(Object value) throws JMSException;

    abstract Object fromString(String value) throws JMSException;

    boolean toBoolean(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(boolean.class, value);
    }

    byte toByte(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(byte.class, value);
    }

    short toShort(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(short.class, value);
    }

    int toInt(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(int.class, value);
    }

    long toLong(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(long.class, value);
    }

    float toFloat(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(float.class, value);
    }

    double toDouble(Object value) throws JMSException {
        throw JMSExceptionUtil.conversionUnsupportedException(double.class, value);
    }

    String toString(Object value) {
        return Objects.toString(value, null);
    }

    private static <T> T convertInternal(Callable<T> callable) throws MessageFormatException {
        try {
            return callable.call();
        } catch (Exception e) {
            throw JMSExceptionUtil.convertExceptionToMessageFormatException(e);
        }
    }

}
