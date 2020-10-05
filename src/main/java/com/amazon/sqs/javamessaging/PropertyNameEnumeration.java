package com.amazon.sqs.javamessaging;

import java.util.Enumeration;
import java.util.Iterator;

class PropertyNameEnumeration implements Enumeration<String> {
    private final Iterator<String> propertyItr;

    public PropertyNameEnumeration(Iterator<String> propertyItr) {
        this.propertyItr = propertyItr;
    }

    @Override
    public boolean hasMoreElements() {
        return propertyItr.hasNext();
    }

    @Override
    public String nextElement() {
        return propertyItr.next();
    }
}
