package org.akhq.models.decorators;

import org.akhq.models.Record;

/**
 * This abstract class defines a Record decorator
 */
public abstract class RecordDecorator extends Record {

    /** Wrapped record instance to decorate */
    private Record wrapped;

    /**
     * Constructor takes a record object and uses it as a reference 
     */
    public RecordDecorator(Record record) {
        this.wrapped = record;
    }

    @Override
    public String getKey() {
        return this.wrapped.getKey();
    }

    @Override
    public String getValue() {
        return this.wrapped.getValue();
    }
}
