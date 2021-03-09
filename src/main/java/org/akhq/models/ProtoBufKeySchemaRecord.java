package org.akhq.models;

import org.akhq.utils.ProtobufToJsonDeserializer;

public class ProtoBufKeySchemaRecord extends Record {
    private final ProtobufToJsonDeserializer protoBufDeserializer;

    public ProtoBufKeySchemaRecord(Record record, ProtobufToJsonDeserializer protoBufDeserializer) {
        super(record);
        this.protoBufDeserializer = protoBufDeserializer;
    }

    @Override
    public String getKey() {
        if(this.key == null) {
            try {
                String record = protoBufDeserializer.deserialize(this.record.topic, this.record.bytesKey, true);
                if (record != null) {
                    this.key = record;
                }
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.key = new String(this.record.bytesKey);
            }
        }

        return this.key;
    }

    @Override
    public String getValue() {
        return this.record.getValue(); // Delegates method call to wrapped record
    }
}
