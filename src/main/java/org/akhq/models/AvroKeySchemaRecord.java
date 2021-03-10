package org.akhq.models;

import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroKeySchemaRecord extends Record {
    private final Deserializer kafkaAvroDeserializer;

    public AvroKeySchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        super(record);
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getKey() {
        if(this.key != null) {
            return this.key;
        }

        try {
            GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(this.topic, this.bytesKey);
            return AvroToJsonSerializer.toJson(record);
        } catch (Exception exception) {
            this.exceptions.add(exception.getMessage());

            return new String(this.bytesKey);
        }
    }

    @Override
    public String getValue() {
        return this.getValue(); // Delegates method call to wrapped record
    }
}
