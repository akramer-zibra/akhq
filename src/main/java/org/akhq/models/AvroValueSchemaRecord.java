package org.akhq.models;

import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroValueSchemaRecord extends Record {
    private final Record record;
    private final Deserializer kafkaAvroDeserializer;

    public AvroValueSchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        this.record = record;
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getValue() {
        if(this.value == null) {
            try {
                GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(topic, bytesValue);
                this.value = AvroToJsonSerializer.toJson(record);
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.value = new String(bytesValue);
            }
        }

        return this.value;
    }
}
