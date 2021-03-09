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
                String parentTopic = this.record.topic;
                byte[] parentBytesValue = this.record.bytesValue;
                GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(parentTopic, parentBytesValue);
                this.value = AvroToJsonSerializer.toJson(record);
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.value = new String(this.record.bytesValue);
            }
        }

        return this.value;
    }
}
