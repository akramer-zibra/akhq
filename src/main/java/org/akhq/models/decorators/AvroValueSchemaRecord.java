package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroValueSchemaRecord extends RecordDecorator {
    private final Deserializer kafkaAvroDeserializer;

    public AvroValueSchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        super(record);
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getValue() {
        if(this.value == null) {
            try {
                String parentTopic = this.topic;
                byte[] parentBytesValue = this.bytesValue;
                GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(parentTopic, parentBytesValue);
                this.value = AvroToJsonSerializer.toJson(record);
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.value = new String(this.bytesValue);
            }
        }

        return this.value;
    }
}
