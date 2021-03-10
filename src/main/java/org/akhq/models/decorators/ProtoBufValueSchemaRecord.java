package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.AvroToJsonSerializer;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoBufValueSchemaRecord extends RecordDecorator {
    private final ProtobufToJsonDeserializer protoBufDeserializer;

    public ProtoBufValueSchemaRecord(Record record, ProtobufToJsonDeserializer protoBufDeserializer) {
        super(record);
        this.protoBufDeserializer = protoBufDeserializer;
    }

    @Override
    public String getValue() {
        if(this.value == null) {
            try {
                String record = protoBufDeserializer.deserialize(this.topic, this.bytesValue, false);
                if (record != null) {
                    this.value = record;
                }
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.value = new String(this.bytesValue);
            }
        }

        return this.value;
    }
}
