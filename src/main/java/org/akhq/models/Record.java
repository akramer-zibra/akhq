package org.akhq.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.utils.AvroToJsonSerializer;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

// TODO: check lombok
public class Record {
    protected String topic;
    private int partition;
    private long offset;
    private ZonedDateTime timestamp;
    @JsonIgnore
    private TimestampType timestampType;
    private Integer keySchemaId;
    private Integer valueSchemaId;
    private Map<String, String> headers = new HashMap<>();
    protected byte[] bytesKey;
    protected String key;
    protected byte[] bytesValue;
    protected String value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return partition == record.partition && offset == record.offset && Objects.equals(topic, record.topic) && Objects.equals(timestamp, record.timestamp) && timestampType == record.timestampType && Objects.equals(keySchemaId, record.keySchemaId) && Objects.equals(valueSchemaId, record.valueSchemaId) && Objects.equals(headers, record.headers) && Arrays.equals(bytesKey, record.bytesKey) && Objects.equals(key, record.key) && Arrays.equals(bytesValue, record.bytesValue) && Objects.equals(value, record.value) && Objects.equals(exceptions, record.exceptions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition, offset, timestamp, timestampType, keySchemaId, valueSchemaId, headers, key, value, exceptions);
        result = 31 * result + Arrays.hashCode(bytesKey);
        result = 31 * result + Arrays.hashCode(bytesValue);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", timestampType=" + timestampType +
                ", keySchemaId=" + keySchemaId +
                ", valueSchemaId=" + valueSchemaId +
                ", headers=" + headers +
                ", bytesKey=" + Arrays.toString(bytesKey) +
                ", key='" + key + '\'' +
                ", bytesValue=" + Arrays.toString(bytesValue) +
                ", value='" + value + '\'' +
                ", exceptions=" + exceptions +
                '}';
    }

    public Record() {
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public TimestampType getTimestampType() {
        return timestampType;
    }

    public Integer getKeySchemaId() {
        return keySchemaId;
    }

    public Integer getValueSchemaId() {
        return valueSchemaId;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBytesKey() {
        return bytesKey;
    }

    public byte[] getBytesValue() {
        return bytesValue;
    }

    public List<String> getExceptions() {
        return exceptions;
    }

    protected final List<String> exceptions = new ArrayList<>();

    public Record(RecordMetadata record, Integer keySchemaId, Integer valueSchemaId, byte[] bytesKey, byte[] bytesValue, Map<String, String> headers) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        this.bytesKey = bytesKey;
        this.keySchemaId = keySchemaId;
        this.bytesValue = bytesValue;
        this.valueSchemaId = valueSchemaId;
        this.headers = headers;
    }

    public Record(ConsumerRecord<byte[], byte[]> record, Integer keySchemaId, Integer valueSchemaId) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        this.timestampType = record.timestampType();
        this.bytesKey = record.key();
        this.keySchemaId = keySchemaId;
        this.bytesValue = record.value();
        this.valueSchemaId = valueSchemaId;
        for (Header header: record.headers()) {
            this.headers.put(header.key(), header.value() != null ? new String(header.value()) : null);
        }
    }

    public String getKey() {
        if(this.bytesKey == null) {
            return null;
        }

        if(this.key == null) {
            this.key = new String(this.bytesKey);
        }
        return this.key;
    }

    public String getValue() {
        if(this.bytesValue == null) {
            return null;
        }

        if(this.value == null) {
            this.value = new String(this.bytesValue);
        }
        return this.value;
    }
}
