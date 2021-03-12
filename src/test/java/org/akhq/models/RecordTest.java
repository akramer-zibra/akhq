package org.akhq.models;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class RecordTest {

    @Test
    public void testKeyByteArrayIsNull() {

        // GIVEN a record with null in key
        byte[] keyBytes = null;
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8); // value property does not matter
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, null, null);

        // WHEN getKey() method is called
        String keyString = record.getKey();

        // EXPECT NULL result
        Assertions.assertNull(keyString);
    }

    @Test
    public void testKeyIsAvroSerializedWithDecorator() {

        // GIVEN a record with an avro serialized key
        // AND a schema id is given
        // WHEN getKey() method is called
        // EXPECT a json result in String type
        Assertions.fail();
    }

    @Test
    public void testKeyIsAvroSerializedFallbackWithDecorator() {

        // GIVEN a record with an avro serialized key
        // AND a schema id is given
        // WHEN getKey() method is called
        // AND kafka avro deserializer throws an exception
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }

    @Test
    public void testKeyIsProtobufSerializedWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id is given
        // AND protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // EXPECT a protobuf deserialized value in String type as result
        Assertions.fail();
    }

    @Test
    public void testKeyIsProtobufSerializedFallbackWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id is given
        // AND protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // AND protobuf deserializer throws an exception
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }

    @Test
    public void testKeyDefaultFallbackWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id exists
        // AND no protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }
}
