package org.akhq.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RecordTest {

    @Test
    public void testKeyByteArrayIsNullWithDecorator() {

        // GIVEN a record with null in key
        // WHEN getKey() method is called
        // EXPECT NULL result
        Assertions.fail();
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
