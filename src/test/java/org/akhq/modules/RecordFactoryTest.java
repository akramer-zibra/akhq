package org.akhq.modules;

import org.akhq.AbstractTest;
import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordFactoryTest {

    @Test
    public void testCreatePlainRecord() {
        KafkaModule kafkaModule = mock(KafkaModule.class);
        CustomDeserializerRepository customDeserializerRepository = mock(CustomDeserializerRepository.class);
        AvroWireFormatConverter avroWireFormatConverter = mock(AvroWireFormatConverter.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        when(schemaRegistryRepository.determineAvroSchemaForPayload(any(), any())).thenReturn(null);
        RecordFactory underTest = new RecordFactory(kafkaModule, customDeserializerRepository, avroWireFormatConverter, schemaRegistryRepository);

        byte[] key = "anyKey".getBytes(StandardCharsets.UTF_8);
        byte[] value = "anyValue".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("egal", 0, 0, key, value);

        Record akhqRecord = underTest.newRecord(kafkaRecord, "none");
        assertEquals("anyKey", akhqRecord.getKey());
        assertEquals("anyValue", akhqRecord.getValue());
    }

    @Test
    public void testCreateAvroRecord() {
        KafkaModule kafkaModule = mock(KafkaModule.class);
        CustomDeserializerRepository customDeserializerRepository = mock(CustomDeserializerRepository.class);
        AvroWireFormatConverter avroWireFormatConverter = mock(AvroWireFormatConverter.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        when(schemaRegistryRepository.determineAvroSchemaForPayload(any(), any())).thenReturn(1);
        RecordFactory underTest = new RecordFactory(kafkaModule, customDeserializerRepository, avroWireFormatConverter, schemaRegistryRepository);

        String expectedString = "{\"id\":10,\"name\":\"Tiger\",\"weight\":\"10.40\"}";

        // TODO: Test
    }
}