package org.akhq.modules;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.AbstractTest;
import org.akhq.Breed;
import org.akhq.Cat;
import org.akhq.KafkaTestCluster;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.AvroValueSchemaRecord;
import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordFactoryTest extends AbstractTest {

    @Inject private RecordFactory recordFactory;

    @Test
    @Disabled
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

        Record akhqRecord = underTest.newRecord(kafkaRecord, KafkaTestCluster.CLUSTER_ID);
        assertThat(akhqRecord, instanceOf(Record.class));
        assertThat(akhqRecord.getKey(), is("anyKey"));
        assertThat(akhqRecord.getValue(), is("anyValue"));
    }

    @Test
    @Disabled
    public void testCreateAvroRecord() {
        KafkaModule kafkaModule = mock(KafkaModule.class);
        CustomDeserializerRepository customDeserializerRepository = mock(CustomDeserializerRepository.class);
        AvroWireFormatConverter avroWireFormatConverter = mock(AvroWireFormatConverter.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        when(schemaRegistryRepository.getKafkaAvroDeserializer(anyString())).thenReturn(new KafkaAvroDeserializer());
        when(schemaRegistryRepository.determineAvroSchemaForPayload(any(), any())).thenReturn(null).thenReturn(1);
        RecordFactory underTest = new RecordFactory(kafkaModule, customDeserializerRepository, avroWireFormatConverter, schemaRegistryRepository);

        String jsonValue = "{\"id\":10,\"name\":\"Tiger\",\"weight\":\"10.40\"}";
        byte[] key = "anyKey".getBytes(StandardCharsets.UTF_8);
        byte[] value = jsonValue.getBytes(StandardCharsets.UTF_8);
        byte[] magic = new byte[1 + value.length];

        System.arraycopy(new byte[] {0x0}, 0, magic, 0, 1);
        System.arraycopy(value, 0, magic, 1, value.length);
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("egal", 0, 0, key, magic);

        Record akhqRecord = underTest.newRecord(kafkaRecord, "none");
        assertThat(akhqRecord, instanceOf(AvroValueSchemaRecord.class));
        assertThat(akhqRecord.getValue(), is(jsonValue));
    }

    @Test
    public void testIntegration() throws Exception {
        assertThat(recordFactory, is(notNullValue()));

        byte[] key = "anyKey".getBytes(StandardCharsets.UTF_8);
        String expectedString = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";
        GenericRecord catExample = aCatExample(10, "Tom", Breed.SPHYNX);
        byte[] catRecord = AvroToJsonSerializer.toJson(catExample).getBytes(StandardCharsets.UTF_8);

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("none", 0, 0, key, catRecord);

        Record akhqRecord = recordFactory.newRecord(kafkaRecord, KafkaTestCluster.CLUSTER_ID);
        assertThat(akhqRecord, is(notNullValue()));
        assertThat(akhqRecord.getValue(), is(expectedString));
    }

    private GenericRecord aCatExample(int id, String name, Breed breed) {
        return new GenericRecordBuilder(Cat.SCHEMA$)
                .set("id", id)
                .set("name", name)
                .set("breed", breed)
                .build();
    }
}