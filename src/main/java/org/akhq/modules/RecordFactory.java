package org.akhq.modules;

import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.*;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.RecordRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RecordFactory {

    private final KafkaModule kafkaModule;
    private final CustomDeserializerRepository customDeserializerRepository;
    private final AvroWireFormatConverter avroWireFormatConverter;
    private final SchemaRegistryRepository schemaRegistryRepository;

    public RecordFactory(KafkaModule kafkaModule,
                         CustomDeserializerRepository customDeserializerRepository,
                         AvroWireFormatConverter avroWireFormatConverter,
                         SchemaRegistryRepository schemaRegistryRepository) {
        this.kafkaModule = kafkaModule;
        this.customDeserializerRepository = customDeserializerRepository;
        this.avroWireFormatConverter = avroWireFormatConverter;
        this.schemaRegistryRepository = schemaRegistryRepository;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(clusterId);
        Integer keySchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.key());
        Integer valueSchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.value());

        Record akhqRecord = new Record(record, keySchemaId, valueSchemaId);

        // TODO: ,
        //                avroWireFormatConverter.convertValueToWireFormat(record, this.kafkaModule.getRegistryClient(clusterId),
        //                        this.schemaRegistryRepository.getSchemaRegistryType(clusterId)

        Deserializer kafkaAvroDeserializer = this.schemaRegistryRepository.getKafkaAvroDeserializer(clusterId);
        ProtobufToJsonDeserializer protobufToJsonDeserializer = customDeserializerRepository.getProtobufToJsonDeserializer(clusterId);

        if(keySchemaId != null) {
            akhqRecord = new AvroKeySchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else if(protobufToJsonDeserializer != null) {
            akhqRecord = new ProtoBufKeySchemaRecord(akhqRecord, protobufToJsonDeserializer);
        }

        if(valueSchemaId != null) {
            akhqRecord = new AvroValueSchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else if (protobufToJsonDeserializer != null) {
            akhqRecord = new ProtoBufValueSchemaRecord(akhqRecord, protobufToJsonDeserializer);
        }

        return akhqRecord;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, RecordRepository.BaseOptions options) {
        return this.newRecord(record, options.getClusterId());
    }
}