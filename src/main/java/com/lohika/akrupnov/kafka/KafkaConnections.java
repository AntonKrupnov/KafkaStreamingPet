package com.lohika.akrupnov.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

class KafkaConnections {

    private static final String EARLIEST = "earliest";
    private static final String BROKER_HOST = "localhost:29092";
    private static final String ID_SERIALIZER = StringSerializer.class.getName();
    private static final String ID_DESERIALIZER = StringDeserializer.class.getName();
    private static final String VALUE_SERIALIZER = ByteArraySerializer.class.getName();

    static void createTopics() {
        for (Topics topic : Topics.values()) {
            createTopicIfNotExists(topic.name());
        }
    }

    static void createTopicIfNotExists(String topicName) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin");

        AdminClient adminClient = AdminClient.create(properties);
        try {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }

    static Producer<String, byte[]> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ID_SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
        return new KafkaProducer<>(properties);
    }

    static <K, V> Consumer<K, V> createConsumer(String consumerName, String valueDeserializer) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerName);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ID_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        return new KafkaConsumer<>(properties);
    }

    static KafkaStreams createStreams(StreamsBuilder builder, String streamId) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, streamId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return new KafkaStreams(builder.build(), properties);
    }

    static void resetKafka() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin");

        AdminClient adminClient = AdminClient.create(properties);
        try {
            System.out.println("Removing topics: " + adminClient.listTopics().names().get());
            adminClient.deleteTopics(adminClient.listTopics().names().get());
            List<String> consumerIds = adminClient
                    .listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());
            adminClient.deleteConsumerGroups(consumerIds);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }
}
