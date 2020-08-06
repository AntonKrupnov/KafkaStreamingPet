package com.lohika.akrupnov.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Set;

public class Main {

    public static void main(String[] args) {
//        KafkaConnections.resetKafka();
        KafkaConnections.createTopics();

        Set<org.apache.kafka.streams.KafkaStreams> streams = KafkaStreamApps.runStreams();

        Producer<String, byte[]> producer = KafkaProducer.startProducer();

        Set<Consumer> consumers = KafkaConsumers.startConsumers();

        System.out.println("Finish");

        close(streams, producer, consumers);
    }

    private static void close(Set<org.apache.kafka.streams.KafkaStreams> streams,
                              Producer<String, byte[]> producer,
                              Set<Consumer> consumers) {
        producer.close();
        for (Consumer consumer : consumers) {
            consumer.close();
        }
        for (org.apache.kafka.streams.KafkaStreams stream : streams) {
            stream.close();
        }
    }

}
