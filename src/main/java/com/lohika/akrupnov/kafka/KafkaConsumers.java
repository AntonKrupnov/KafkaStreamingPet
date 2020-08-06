package com.lohika.akrupnov.kafka;

import auto.ria.core.CarAvro;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KafkaConsumers {
    private static final String STRING_DESERIALIZER = StringDeserializer.class.getName();
    private static final String BYTES_DESERIALIZER = ByteArrayDeserializer.class.getName();

    static Set<Consumer> startConsumers() {
        Set<Consumer> consumers = new HashSet<>();
        consumers.add(startMainTopicConsumer());
        consumers.add(startYearCostConsumer());
        consumers.add(startTotalCostTopicConsumer());
        consumers.add(startMinMaxTopicConsumer());
        return consumers;
    }

    private static Consumer<String, String> startYearCostConsumer() {
        System.out.println("Creating Year-Cost consumer");
        Consumer<String, String> yearCostConsumer = KafkaConnections.createConsumer("yearCostConsumer", STRING_DESERIALIZER);
        yearCostConsumer.subscribe(Collections.singletonList(Topics.YEAR_COST.name()));

        System.out.println("(YEAR_COST) Fetching records");
        ConsumerRecords<String, String> records = yearCostConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records.records(Topics.YEAR_COST.name())) {
                System.out.println("Year->Cost received: " + record.key() + " => " + record.value());
            }
        }
        yearCostConsumer.commitSync();
        return yearCostConsumer;
    }

    private static Consumer<String, byte[]> startMainTopicConsumer() {
        System.out.println("Creating Main consumer");
        Consumer<String, byte[]> mainConsumer = KafkaConnections.createConsumer("mainConsumer", BYTES_DESERIALIZER);
        mainConsumer.subscribe(Collections.singletonList(Topics.MAIN_TOPIC.name()));
        System.out.println("(MAIN) Fetching records");
        ConsumerRecords<String, byte[]> records = mainConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        if (!records.isEmpty()) {
            for (ConsumerRecord<String, byte[]> record : records.records(Topics.MAIN_TOPIC.name())) {
                CarAvro object = SerializationUtils.deserialize(record.value());
                System.out.println("Received: " + record.key() + " => " + object);
            }
        }
        mainConsumer.commitSync();
        return mainConsumer;
    }

    private static Consumer<String, String> startTotalCostTopicConsumer() {
        System.out.println("Creating Total Cost consumer");
        Consumer<String, String> totalCostConsumer = KafkaConnections.createConsumer("brandT§otalCostConsumer", STRING_DESERIALIZER);
        totalCostConsumer.subscribe(Collections.singletonList(Topics.TOTAL_COST.name()));
        System.out.println("(TOTAL_COST) Fetching records");
        ConsumerRecords<String, String> records = totalCostConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records.records(Topics.TOTAL_COST.name())) {
                System.out.println("Received: " + record.key() + " => " + record.value());
            }
        }
        totalCostConsumer.commitSync();
        return totalCostConsumer;
    }

    private static Consumer<String, String> startMinMaxTopicConsumer() {
        System.out.println("Creating min/max consumer");
        Consumer<String, String> totalCostConsumer = KafkaConnections.createConsumer("minMaxConsumer", STRING_DESERIALIZER);
        totalCostConsumer.subscribe(Collections.singletonList(Topics.MIN_MAX_COST.name()));
        System.out.println("(MIN_MAX) Fetching records");
        ConsumerRecords<String, String> records = totalCostConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records.records(Topics.MIN_MAX_COST.name())) {
                System.out.println("Received: " + record.key() + " => " + record.value());
            }
        }
        totalCostConsumer.commitSync();
        return totalCostConsumer;
    }
}
