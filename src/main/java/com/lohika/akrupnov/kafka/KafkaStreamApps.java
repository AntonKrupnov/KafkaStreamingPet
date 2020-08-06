package com.lohika.akrupnov.kafka;

import auto.ria.core.CarAvro;
import com.lohika.akrupnov.kafka.dataobjects.CountTotalCost;
import com.lohika.akrupnov.kafka.dataobjects.MinMaxTotalCost;
import org.apache.commons.compress.utils.Sets;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

import java.io.Serializable;
import java.util.Set;

class KafkaStreamApps {
    static Set<org.apache.kafka.streams.KafkaStreams> runStreams() {

        return Sets.newHashSet(yearCost(), totalCostConsumer(), minMaxCostConsumer());
    }

    private static org.apache.kafka.streams.KafkaStreams yearCost() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(Topics.MAIN_TOPIC.name())
                .map((KeyValueMapper<Object, Object, KeyValue<String, String>>) (key, value) -> {
                    CarAvro carAvro = SerializationUtils.deserialize((byte[]) value);
                    return KeyValue.pair(Integer.toString(carAvro.getYear()), Integer.toString(carAvro.getPriceUsd()));
                })
                .to(Topics.YEAR_COST.name(), Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams yearCost = KafkaConnections.createStreams(builder, "year-cost-stream");
        yearCost.start();
        return yearCost;
    }

    private static org.apache.kafka.streams.KafkaStreams totalCostConsumer() {
        StreamsBuilder builder;
        builder = new StreamsBuilder();
        builder
                .stream(Topics.MAIN_TOPIC.name())
                .mapValues(value -> (CarAvro) SerializationUtils.deserialize((byte[]) value))
                .mapValues(value ->
                        new CountTotalCost(
                                value.getBrand() + ":" + value.getYear(),
                                1,
                                value.getPriceUsd()))
                .groupBy((key, value) -> value.getBrandYear(), Grouped.with(Serdes.String(), javaSerialization()))
                .reduce((value1, value2) ->
                        new CountTotalCost(
                                value1.getBrandYear(),
                                value1.getCount() + value2.getCount(),
                                value1.getTotalCost() + value2.getTotalCost()))
                .mapValues(value -> value.getCount() + ":" + value.getTotalCost())
                .toStream()
                .to(Topics.TOTAL_COST.name(), Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams totalCost = KafkaConnections.createStreams(builder, "brand-total-cost-stream");
        totalCost.start();
        return totalCost;
    }

    private static org.apache.kafka.streams.KafkaStreams minMaxCostConsumer() {
        StreamsBuilder builder;
        builder = new StreamsBuilder();
        builder
                .stream(Topics.MAIN_TOPIC.name())
                .mapValues(value -> (CarAvro) SerializationUtils.deserialize((byte[]) value))
                .mapValues(value ->
                        new MinMaxTotalCost(
                                value.getBrand() + ":" + value.getModel() + ":" + value.getYear(),
                                1,
                                value.getPriceUsd(),
                                value.getPriceUsd(),
                                value.getPriceUsd()))
                .groupBy((key, value) -> value.getBrandModelYear(), Grouped.with(Serdes.String(), javaSerialization()))
                .reduce((value1, value2) ->
                        new MinMaxTotalCost(
                                value1.getBrandModelYear(),
                                value1.getCount() + value2.getCount(),
                                value1.getTotalCost() + value2.getTotalCost(),
                                (value1.getMin() < value2.getMin() ? value1.getMin() : value2.getMin()),
                                (value1.getMax() > value2.getMax() ? value1.getMax() : value2.getMax())))
                .mapValues(value -> value.getCount() + ":" + value.getTotalCost() + ":" + value.getMin() + ":" + value.getMax())
                .toStream()
                .to(Topics.MIN_MAX_COST.name(), Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams minMaxCost = KafkaConnections.createStreams(builder, "min-max-total-stream");
        minMaxCost.start();
        return minMaxCost;
    }

    private static <T extends Serializable> Serde<T> javaSerialization() {
        return Serdes.serdeFrom(
                (topic, data) -> SerializationUtils.serialize(data),
                (topic, data) -> SerializationUtils.deserialize(data));
    }
}
