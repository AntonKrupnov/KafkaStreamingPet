package com.lohika.akrupnov.kafka;

import auto.ria.core.CarAvro;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;

public class KafkaProducer {
    static Producer<String, byte[]> startProducer() {
        Producer<String, byte[]> producer = KafkaConnections.createProducer();
        File file = new File("auto.ria.avro");
        DatumReader<CarAvro> reader = new SpecificDatumReader<>(CarAvro.class);
        DataFileReader<CarAvro> fileReader = null;
        try {
            fileReader = new DataFileReader<>(file, reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (fileReader != null) {
            for (int i = 0; fileReader.hasNext() && i < 20; i++) {
                System.out.println("Sending...");
                CarAvro carAvro = fileReader.next();
                byte[] datum = SerializationUtils.serialize(carAvro);
                producer.send(new ProducerRecord<>(Topics.MAIN_TOPIC.name(),
                        carAvro.getId().toString(), datum));
                System.out.println("Sent: " + carAvro);
            }
        }
        return producer;
    }
}
