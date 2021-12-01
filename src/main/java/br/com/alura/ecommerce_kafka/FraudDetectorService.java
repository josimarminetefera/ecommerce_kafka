package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) {
        System.out.println("Iniciando FraudDetectorService() ............");
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        KafkaService kafkaService = new KafkaService(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_ORDER", fraudDetectorService::parse);
        kafkaService.run();
    }

    //CADA RECORD (REGISTRO) VAI CHAMAR ESTA FUNÇÃO
    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------------------------------");
        System.out.println("Processando Fraude Detector: ");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Fraude Detector Processada");
    }
}
