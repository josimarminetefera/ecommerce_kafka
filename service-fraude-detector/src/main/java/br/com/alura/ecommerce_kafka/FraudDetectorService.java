package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {
    public static void main(String[] args) {
        System.out.println("Iniciando FraudDetectorService() ............");
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        try (KafkaConsumerService kafkaConsumerService = new KafkaConsumerService<Order>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                new HashMap<String, String>()
        )) {
            kafkaConsumerService.run();
        }
    }

    //CADA RECORD (REGISTRO) VAI CHAMAR ESTA FUNÇÃO
    private void parse(ConsumerRecord<String, Order> record) {
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
