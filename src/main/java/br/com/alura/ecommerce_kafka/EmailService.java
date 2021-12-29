package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        System.out.println("Iniciando EmailService() ............");
        EmailService emailService = new EmailService();
        try (KafkaConsumerService kafkaConsumerService = new KafkaConsumerService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse)) {
            kafkaConsumerService.run();
        }
    }

    //CADA RECORD (REGISTRO) VAI CHAMAR ESTA FUNÇÃO
    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------------------------------");
        System.out.println("Processando e-mail: ");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("E-mail Processado");
    }
}
