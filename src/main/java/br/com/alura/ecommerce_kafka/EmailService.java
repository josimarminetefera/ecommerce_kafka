package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) {
        //CRIAR O CONSUMIDOR DAS MENSAGENS
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties());
        //PARA CONSUMIR A MENSAGEM subscribe
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));
        System.out.println("Iniciando...........");
        while (true) {
            //VERIFICAR SE TEM MENSAGEM DENTRO DO CONSUMIDOR ISSO RETORNA VÁRIOS REGISTROS
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Verificando.................");
                System.out.println("Encontrei " + records.count() + " registros!");

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("-----------------------------------------------------");
                    System.out.println("Procesando send email: ");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Send email processado");
                }

            }

        }
    }

    private static Properties properties() {
        Properties properties = new Properties();

        //ONDE VAI BUSCAR AS MENSAGENS
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //AS KEY E VALUE TEM QUE SER DESCRIPTOGRAFADO DE BYTE PARA STRING
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //TEM QUE FALAR O ID DO GRUPO QUE NESTE CASO É O NOME DA CLASS
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }
}
