package br.com.alura.ecommerce_kafka;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (KafkaProdutor kafkaProdutor = new KafkaProdutor()) {
            for (int i = 0; i < 10; i++) {
                //CHAVE DO PRODUTOR
                String key = UUID.randomUUID().toString();

                //MENSAGEM QUE EU QUERO MANDAR
                String value = key + ";123456456;JOSIMAR VENTURIM MINETE;15000";
                String email = "Obrigado por finalizar a ordem!";

                //PRODUTOR 1 DE MENSAGEM
                kafkaProdutor.send("ECOMMERCE_NEW_ORDER", key, value);

                //PRODUTOR 2 DE MENSAGEM
                kafkaProdutor.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
