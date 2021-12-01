package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //ENVIAR UMA MENSAGEM NO KAFKA
        KafkaProducer producer = new KafkaProducer<String, String>(properties());

        for (int i = 0; i < 10; i++) {
            //CHAVE DO PRODUTOR
            String key = UUID.randomUUID().toString();

            //CALBACK PARA AGUARDAR E PEGAR A RESPOSTA ASSIM QUE CHEGAR
            Callback callback = (data, ex) -> {
                //ESCUTAR O RETORNO DA MENSAGEM
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("sucesso >> " + data.topic() + ":::partition = " + data.partition() + " /offset = " + data.offset() + " /timestamp = " + data.timestamp());
            };

            //PRODUTOR 1 DE MENSAGEM
            //MENSAGEM QUE EU QUERO MANDAR
            String value = key + ",67523,10000";

            //CRIAR REGISTRO DO PRODUCER
            ProducerRecord record = new ProducerRecord("ECOMMERCE_NEW_ORDER", key, value);

            //PARA ENVIAR UMA MESAGEM send
            //ENVIAR MENSAGEM E COM get()
            producer.send(record, callback).get();

            //PRODUTOR 2 DE MENSAGEM
            String email = "Obrigado por sua New Order!";
            ProducerRecord<String, String> emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            //ENVIAR UM E-MAIL PARA VERIFICAR
            producer.send(emailRecord, callback).get();
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        //ONDE VAI CONECTAR O KAFKA
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //TEM QUE TRANSFORMAR A KEY E O VALOR DO KafkaProducer DE STRING PARA BYTE
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
