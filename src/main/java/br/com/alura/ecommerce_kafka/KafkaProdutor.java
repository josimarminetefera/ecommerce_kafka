package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaProdutor implements Closeable {
    //ENVIAR UMA MENSAGEM NO KAFKA
    private final KafkaProducer<String, String> producer;

    KafkaProdutor() {
        //ESTANCIANDO O PRODUTOR
        this.producer = new KafkaProducer<String, String>(properties());
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

    void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        //CALBACK PARA AGUARDAR E PEGAR A RESPOSTA ASSIM QUE CHEGAR
        Callback callback = (data, ex) -> {
            //ESCUTAR O RETORNO DA MENSAGEM
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso >> " + data.topic() + ":::partition = " + data.partition() + " /offset = " + data.offset() + " /timestamp = " + data.timestamp());
        };

        //CRIAR REGISTRO DO PRODUCER
        ProducerRecord record = new ProducerRecord(topic, key, value);

        //PARA ENVIAR UMA MESAGEM send
        //ENVIAR MENSAGEM E COM get()
        this.producer.send(record, callback).get();
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
