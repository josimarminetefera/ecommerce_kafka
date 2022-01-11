package br.com.alura.ecommerce_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaConsumerService<T> implements Closeable {

    //CRIAR O CONSUMIDOR DAS MENSAGENS
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    private KafkaConsumerService(ConsumerFunction parse, String grupoNome, Class<T> type, Map<String, String> propriedades) {
        System.out.println("Iniciando KafkaConsumerService() Construtor 0 ............");
        this.parse = parse;
        //INICIAR O CONSUMIDOR DAS MENSAGENS
        this.consumer = new KafkaConsumer<String, T>(properties(type, grupoNome, propriedades));
    }

    KafkaConsumerService(String grupoNome, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> propriedades) {
        this(parse, grupoNome, type, propriedades);
        System.out.println("Iniciando KafkaConsumerService() Construtor 1 ............");
        //PARA CONSUMIR A MENSAGEM subscribe
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaConsumerService(String grupoNome, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> propriedades) {
        this(parse, grupoNome, type, propriedades);
        System.out.println("Iniciando KafkaConsumerService() Construtor 2 ............");
        //PARA CONSUMIR A MENSAGEM subscribe
        this.consumer.subscribe(topic);
    }

    //FUNÇÃO PARA EXECUTAR O CONSUMIDOR
    void run() {
        System.out.println("Iniciando KafkaConsumerService() ----- run()");
        while (true) {
            //VERIFICAR SE TEM MENSAGEM DENTRO DO CONSUMIDOR ISSO RETORNA VÁRIOS REGISTROS
            //ISSO DEPENDENDO DO MAX_POLL_RECORDS_CONFIG
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println(">>>>>>>>>>>>>>>>>Tem registro ............");
                System.out.println("Encontrei " + records.count() + " registro!");
                for (ConsumerRecord<String, T> record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> type, String grupoNome, Map<String, String> propriedades) {
        System.out.println("Iniciando KafkaConsumerService() ----- properties()");
        Properties properties = new Properties();

        //ONDE VAI BUSCAR AS MENSAGENS
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //AS KEY E VALUE TEM QUE SER DESCRIPTOGRAFADO DE BYTE PARA STRING
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

        //TEM QUE FALAR O ID DO GRUPO QUE NESTE CASO É O NOME DA CLASS
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grupoNome);

        //SETAR O NOME DO MEU CONSUMIDOR
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, grupoNome + "-" + UUID.randomUUID().toString());

        //MAXIMO DE REGISTRO QUE EU QUERO CONSUMIR
        //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        //PARA ADICIONAR OUTRAS PROPRIEDADES
        properties.putAll(propriedades);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
