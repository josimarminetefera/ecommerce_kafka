package br.com.alura.ecommerce_kafka;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Iniciando NewOrderMain() ............");
        //QUANDO ENVIAMOS UMA MENSAGEM SERÁ OU UMA ORDER OU UM EMAIL
        try (KafkaProdutor kafkaProdutorOrdem = new KafkaProdutor<Order>()) {
            try (KafkaProdutor kafkaProdutorEmail = new KafkaProdutor<Email>()) {
                for (int i = 0; i < 10; i++) {
                    System.out.println("-----------------------------------------------------");
                    //CHAVE DO PRODUTOR
                    String userId = UUID.randomUUID().toString();
                    //CODIGO DA ORDEM
                    String orderId = UUID.randomUUID().toString();
                    //VALOR DA ORDEM
                    BigDecimal valor = new BigDecimal(Math.random() * 5000 + 1);
                    System.out.println("Novo produto: " + userId);

                    //NOVA ORDEM DO PRODUTO
                    Order order = new Order(userId, orderId, valor);
                    String email = "Obrigado por finalizar a ordem!";

                    //PRODUTOR 1 DE MENSAGEM
                    kafkaProdutorOrdem.send("ECOMMERCE_NEW_ORDER", userId, order);

                    //PRODUTOR 2 DE MENSAGEM
                    kafkaProdutorEmail.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
