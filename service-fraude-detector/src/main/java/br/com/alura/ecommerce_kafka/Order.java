package br.com.alura.ecommerce_kafka;

import java.math.BigDecimal;

public class Order {

    private final String userId, orderId;
    private final BigDecimal valor;

    public Order(String userId, String orderId, BigDecimal valor) {
        this.userId = userId;
        this.orderId = orderId;
        this.valor = valor;
    }
}
