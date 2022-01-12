package br.com.alura.ecommerce_kafka;

public class Email {
    private final String subject, corpo;

    public Email(String subject, String corpo) {
        this.subject = subject;
        this.corpo = corpo;
    }
}
