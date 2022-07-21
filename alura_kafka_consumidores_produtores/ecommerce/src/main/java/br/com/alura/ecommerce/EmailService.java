package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailService {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.out.println("iniciando ----------");

        var emailService = new EmailService();
        var groupName = EmailService.class.getSimpleName();

        try(var service = new KafkaService(groupName,"ECOMMERCE_SEND_EMAIL", emailService::parse)){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {

        System.out.println("----------");
        System.out.println("Processando o envio do e-mail");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

    }

}
