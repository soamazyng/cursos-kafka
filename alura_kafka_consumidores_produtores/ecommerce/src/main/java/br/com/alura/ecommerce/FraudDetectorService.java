package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.out.println("iniciando ----------");

        var fraudDetectorService = new FraudDetectorService();
        var groupName = FraudDetectorService.class.getSimpleName();

        try(var service = new KafkaService(groupName, "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {

        System.out.println("----------");
        System.out.println("Processando a fraude da msg");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

    }

}
