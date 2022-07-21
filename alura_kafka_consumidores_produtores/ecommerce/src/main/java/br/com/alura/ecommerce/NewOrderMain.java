package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var dispatcher = new KafkaDispatcher()) {

            for (var n = 0; n <= 10; n++) {

                var key = UUID.randomUUID().toString();
                var value = "321, 123, 55.99";
                var email = "teste@teste.com.br";

                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }

        }
    }

}
