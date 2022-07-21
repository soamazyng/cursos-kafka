package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {

    private final KafkaProducer<String, String> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<String, String>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<String, String>(topic, key, value);
        this.producer.send(record, getCallback()).get();
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviado " + data.topic() + "::: partition " + data.partition() + "/ offset " + data.offset() + " timestamp " + data.timestamp());
        };
    }

    @Override
    public void close() {
        producer.close();
    }
}
