package kafka.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class ProducerAPI {
    public static void main(String[] args) {
        //properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties)) {
            //send record
            IntStream.range(0, 10)
                    .forEach(i -> {
                        //create record
                        ProducerRecord<Integer, String> record = new ProducerRecord<>("my-first-topic", i, "message from java");
                        producer.send(record, ((metadata, e) -> {
                            if (e != null) {
                                log.error("Error occured");
                                e.printStackTrace();
                            } else {
                                log.warn("The partition the message was sent to: {}", metadata.partition());
                            }
                        }));
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
