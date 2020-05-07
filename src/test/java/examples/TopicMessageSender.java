package examples;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.experimental.UtilityClass;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

@UtilityClass
public class TopicMessageSender {

  public static void keepSendingMessages() {
    final Map<String, Object> props = new LinkedHashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

    Executors
        .newSingleThreadExecutor()
        .submit(() -> {
          while (true) {
            producer.send(new ProducerRecord<>(
                "stock_client_v2",
                String.format("stock=PAGS, price=%.2f", Math.random() * 100)
                    .getBytes()
            ));
            TimeUnit.MILLISECONDS.sleep(3000);
          }
        });
  }
}
