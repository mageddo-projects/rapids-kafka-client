package templates;

import java.time.Duration;
import java.util.Collections;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.RetryPolicy;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerConfigTemplates {

  public static <K, V> ConsumerConfig<K, V> builder() {
    return new ConsumerConfig<K, V>()
        .setBatchCallback((consumer, records, error) -> System.out.println("batch callback"))
        .setCallback((consumer, record, error) -> System.out.println("callback"))
        .setConsumers(3)
        .setInterval(Duration.ofMillis(1000 / 3))
        .setRetryPolicy(RetryPolicy
            .builder()
            .maxTries(2)
            .delay(Duration.ofMinutes(1))
            .build()
        )
        .setRecoverCallback((record, lastFailure) -> System.out.println("recover callback"))
        .setTopics(Collections.singleton("data_topic"))
        .setTimeout(Duration.ofMillis(100))
        ;
  }

}
