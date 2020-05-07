package templates;

import java.time.Duration;
import java.util.Collections;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.RetryPolicy;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerConfigTemplates {

  public static <K, V> ConsumerConfig<K, V> build() {
    return new ConsumerConfig<K, V>()
        .batchCallback((consumer, records, error) -> System.out.println("batch callback"))
        .callback((consumer, record, error) -> System.out.println("callback"))
        .consumers(3)
        .interval(Duration.ofMillis(1000 / 3))
        .retryPolicy(RetryPolicy
            .builder()
            .maxTries(2)
            .delay(Duration.ofMinutes(1))
            .build()
        )
        .recoverCallback((record, lastFailure) -> System.out.println("recover callback"))
        .topics(Collections.singleton("fruit_topic"))
        .timeout(Duration.ofMillis(100))
        ;
  }

}
