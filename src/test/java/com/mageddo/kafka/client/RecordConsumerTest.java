package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;
import templates.ConsumerRecordTemplates;
import templates.ConsumerRecordsTemplates;
import templates.ConsumerTemplates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RecordConsumerTest {

  private static final String TOPIC = "fruit_topic";

  @Test
  void mustRetry() {

    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .callback((c, record, error) -> {
          throw new RuntimeException("Failed consuming");
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(2)
                .delay(Duration.ofMillis(5))
                .build()
        );
    final RecordConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer(), times(3)).commitSync(any(Map.class));

  }

  @Test
  void mustNotRetry() {

    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .callback((c, record, error) -> {
          throw new RuntimeException("Failed consuming");
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(0)
                .delay(Duration.ofMillis(5))
                .build()
        );
    final RecordConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer()).commitSync(any(Map.class));

  }

  private RecordConsumer<String, byte[]> createConsumer(ConsumerConfig<String, byte[]> consumerConfig) {
    return new RecordConsumer<>(spy(ConsumerTemplates.buildWithOnePartition(TOPIC)), consumerConfig);
  }


}
