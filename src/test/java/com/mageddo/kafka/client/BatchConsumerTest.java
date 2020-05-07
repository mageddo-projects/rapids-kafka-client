package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;
import templates.ConsumerRecordTemplates;
import templates.ConsumerRecordsTemplates;
import templates.ConsumerTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class BatchConsumerTest {

  static final String TOPIC = "fruit_topic";

  @Test
  void mustRetryAndRecover() {

    // arrange
    final AtomicBoolean recoverCalled = new AtomicBoolean();
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .batchCallback((c, record, error) -> {
          throw new RuntimeException("Failed consuming");
        })
        .recoverCallback((record, lastFailure) -> {
          assertNotNull(lastFailure);
          assertEquals("Failed consuming", lastFailure.getMessage());
          recoverCalled.set(true);
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(2)
                .delay(Duration.ofMillis(5))
                .build()
        );
    final DefaultConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer(), times(3)).commitSync(any(Map.class));
    assertTrue(recoverCalled.get());
  }

  @Test
  void mustNotRetryButRecover() {

    // arrange
    final AtomicBoolean recoverCalled = new AtomicBoolean();
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .batchCallback((c, record, error) -> {
          throw new RuntimeException("Failed consuming");
        })
        .recoverCallback((record, lastFailure) -> {
          assertNotNull(lastFailure);
          assertEquals("Failed consuming", lastFailure.getMessage());
          recoverCalled.set(true);
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(0)
                .delay(Duration.ofMillis(5))
                .build()
        );
    final DefaultConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer()).commitSync(any(Map.class));
    assertTrue(recoverCalled.get());

  }


  protected DefaultConsumer<String, byte[]> createConsumer(ConsumerConfig<String, byte[]> consumerConfig) {
    return new BatchConsumer<>(spy(ConsumerTemplates.buildWithOnePartition(TOPIC)), consumerConfig);
  }

}
