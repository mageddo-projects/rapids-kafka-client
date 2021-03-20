package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import templates.ConsumerConfigTemplates;
import templates.ConsumerRecordTemplates;
import templates.ConsumerRecordsTemplates;
import templates.ConsumerTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Slf4j
class BatchConsumerTest {

  static final String TOPIC = "fruit_topic";

  @Test
  void mustRetryAndRecover() {

    // arrange
    final AtomicBoolean recoverCalled = new AtomicBoolean();
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates.<String, byte[]>build()
        .toBuilder()
        .batchCallback((ctx, record) -> {
          throw new RuntimeException("Failed consuming");
        })
        .recoverCallback((ctx) -> {
          assertNotNull(ctx.lastFailure());
          assertEquals("Failed consuming", ctx.lastFailure().getMessage());
          recoverCalled.set(true);
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(2)
                .delay(Duration.ofMillis(5))
                .build()
        )
        .build();
    final DefaultConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer(), times(4)).commitSync(any(Map.class));
    assertTrue(recoverCalled.get());
  }

  @Test
  void mustNotRetryButRecover() {

    // arrange
    final AtomicBoolean recoverCalled = new AtomicBoolean();
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .batchCallback((ctx, record) -> {
          throw new RuntimeException("Failed consuming");
        })
        .recoverCallback((ctx) -> {
          assertNotNull(ctx.lastFailure());
          assertEquals("Failed consuming", ctx.lastFailure().getMessage());
          recoverCalled.set(true);
        })
        .retryPolicy(
            RetryPolicy
                .builder()
                .maxTries(0)
                .delay(Duration.ofMillis(5))
                .build()
        )
        .build();
    final DefaultConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer(), times(2)).commitSync(any(Map.class));
    assertTrue(recoverCalled.get());

  }

  @Test
  void mustCommitAfterSuccessfullyConsume() {
    // arrange
    final AtomicInteger timesRetried = new AtomicInteger();
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates.<String, byte[]>build()
        .toBuilder()
        .recoverCallback((ctx) -> {
          fail("Can't recover");
        })
        .batchCallback((ctx, records) -> {
          log.info("consumed: {}", records.count());
          timesRetried.incrementAndGet();
        })
        .build();
    final DefaultConsumer<String, byte[]> consumer = createConsumer(consumerConfig);
    final ConsumerRecords<String, byte[]> records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer.consumer()).commitSync(any(Map.class));
    assertEquals(1, timesRetried.get());
  }

  @Test
  void mustUseDefaultRetryStrategyWhenItComesNull() {
    // arrange
    final var consumerConfig = ConsumerConfigTemplates.<String, byte[]>build()
        .toBuilder()
        .retryPolicy(null)
        .build();
    final var consumer = this.createConsumer(consumerConfig);
    final var records = ConsumerRecordsTemplates.build(
        TOPIC,
        ConsumerRecordTemplates.build("Hello World".getBytes())
    );

    // act
    consumer.consume(records);

    // assert
    verify(consumer).getRetryPolicy();
  }


  protected BatchConsumer<String, byte[]> createConsumer(ConsumerConfigDefault<String, byte[]> consumerConfig) {
    return spy(new BatchConsumer<>(spy(ConsumerTemplates.buildWithOnePartition(TOPIC)), consumerConfig, 0));
  }

}
