package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import templates.ConsumerConfigTemplates;
import templates.DefaultConsumerTemplates;
import templates.DefaultConsumerTemplates.MockedDefaultConsumer;
import templates.PartitionInfoTemplates;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class DefaultConsumerTest {


  @Test
  void cantStartTwice() {

    // arrange
    final DefaultConsumer<String, byte[]> consumer = spy(DefaultConsumerTemplates.build());

    // act
    consumer.start();
    consumer.start();

    // assert
    verify(consumer).consumer();

  }

  @Test
  @SneakyThrows
  void mustStopConsumeAfterInterruptTheThread() {

    // arrange
    final String topic = "fruit_topic";
    final MockedDefaultConsumer<String, byte[]> consumer = spy(DefaultConsumerTemplates.build(
        topic,
        PartitionInfoTemplates.build(topic)
    ));

    // act
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      consumer.poll(consumer.consumer(), ConsumerConfigTemplates.build());
    });

    // assert
    executor.shutdownNow();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    assertTrue(executor.isShutdown());
    assertTrue(executor.isTerminated());
  }

  @Test
  @SneakyThrows
  void mustStopConsumeAfterInterruptTheThread2() {

    // arrange
    final String topic = "fruit_topic";
    final MockedDefaultConsumer<String, byte[]> consumer = spy(DefaultConsumerTemplates.build(
        topic,
        PartitionInfoTemplates.build(topic)
    ));

    // act
    consumer.start();
    consumer.close();

    // assert
    assertTrue(consumer.getExecutor().isShutdown());
  }

  @Test
  void mustNotSleepWhenPollIntervalIsSetToZero() throws InterruptedException {
    // arrange
//    final ConsumerRecord<String, String> consumerRecord = ConsumerRecordTemplates.build("hello world!!");
    final String topic = "fruit_topic";
    final MockedDefaultConsumer<String, byte[]> consumer = spy(DefaultConsumerTemplates.build(
        topic,
        PartitionInfoTemplates.build(topic)
    ));
    final Consumers<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .pollInterval(Duration.ZERO)
        .build();
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    // act
    executor.submit(() -> {
      consumer.poll(consumer.consumer(), consumerConfig);
    });
    executor.awaitTermination(100, TimeUnit.MILLISECONDS);
    executor.shutdownNow();

    // assert
    verify(consumer, never()).sleep(any());
  }
}
