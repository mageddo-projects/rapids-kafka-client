package com.mageddo.kafka.client;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;
import templates.ConsumerTemplates;
import templates.DefaultConsumerTemplates;
import templates.PartitionInfoTemplates;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class DefaultConsumerTest {

  private DefaultConsumer<String, byte[]> consumer = spy(DefaultConsumerTemplates.build());

  @Test
  void cantStartTwitch() {

    // arrange

    // act
    this.consumer.start(Collections.EMPTY_LIST);
    this.consumer.start(Collections.EMPTY_LIST);

    // assert
    verify(this.consumer).consumer();

  }

  @Test
  @SneakyThrows
  void mustStopConsumeAfterInterruptTheThread() {

    // arrange
    final String topic = "fruit_topic";
    final List<PartitionInfo> partitionInfos = Collections.singletonList(PartitionInfoTemplates.build(topic));
    final MockConsumer<String, byte[]> consumer = ConsumerTemplates.build(topic, partitionInfos);
    consumer.scheduleNopPollTask();

    // act
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      this.consumer.poll(consumer, ConsumerConfigTemplates.build());
    });

    // assert
    executor.shutdownNow();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    assertTrue(executor.isShutdown());
    assertTrue(executor.isTerminated());
  }
}
