package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extensions;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ConsumerConfigTemplates;
import templates.ConsumerTemplates;
import templates.PartitionInfoTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ConsumerFactoryTest {

  private final ConsumerFactory<String, byte[]> consumerFactory = spy(new ConsumerFactory<>());

  @Mock
  private ThreadConsumer threadConsumer;

  @Test
  void mustValidateConfiguredRetryPolicyIsNotRecommendedDuePollInterval() {

    // arrange
    final String topic = "fruit_topic";
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .retryPolicy(RetryPolicy
            .builder()
            .delay(Duration.ofMinutes(3))
            .maxTries(2)
            .build()
        );

    doReturn(ConsumerTemplates.build(topic, Collections.emptyList()))
        .when(this.consumerFactory)
        .create(eq(consumerConfig));

    // act
    this.consumerFactory.consume(consumerConfig);

    // assert
    verify(this.consumerFactory).notifyNotRecommendedRetryPolicy(anyInt(), any(), anyLong());

  }

  @Test
  void mustCheckConfiguredRetryPolicyIsRecommendedDuePollInterval() {

    // arrange
    final String topic = "fruit_topic";
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .retryPolicy(RetryPolicy
            .builder()
            .delay(Duration.ofMinutes(2))
            .maxTries(2)
            .build()
        );

    doReturn(ConsumerTemplates.build(topic, Collections.emptyList()))
        .when(this.consumerFactory)
        .create(eq(consumerConfig));

    // act
    this.consumerFactory.consume(consumerConfig);

    // assert
    verify(this.consumerFactory, never()).notifyNotRecommendedRetryPolicy(anyInt(), any(), anyLong());

  }

  @Test
  void mustCreateTwoConsumersWithOnePartitionEachAndTheLastConsumerWithTwo() {
    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .consumers(2)
        .build();
    final String topic = "fruit_topic";

    doReturn(this.threadConsumer).when(this.consumerFactory)
        .getInstance(any(), any());

    doReturn(ConsumerTemplates.build(topic, Arrays.asList(
        PartitionInfoTemplates.build(topic, 1),
        PartitionInfoTemplates.build(topic, 2),
        PartitionInfoTemplates.build(topic, 3)
    )))
        .when(this.consumerFactory)
        .create(eq(consumerConfig));

    // act
    this.consumerFactory.consume(consumerConfig);

    // assert

    final ArgumentCaptor<List<PartitionInfo>> argumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(this.threadConsumer, times(2)).start(argumentCaptor.capture());
    final List<List<PartitionInfo>> partitions = argumentCaptor.getAllValues();
    assertEquals(2, partitions.size());

    final List<PartitionInfo> firstConsumerPartitions = partitions.get(0);
    assertEquals(1, firstConsumerPartitions.size());
    assertEquals(1, firstConsumerPartitions.get(0).partition());

    final List<PartitionInfo> secondConsumerPartitions = partitions.get(1);
    assertEquals(2, secondConsumerPartitions.size());
    assertEquals(2, secondConsumerPartitions.get(0).partition());
    assertEquals(3, secondConsumerPartitions.get(1).partition());
  }
}
