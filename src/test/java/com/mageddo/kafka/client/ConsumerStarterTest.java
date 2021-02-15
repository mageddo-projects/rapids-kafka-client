package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ConsumerConfigTemplates;

import static com.mageddo.kafka.client.ConsumerConfigDefault.CONSUMERS_NOT_SET;
import static com.mageddo.kafka.client.DefaultConsumingConfig.DEFAULT_RETRY_STRATEGY;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ConsumerStarterTest {

  @Captor
  ArgumentCaptor<ConsumerConfig> argumentCaptor;

  @Test
  void mustStartConsumerUsingDefaultConfigs() {

    // arrange
    final var templateConsumerConfig = ConsumerConfig.builder().build();
    final var consumerStarter = spy(new ConsumerStarter(templateConsumerConfig));
    final var groupId = "my_group_id";
    final var consumerConfig = ConsumerConfig
        .builder()
        .prop(GROUP_ID_CONFIG, groupId)
        .build();
    doReturn(mock(ConsumerController.class))
        .when(consumerStarter)
        .start(any(ConsumerConfig.class));

    // act
    consumerStarter.startFromConfig(List.of(consumerConfig));

    // assert
    verify(consumerStarter).start(this.argumentCaptor.capture());
    final var producedConfig = this.argumentCaptor.getValue();
    assertEquals(groupId, producedConfig.groupId());
    assertNull(producedConfig.retryPolicy());
    assertNull(producedConfig.recoverCallback());
    assertNull(producedConfig.batchCallback());
    assertNull(producedConfig.callback());
    assertEquals(CONSUMERS_NOT_SET, producedConfig.consumers());
    assertNull(producedConfig.pollInterval());
    assertNull(producedConfig.pollTimeout());
    assertEquals(1, producedConfig.props().size());

  }

  @Test
  void mustStartConsumerUsingTemplateConfigs() {

    // arrange
    final var groupId = "my_group_id";
    final var templatePollInterval = Duration.ofSeconds(3);
    final var templatePollTimeout = Duration.ofSeconds(1);
    final var templateConsumers = 3;

    final var templateConfig = ConsumerConfigTemplates.builder()
        .prop(GROUP_ID_CONFIG, groupId)
        .consumers(templateConsumers)
        .pollInterval(templatePollInterval)
        .pollTimeout(templatePollTimeout)
        .retryPolicy(DEFAULT_RETRY_STRATEGY)
        .build();
    final var consumerStarter = spy(new ConsumerStarter<>(templateConfig));

    final var consumerConfig = ConsumerConfig
        .builder()
        .build();
    doReturn(mock(ConsumerController.class))
        .when(consumerStarter)
        .start(any(ConsumerConfig.class));

    // act
    consumerStarter.startFromConfig(List.of(consumerConfig));

    // assert
    verify(consumerStarter).start(this.argumentCaptor.capture());
    final var actualConfig = this.argumentCaptor.getValue();
    assertEquals(groupId, actualConfig.groupId());
    assertEquals(DEFAULT_RETRY_STRATEGY, actualConfig.retryPolicy());
    assertEquals(templateConfig.recoverCallback(), actualConfig.recoverCallback());
    assertEquals(templateConfig.batchCallback(), actualConfig.batchCallback());
    assertEquals(templateConfig.callback(), actualConfig.callback());
    assertEquals(templateConsumers, actualConfig.consumers());
    assertEquals(templatePollInterval, actualConfig.pollInterval());
    assertEquals(templatePollTimeout, actualConfig.pollTimeout());
    assertEquals(1, actualConfig.props().size());
  }

  @Test
  void consumerSpecificConfigMustOverrideTemplateConfigs() {

    // arrange
    final var templateConfig = ConsumerConfigTemplates.builder()
        .prop(GROUP_ID_CONFIG, "my_group_id")
        .build();
    final var consumerStarter = spy(new ConsumerStarter<>(templateConfig));

    final var groupId = "customGroupId";
    final var pollInterval = Duration.ofSeconds(33);
    final var pollTimeout = Duration.ofSeconds(135);
    final var consumers = 333;
    final var consumerConfig = ConsumerConfig
        .builder()
        .prop(GROUP_ID_CONFIG, groupId)
        .retryPolicy(DEFAULT_RETRY_STRATEGY)
        .consumers(consumers)
        .pollInterval(pollInterval)
        .pollTimeout(pollTimeout)
        .build();

    doReturn(mock(ConsumerController.class))
        .when(consumerStarter)
        .start(any(ConsumerConfig.class));

    // act
    consumerStarter.startFromConfig(List.of(consumerConfig));

    // assert
    verify(consumerStarter).start(this.argumentCaptor.capture());
    final var actualConfig = this.argumentCaptor.getValue();
    assertEquals(groupId, actualConfig.groupId());
    assertEquals(DEFAULT_RETRY_STRATEGY, actualConfig.retryPolicy());
    assertEquals(templateConfig.recoverCallback(), actualConfig.recoverCallback());
    assertEquals(templateConfig.batchCallback(), actualConfig.batchCallback());
    assertEquals(templateConfig.callback(), actualConfig.callback());
    assertEquals(consumers, actualConfig.consumers());
    assertEquals(pollInterval, actualConfig.pollInterval());
    assertEquals(pollTimeout, actualConfig.pollTimeout());
    assertEquals(1, actualConfig.props().size());

  }

  @Test
  void mustNotOverrideConsumerThreadsWhenDefaultValueIsDisabled() {

    // arrange
    final var consumingDisabled = Integer.MIN_VALUE;
    final var templateConfig = ConsumerConfigTemplates.builder()
        .consumers(consumingDisabled)
        .build();
    final var consumerStarter = spy(new ConsumerStarter<>(templateConfig));
    final var consumers = 333;
    final var consumerConfig = ConsumerConfig
        .builder()
        .consumers(consumers)
        .build();

    doReturn(mock(ConsumerController.class))
        .when(consumerStarter)
        .start(any(ConsumerConfig.class));

    // act
    consumerStarter.startFromConfig(List.of(consumerConfig));

    // assert
    verify(consumerStarter).start(this.argumentCaptor.capture());
    final var actualConfig = this.argumentCaptor.getValue();
    assertEquals(consumingDisabled, actualConfig.consumers());

  }
}
