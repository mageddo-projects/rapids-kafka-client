package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;
import templates.ConsumerTemplates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class ConsumerFactoryTest {

  private final ConsumerFactory<String, byte[]> consumerFactory = spy(new ConsumerFactory<>());

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

  @Disabled
  @Test
  void consume() {
    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();

    // act
    this.consumerFactory.consume(consumerConfig);

    // assert
  }
}
