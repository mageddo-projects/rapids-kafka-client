package com.mageddo.kafka.client;

import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;

class ConsumersTest {

  @Test
  void consumerMustStayDisabled() {

    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .consumers(Integer.MIN_VALUE)
        .build();

    // act
    Consumers.consume(consumerConfig
        .toBuilder()
        .build()
    );

    // assert

  }
}
