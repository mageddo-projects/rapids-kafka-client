package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;

class ConsumerFactoryTest {

  private final ConsumerFactory<String, byte[]> consumerFactory = new ConsumerFactory<>();

  @Disabled
  @Test
  void consume() {
    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfigTemplates.builder();

    // act
    this.consumerFactory.consume(consumerConfig);

    // assert
  }
}
