package com.mageddo.kafka.client;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerConfigTest {

  @Test
  void validateDefaultConfigs() {

    // arrange
    final ConsumerConfig<Object, Object> consumerConfig = ConsumerConfig
        .builder()
        .topics(Collections.singletonList("topic"))
        .build();

    // act
    final String props = consumerConfig
        .props()
        .toString();

    //assert
    assertEquals("{enable.auto.commit=false}", props);

  }
}
