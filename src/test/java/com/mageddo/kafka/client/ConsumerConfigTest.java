package com.mageddo.kafka.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ConsumerConfigTest {

  @Test
  void validateDefaultConfigs() {

    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfig
        .<String, byte[]>builder()
        .topics("topic")
        .build();

    // act
    final String props = consumerConfig
        .props()
        .toString();

    //assert
    assertEquals("{enable.auto.commit=false, bootstrap.servers=localhost:9092}", props);

  }

  @Test
  void mustCopy(){
    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfig
        .<String, byte[]>builder()
        .topics("topic")
        .build();

    // act
    final ConsumerConfig<String, byte[]> copy = consumerConfig.copy();

    // assert
    assertEquals(consumerConfig, copy);
    assertNotEquals(System.identityHashCode(consumerConfig), System.identityHashCode(copy));
  }
}
