package com.mageddo.kafka.client;

import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
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
        .prop(MAX_POLL_RECORDS_CONFIG, 30)
        .build();

    // act
    final ConsumerConfig<String, byte[]> copy = consumerConfig.toBuilder().build();

    // assert
    assertEquals(consumerConfig, copy);
    assertEquals(3, consumerConfig.props().size());
    assertEquals(3, copy.props().size());
    assertNotEquals(System.identityHashCode(consumerConfig), System.identityHashCode(copy));
  }

  @Test
  void consumersConfigMustStayDisabled(){
    // arrange
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfig
        .<String, byte[]>builder()
        .consumers(Integer.MIN_VALUE)
        .consumers(5)
        .build();

    // act
    final int consumers =  consumerConfig
        .toBuilder()
        .consumers(10)
        .build()
        .consumers()
    ;

    // assert
    assertEquals(Integer.MIN_VALUE, consumers);
  }
}
