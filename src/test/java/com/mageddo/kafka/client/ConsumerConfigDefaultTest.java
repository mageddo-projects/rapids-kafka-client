package com.mageddo.kafka.client;

import org.junit.jupiter.api.Test;

import templates.ConsumerConfigTemplates;

import static com.mageddo.kafka.client.ConsumerConfigDefault.CONSUMERS_NOT_SET;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConsumerConfigDefaultTest {

  @Test
  void mustNotSetDefaultFields(){
    // arrange
    final ConsumerConfigDefault<String, byte[]> config = ConsumerConfigDefault
        .<String, byte[]>builder()
        .topics("topic")
        .build();

    // act
    // assert
    assertNull(config.pollTimeout());
    assertNull(config.pollInterval());
    assertNull(config.retryPolicy());
    assertNull(config.consumerSupplier());
    assertEquals(CONSUMERS_NOT_SET, config.consumers());

  }

  /**
   * Default props must be set the consumer creator at the last moment
   */
  @Test
  void mustNotSetDefaultProps() {

    // arrange
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigDefault
        .<String, byte[]>builder()
        .topics("topic")
        .build();

    // act
    final String props = consumerConfig
        .props()
        .toString();

    //assert
    assertEquals("{}", props);

  }

  @Test
  void mustCopy() {
    // arrange
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigDefault
        .<String, byte[]>builder()
        .topics("topic")
        .prop(MAX_POLL_RECORDS_CONFIG, 30)
        .build();

    // act
    final ConsumerConfigDefault<String, byte[]> copy = consumerConfig.toBuilder()
        .build();

    // assert
    assertEquals(String.valueOf(consumerConfig), String.valueOf(copy));
    assertEquals(1, consumerConfig.props()
        .size());
    assertEquals(1, copy.props()
        .size());
    assertNotEquals(System.identityHashCode(consumerConfig), System.identityHashCode(copy));
  }

  @Test
  void consumersConfigMustStayDisabled() {
    // arrange
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigDefault
        .<String, byte[]>builder()
        .consumers(Integer.MIN_VALUE)
        .consumers(5)
        .build();

    // act
    final int consumers = consumerConfig
        .toBuilder()
        .consumers(10)
        .build()
        .consumers();

    // assert
    assertEquals(Integer.MIN_VALUE, consumers);
  }

  @Test
  void consumerMustStayDisabled() {

    // arrange
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .consumers(Integer.MIN_VALUE)
        .build();

    // act
    consumerConfig
        .toBuilder()
        .build()
        .consume();

    // assert

  }
}
