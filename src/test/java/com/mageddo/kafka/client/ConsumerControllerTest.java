package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.SneakyThrows;
import templates.ConsumerConfigTemplates;
import templates.ConsumerTemplates;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ConsumerControllerTest {

  private final ConsumerController<String, byte[]> consumerController = spy(new ConsumerController<>());

  @Mock
  private ThreadConsumer threadConsumer;

  @Test
  void mustValidateConfiguredRetryPolicyIsNotRecommendedDuePollInterval() {

    // arrange
    final String topic = "fruit_topic";
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates
        .<String, byte[]>builder()
        .retryPolicy(RetryPolicy
            .builder()
            .delay(Duration.ofMinutes(3))
            .maxTries(2)
            .build()
        )
        .build();

    doReturn(ConsumerTemplates.buildMock(topic, Collections.emptyList()))
        .when(this.consumerController)
        .create(eq(consumerConfig));

    // act
    this.consumerController.consume(consumerConfig);

    // assert
    verify(this.consumerController).notifyNotRecommendedRetryPolicy(anyInt(), any(), anyLong());

  }

  @Test
  void mustCheckConfiguredRetryPolicyIsRecommendedDuePollInterval() {

    // arrange
    final String topic = "fruit_topic";
    final ConsumerConfigDefault<String, byte[]> consumerConfig = ConsumerConfigTemplates.build();
    consumerConfig
        .toBuilder()
        .retryPolicy(RetryPolicy
            .builder()
            .delay(Duration.ofMinutes(2))
            .maxTries(2)
            .build()
        )
        .build();

    doReturn(ConsumerTemplates.buildMock(topic, Collections.emptyList()))
        .when(this.consumerController)
        .create(eq(consumerConfig));

    // act
    this.consumerController.consume(consumerConfig);

    // assert
    verify(this.consumerController, never()).notifyNotRecommendedRetryPolicy(anyInt(), any(), anyLong());

  }

  @Test
  void mustUseDefaultRetryStrategyWhenNotSet() {

    // arrange
    final var topic = "fruit_topic";
    final var consumerConfig = ConsumerConfigTemplates.<String, byte[]>noConsumers()
        .toBuilder()
        .retryPolicy(null)
        .build();

    doReturn(ConsumerTemplates.buildMock(topic, Collections.emptyList()))
        .when(this.consumerController)
        .create(eq(consumerConfig));

    // act
    // assert
    this.consumerController.consume(consumerConfig);

  }

  @Test
  void mustUseDefaultConsumerSupplierWhenItsNotSet() {
    // arrange
    final var topic = "fruit_topic";
    final var consumerConfig = ConsumerConfigTemplates.<String, byte[]>build()
        .toBuilder()
        .consumers(1)
        .consumerSupplier(null)
        .build();

    // act
    // assert
    assertThrows(ConfigException.class, () -> this.consumerController.create(consumerConfig));

  }

  @SneakyThrows
  @Test
  void mustUseDefaultPollIntervalWhenItsNotSet() {
    // arrange
    final var consumerConfig = ConsumerConfig
        .<String, byte[]>builder()
        .consumerSupplier(config -> new MockConsumer<>(OffsetResetStrategy.EARLIEST))
        .consumers(1)
        .callback((callbackContext, record) -> System.out.println("nop"))
        .build();

//    Mockito
//        .doAnswer(invocation -> {
//          final var actual = (DefaultConsumer) invocation.callRealMethod();
//          return new ThreadConsumer<>() {
//
//            @Override
//            public void close() {
//              actual.close();
//            }
//
//            @Override
//            public void start() {
//              actual.executor = new Thread();
//              actual.poll(actual.consumer(), actual.consumerConfig());
//            }
//
//            @Override
//            public String id() {
//              return actual.id();
//            }
//          };
//        })
//        .when(this.consumerController)
//        .getInstance(any(), any());


    // act
    this.consumerController.consume(consumerConfig);

    // assert
    Thread.sleep(1000);
    this.consumerController.close();
    final var consumer = (DefaultConsumer) this.consumerController
        .getConsumers()
        .stream()
        .findFirst()
        .orElseThrow();

    assertNull(consumer.getConsumerError());

  }

}
