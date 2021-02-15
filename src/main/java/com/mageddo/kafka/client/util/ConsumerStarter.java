package com.mageddo.kafka.client.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.ConsumerConfigDefault;
import com.mageddo.kafka.client.ConsumerController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class make it easy to set default configs and create many consumers,
 * also states that consumers and make it easy to stop them later.
 *
 * <pre>
 *  public static void main(String[] args) {
 *   final ConsumerStarter consumerStarter = ConsumerStarter.start(defaultConfig(), Arrays.asList(
 *       new StockConsumer() // and many other consumers
 *   ));
 *   consumerStarter.waitFor();
 * //    consumerStarter.stop();
 * }
 *
 * static ConsumerConfig defaultConfig() {
 *   return ConsumerConfig.builder()
 *       .prop(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, &#x22;localhost:9092&#x22;)
 *       .prop(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
 *       .prop(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
 *       .prop(GROUP_ID_CONFIG, &#x22;my-group-id&#x22;)
 *       .build();
 * }
 *
 * static class StockConsumer implements Consumer {
 *
 *   ConsumeCallback&#x3C;String, String&#x3E; consume() {
 *     return (callbackContext, record) -&#x3E; {
 *       System.out.printf(&#x22;message from kafka: %s\n&#x22;, record.value());
 *     };
 *   }
 *
 *   public ConsumerConfig&#x3C;String, String&#x3E; config() {
 *     return ConsumerConfig
 *         .&#x3C;String, String&#x3E;builder()
 *         .topics(&#x22;stocks_events&#x22;)
 *         .consumers(1)
 *         .callback(this.consume())
 *         .build();
 *   }
 * }
 * </pre>
 */
public class ConsumerStarter {

  private static final Logger log = LoggerFactory.getLogger(ConsumerStarter.class);

  private final ConsumerConfig<?, ?> config;
  private final List<ConsumerController<?, ?>> factories;
  private boolean started = false;
  private boolean stopped = false;

  public ConsumerStarter(ConsumerConfig<?, ?> config) {
    this.config = config;
    this.factories = new ArrayList<>();
  }

  public static ConsumerStarter startFromConfig(ConsumerConfig<?, ?> config, List<ConsumerConfig> configs) {
    return new ConsumerStarter(config).startFromConfig(configs);
  }

  public static ConsumerStarter start(ConsumerConfig<?, ?> config, List<Consumer> consumers) {
    return new ConsumerStarter(config).start(consumers);
  }

  public ConsumerStarter start(List<Consumer> consumers) {
    this.startFromConfig(consumers
        .stream()
        .map(Consumer::config)
        .collect(Collectors.toList())
    );
    return this;
  }

  public ConsumerStarter startFromConfig(List<ConsumerConfig> consumers) {
    if (this.started) {
      throw new IllegalStateException("ConsumerConfig were already started");
    }
    this.started = true;
    for (final ConsumerConfig config : consumers) {
      this.factories.add(this.buildConsumer(config)
          .consume());
    }
    return this;
  }

  public void stop() {
    if (this.stopped) {
      throw new IllegalStateException("Already stopped");
    }
    this.stopped = true;
    final ExecutorService executorService = Executors.newScheduledThreadPool(5);
    try {
      final List<Future<String>> futures = new ArrayList<>();
      for (ConsumerController<?, ?> factory : this.factories) {
        futures.add(executorService.submit(() -> {
          try {
            factory.close();
            return factory.toString();
          } catch (Exception e) {
            log.warn("status=failed-to-stop-consumer, consumer={}", factory);
            return String.format("failed to stop: %s, %s", factory.toString(), e.getMessage());
          }
        }));
      }
      for (Future<String> future : futures) {
        try {
          final String id = future.get();
          log.info("status=stopped, factory={}", id);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      executorService.shutdown();
      log.info("status=consumers-stopped, count={}", this.factories.size());

    }
  }

  private ConsumerConfigDefault<?, ?> buildConsumer(ConsumerConfig<?, ?> config) {
    final ConsumerConfigDefault.Builder builder = ConsumerConfigDefault.builderOf(this.config);
    config
        .props()
        .forEach(builder::prop)
    ;
    return builder
        .callback(config.callback())
        .batchCallback(config.batchCallback())
        .topics(config.topics())
        .consumers(config.consumers())
        .recoverCallback(config.recoverCallback())
        .retryPolicy(config.retryPolicy())
        .consumerSupplier(config.consumerSupplier())
        .pollInterval(config.pollInterval())
        .pollTimeout(config.pollTimeout())
        .build()
        ;
  }

  public void waitFor(){
    ConsumerConfigDefault.waitFor();
  }
}
