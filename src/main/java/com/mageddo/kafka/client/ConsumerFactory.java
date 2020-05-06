package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

@Slf4j
public class ConsumerFactory<K, V> {

  private final BatchConsumer<K,V> batchConsumer = new BatchConsumer<>();
  private final RecordConsumer<K,V> recordConsumer = new RecordConsumer<>();

  public void consume(ConsumerConfig<K, V> consumerConfig) {
    this.checkReasonablePollInterval(consumerConfig);
    final Consumer<K, V> consumer = this.create(consumerConfig);
    this.poll(consumer, consumerConfig);
  }

  public KafkaConsumer<K, V> create(final ConsumerCreateConfig<K, V> consumerCreateConfig) {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerCreateConfig.getProps());
    consumer.subscribe(consumerCreateConfig.getTopics());
    return consumer;
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.getBatchCallback() == null && consumingConfig.getCallback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    final boolean batchConsuming = consumingConfig.getBatchCallback() != null;
    while (true) {
      try {
        final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.getTimeout());
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(consumer, consumingConfig, records, batchConsuming);
      } catch (Exception e) {
        log.warn("status=consuming-error", e);
        if (batchConsuming) {
          consumingConfig
              .getBatchCallback()
              .accept(consumer, null, e);
        } else {
          consumingConfig
              .getCallback()
              .accept(consumer, null, e);
        }
      }
      try {
        TimeUnit.MILLISECONDS.sleep(
            consumingConfig
                .getInterval()
                .toMillis()
        );
      } catch (InterruptedException e) {
        Thread
            .currentThread()
            .interrupt();
        break;
      }
    }
  }

  private void consume(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecords<K, V> records,
      boolean batchConsuming
  ) {

    if (log.isTraceEnabled()) {
      log.trace("batch-consuming={}, records={}", batchConsuming, records.count());
    }
    if (batchConsuming) {
      this.batchConsumer.consume(consumer, consumingConfig, records);
    } else {
      this.recordConsumer.consume(consumer, consumingConfig, records);
    }

  }

  private void checkReasonablePollInterval(ConsumerConfig<K, V> consumerConfig) {
    final int defaultPollInterval = (int) Duration
        .ofMinutes(5)
        .toMillis();

    final int currentPollInterval = (int) consumerConfig
        .getProps()
        .getOrDefault(MAX_POLL_INTERVAL_MS_CONFIG, defaultPollInterval);

    final RetryPolicy retryPolicy = consumerConfig.getRetryPolicy();

    final long retryMaxWaitTime = retryPolicy
        .calcMaxTotalWaitTime()
        .toMillis();

    if (currentPollInterval < retryMaxWaitTime) {
      log.warn(
          "msg=your 'max.poll.interval.ms' is set to a value less than the retry policy, it will cause consumer "
              + "rebalancing, increase 'max.poll.interval.ms' or decrease the retry policy delay or retries, "
              + "max.poll.interval.ms={}, retryMaxWaitTime={} (retries={}, delay={})",
          Duration.ofMillis(currentPollInterval),
          Duration.ofMillis(retryMaxWaitTime),
          retryPolicy.getMaxTries(),
          retryPolicy.getDelay()
      );
    }
  }
}
