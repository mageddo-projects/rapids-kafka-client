package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

@Slf4j
public class ConsumerController<K, V> implements AutoCloseable {

  private List<ThreadConsumer<K, V>> consumers = new ArrayList<>();
  private boolean started;
  private boolean closed;
  private ConsumerConfigDefault<K, V> consumerConfig;

  public static <K, V> ConsumerSupplier<K, V> defaultConsumerSupplier() {
    return config -> new KafkaConsumer<>(config.props());
  }

  public void consume(ConsumerConfigDefault<K, V> consumerConfig) {
    if (this.started) {
      throw new IllegalStateException(String.format("Can't start twice: %s", consumerConfig));
    }
    this.started = true;
    this.consumerConfig = consumerConfig;
    if (consumerConfig.consumers() == Integer.MIN_VALUE) {
      log.info(
          "status=disabled-consumer, groupId={}, topics={}",
          this.consumerConfig.getGroupId(),
          consumerConfig.topics()
      );
      return;
    }
    this.checkReasonablePollInterval(consumerConfig);

    for (int i = 0; i < consumerConfig.consumers(); i++) {
      final ThreadConsumer<K, V> consumer = getInstance(create(consumerConfig), consumerConfig);
      consumer.start();
    }
    log.info(
        "status=consumers started, threads={}, topics={}, groupId={}",
        this.consumers.size(), consumerConfig.topics(), this.consumerConfig.getGroupId()
    );
  }

  ThreadConsumer<K, V> getInstance(Consumer<K, V> consumer, ConsumerConfigDefault<K, V> consumerConfig) {
    if (consumerConfig.batchCallback() != null) {
      return bindInstance(new BatchConsumer<>(consumer, consumerConfig));
    }
    return bindInstance(new RecordConsumer<>(consumer, consumerConfig));
  }

  private ThreadConsumer<K, V> bindInstance(ThreadConsumer<K, V> consumer) {
    this.consumers.add(consumer);
    return consumer;
  }

  Consumer<K, V> create(ConsumerConfigDefault<K, V> consumerConfig) {
    return consumerConfig.consumerSupplier()
        .get(consumerConfig);
  }

  private void checkReasonablePollInterval(ConsumerConfigDefault<K, V> consumerConfig) {
    final int defaultPollInterval = (int) Duration
        .ofMinutes(5)
        .toMillis();

    final int currentPollInterval = (int) consumerConfig
        .props()
        .getOrDefault(MAX_POLL_INTERVAL_MS_CONFIG, defaultPollInterval);

    final RetryPolicy retryPolicy = consumerConfig.retryPolicy();

    final long retryMaxWaitTime = retryPolicy
        .calcMaxTotalWaitTime()
        .toMillis();

    if (currentPollInterval < retryMaxWaitTime) {
      notifyNotRecommendedRetryPolicy(currentPollInterval, retryPolicy, retryMaxWaitTime);
    }
  }

  void notifyNotRecommendedRetryPolicy(int currentPollInterval, RetryPolicy retryPolicy, long retryMaxWaitTime) {
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

  @Override
  public void close() throws Exception {
    if (this.closed) {
      log.warn("status=already-closed, factory={}", this.toString());
      return;
    }
    for (ThreadConsumer<K, V> consumer : this.consumers) {
      consumer.close();
    }
    this.closed = true;
  }

  /**
   * wait until all consumers threads termination
   */
  @SneakyThrows
  public ConsumerController<K, V> waitFor() {
    Thread.currentThread()
        .join();
    return this;
  }

  @Override
  public String toString() {
    return String.format("ConsumerController(groupId=%s, topics=%s)", this.consumerConfig.getGroupId(), this.consumerConfig.topics());
  }
}
