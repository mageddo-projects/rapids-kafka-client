package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mageddo.kafka.client.internal.ObjectsUtils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.kafka.client.ConsumerConfigDefault.CONSUMERS_NOT_SET;
import static com.mageddo.kafka.client.DefaultConsumingConfig.DEFAULT_RETRY_STRATEGY;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

/**
 * Responsible to states and control consumer threads,
 * here you can start consuming by passing a {@link ConsumerConfig},
 * also can stop the consumer related threads
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class ConsumerController<K, V> implements AutoCloseable {

  private List<ThreadConsumer<K, V>> consumers = new ArrayList<>();
  private boolean started;
  private boolean closed;
  private ConsumerConfig<K, V> consumerConfig;

  public static <K, V> ConsumerSupplier<K, V> defaultConsumerSupplier() {
    return config -> new KafkaConsumer<>(addDefaultConfigs(config));
  }

  private static <K, V> Map<String, Object> addDefaultConfigs(ConsumerCreateConfig<K, V> config) {
    final HashMap<String, Object> props = new HashMap<>(config.props());
    if (!props.containsKey(ENABLE_AUTO_COMMIT_CONFIG)) {
      props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
    }
    if (!props.containsKey(BOOTSTRAP_SERVERS_CONFIG)) {
      props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }
    return props;
  }

  public boolean isRunning() {
    return this.started && !this.closed;
  }

  public void consume(ConsumerConfig<K, V> consumerConfig) {
    if (this.started) {
      throw new IllegalStateException(String.format("Can't start twice: %s", consumerConfig));
    }
    this.started = true;
    this.consumerConfig = consumerConfig;
    final int threads = consumerConfig.consumers() != CONSUMERS_NOT_SET
        ? consumerConfig.consumers()
        : 1;
    if (threads == Integer.MIN_VALUE) {
      log.info(
          "status=disabled-consumer, groupId={}, topics={}",
          this.consumerConfig.groupId(),
          consumerConfig.topics()
      );
      return;
    }
    this.checkReasonablePollInterval(consumerConfig);

    for (int i = 0; i < threads; i++) {
      final ThreadConsumer<K, V> consumer = this.getInstance(this.create(consumerConfig), consumerConfig);
      consumer.start();
    }
    log.info(
        "status=consumers started, threads={}, topics={}, groupId={}",
        this.consumers.size(), consumerConfig.topics(), this.consumerConfig.groupId()
    );
  }

  ThreadConsumer<K, V> getInstance(Consumer<K, V> consumer, ConsumerConfig<K, V> consumerConfig) {
    if (consumerConfig.batchCallback() != null) {
      return this.bindInstance(new BatchConsumer<>(consumer, consumerConfig));
    }
    return this.bindInstance(new RecordConsumer<>(consumer, consumerConfig));
  }

  private ThreadConsumer<K, V> bindInstance(ThreadConsumer<K, V> consumer) {
    this.consumers.add(consumer);
    return consumer;
  }

  Consumer<K, V> create(ConsumerConfig<K, V> consumerConfig) {
    return ObjectsUtils
        .firstNonNull(
            consumerConfig.consumerSupplier(),
            ConsumerController.<K, V>defaultConsumerSupplier()
        )
        .get(consumerConfig);
  }

  private void checkReasonablePollInterval(ConsumerConfig<K, V> consumerConfig) {
    final int defaultPollInterval = (int) Duration
        .ofMinutes(5)
        .toMillis();

    final int currentPollInterval = (int) consumerConfig
        .props()
        .getOrDefault(MAX_POLL_INTERVAL_MS_CONFIG, defaultPollInterval);

    final RetryPolicy retryPolicy = ObjectsUtils.firstNonNull(consumerConfig.retryPolicy(), DEFAULT_RETRY_STRATEGY);
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
    return String.format("ConsumerController(groupId=%s, topics=%s)", this.consumerConfig.groupId(),
        this.consumerConfig.topics()
    );
  }

  List<ThreadConsumer<K, V>> getConsumers() {
    return consumers;
  }
}
