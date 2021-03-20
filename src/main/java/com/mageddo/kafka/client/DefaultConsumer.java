package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mageddo.kafka.client.internal.ObjectsUtils;
import com.mageddo.kafka.client.internal.Threads;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.kafka.client.DefaultConsumingConfig.DEFAULT_POLL_TIMEOUT;
import static com.mageddo.kafka.client.internal.StringUtils.clearNonAlpha;

@Slf4j
public abstract class DefaultConsumer<K, V> implements ThreadConsumer {

  Thread executor;
  private AtomicBoolean started = new AtomicBoolean();
  private boolean closed;
  private boolean stopped;
  private Exception consumerError;

  protected abstract void consume(ConsumerRecords<K, V> records);

  protected abstract Consumer<K, V> consumer();

  protected abstract ConsumerConfig<K, V> consumerConfig();

  protected abstract int getNumber();

  @Override
  public void start() {
    if (started.get()) {
      log.warn("status=already-started, thread={}, config={}", this.id(), this.consumerConfig());
      return;
    }
    final Consumer<K, V> consumer = consumer();

    this.executor = newThread(() -> {
      this.poll(consumer, consumerConfig());
    });
    this.executor.start();
    started.set(true);
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    try {

      log.debug("status=consumer-thread-starting, id={}", this.id());
      consumer.subscribe(consumerConfig().topics());
      if (consumingConfig.batchCallback() == null && consumingConfig.callback() == null) {
        throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
      }
      log.debug("status=consumer-thread-subscribed, id={}", this.id());

      while (this.mustRun()) {
        final ConsumerRecords<K, V> records = consumer.poll(this.getPollTimeout(consumingConfig));
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(records);
        this.conditionalSleep(consumingConfig);
      }
    } catch (Exception e) {
      log.error("status=thread-stopped-by-failure, id={}", this.id(), e);
      this.consumerError = e;
    } finally {
      try {
        consumer.close();
      } catch (InterruptException e) {
      }
      log.debug("status=thread-stopped, id={}, config={}", this.id(), this.consumerConfig());
      this.stopped = true;
    }
  }

  private Duration getPollTimeout(ConsumingConfig<K, V> consumingConfig) {
    return ObjectsUtils.firstNonNull(consumingConfig.pollTimeout(), DEFAULT_POLL_TIMEOUT);
  }

  @Override
  @SneakyThrows
  public void close() {
    if (this.isClosed()) {
      log.warn("status=already-closed, id={}", this.id());
      return;
    }
    log.debug("status=closing, thread={}, config={}", this.id(), this.consumerConfig());
    this.closed = true;
    while (!this.isStopped()) {
      this.conditionalSleep(this.consumerConfig());
    }
    log.debug("status=closed, thread={}, config={}", this.id(), this.consumerConfig());
  }

  @Override
  public String id() {
    return String.format("%d-%s", this.executor.getId(), this.executor.getName());
  }

  public boolean isClosed() {
    return this.closed;
  }

  public boolean isStopped() {
    return this.stopped;
  }

  void conditionalSleep(ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.pollInterval() != null && !Duration.ZERO.equals(consumingConfig.pollInterval())) {
      this.sleep(consumingConfig.pollInterval());
    }
  }

  protected boolean mustRun() {
    return !this.isClosed() && !Thread.currentThread()
        .isInterrupted();
  }

  /**
   * Sleep for some duration
   */
  protected void sleep(Duration timeout) {
    try {
      TimeUnit.MILLISECONDS.sleep(timeout.toMillis());
    } catch (InterruptedException e) {
      Thread
          .currentThread()
          .interrupt();
    }
  }

  void commitSyncRecord(Consumer<K, V> consumer, ConsumerRecord<K, V> record) {
    consumer.commitSync(Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset())
    ));
  }

  void doRecoverWhenAvailable(RecoverContext<K, V> ctx, RecoverCallback<K, V> recoverCallback) {
    if (recoverCallback != null) {
      recoverCallback.recover(ctx);
      commitSyncRecord(ctx.consumer(), ctx.record());
    } else {
      log.warn(
          "status=no recover callback was specified, ConsumerRecord consume failed and will be discarded, config={}, "
              + "err={}",
          this.consumerConfig(),
          ctx.lastFailure()
              .getMessage()
      );
    }
  }

  Exception getConsumerError() {
    return this.consumerError;
  }

  private Thread newThread(Runnable r) {
    return Threads.newThread(r, this.createThreadId());
  }

  private String createThreadId() {
    return String.format(
        "%d-%s-%s",
        this.getNumber(),
        clearNonAlpha(consumerConfig().groupId()),
        clearNonAlpha(consumerConfig()
            .topics()
            .toString()
        )
    );
  }

}
