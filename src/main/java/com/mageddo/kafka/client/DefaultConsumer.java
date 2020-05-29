package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DefaultConsumer<K, V> implements ThreadConsumer<K, V>, AutoCloseable {

  private AtomicBoolean started = new AtomicBoolean();
  private Thread executor;
  private boolean stopped;

  protected abstract void consume(ConsumerRecords<K, V> records);

  protected abstract Consumer<K, V> consumer();

  protected abstract Consumers<K, V> consumerConfig();

  @Override
  public void start() {
    if (started.get()) {
      log.warn("status=already-started");
      return;
    }
    final Consumer<K, V> consumer = consumer();
    this.executor = new Thread(() -> {
      log.info("status=consumer-starting");
      consumer.subscribe(consumerConfig().topics());
      this.poll(consumer, consumerConfig());
    });
    this.executor.start();
    started.set(true);
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.batchCallback() == null && consumingConfig.callback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    try {
      while (!Thread.currentThread().isInterrupted()) {
        final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.pollTimeout());
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(records);
        if (!Duration.ZERO.equals(consumingConfig.pollInterval())) {
          this.sleep(consumingConfig.pollInterval());
        }
      }
      consumer.unsubscribe();
      consumer.close(Duration.ofSeconds(3));
      log.debug("status=kafka-consumer-released, id={}", this.id());
    } catch (InterruptException e) {}
    log.debug("status=consumer-stopped, id={}", this.id());
    this.stopped = true;
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
      log.warn("status=no recover callback was specified");
    }
  }

  @SneakyThrows
  @Override
  public void close() {
    log.debug("status=closing, id={}", this.id());
    this.executor.interrupt();
    while (this.executor.isAlive()) {
      Thread.sleep(this.consumerConfig()
          .pollInterval()
          .toMillis());
    }
    log.info("status=closed, id={}", this.id());
  }

  @Override
  public String id() {
    return String.format("%d-%s", this.executor.getId(), this.executor.getName());
  }

  public static void main(String[] args) {
    final Thread t = new Thread(() -> {
      while (true) {
        System.out.println("hi!");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    t.start();
    t.interrupt();

    while (true) {
      System.out.println(t.isAlive());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }

  public boolean isStopped() {
    return stopped;
  }
}
