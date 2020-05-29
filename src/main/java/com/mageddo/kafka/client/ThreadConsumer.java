package com.mageddo.kafka.client;

public interface ThreadConsumer<K ,V> extends AutoCloseable {
  void start();
  String id();
}
