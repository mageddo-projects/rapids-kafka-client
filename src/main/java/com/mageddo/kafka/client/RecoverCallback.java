package com.mageddo.kafka.client;

public interface RecoverCallback<K, V> {
  void recover(RecoverContext<K, V> ctx);
}
