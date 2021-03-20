package com.mageddo.kafka.client.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Threads {
  public static ExecutorService createPool(int size) {
    return Executors.newFixedThreadPool(size, r -> {
      Thread t = Executors.defaultThreadFactory()
          .newThread(r);
      t.setDaemon(true);
      return t;
    });
  }

  public static Thread newThread(Runnable r) {
    return newThread(r, null);
  }

  public static Thread newThread(Runnable r, String id) {
    final Thread t = new Thread(r);
    if (id != null) {
      t.setName(id);
    }
    t.setDaemon(true);
    return t;
  }
}
