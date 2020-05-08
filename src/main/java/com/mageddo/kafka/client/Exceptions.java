package com.mageddo.kafka.client;

public class Exceptions {
  public static <T extends Throwable> T throwException(Throwable t) throws T {
    throw (T) t;
  }
}
