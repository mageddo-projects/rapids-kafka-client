package com.mageddo.kafka.client.internal;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringUtils {
  public static String clearNonAlpha(String text) {
    if (text == null) {
      return null;
    }
    return text.replaceAll("^[a-zA-Z_\\-.]", "");
  }
}
