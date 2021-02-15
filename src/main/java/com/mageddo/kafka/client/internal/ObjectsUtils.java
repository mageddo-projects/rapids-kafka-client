package com.mageddo.kafka.client.internal;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ObjectsUtils {
  /**
   * Copied from apache commons lang
   * @see
   * <a href="https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/ObjectUtils.html#firstNonNull-T...-">firstNonNull</a>
   */
  @SafeVarargs
  public static <T> T firstNonNull(final T... values) {
    if (values != null) {
      for (final T val : values) {
        if (val != null) {
          return val;
        }
      }
    }
    return null;
  }
}
