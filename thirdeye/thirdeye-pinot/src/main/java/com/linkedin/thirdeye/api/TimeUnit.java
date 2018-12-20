package com.linkedin.thirdeye.api;

public enum TimeUnit {
  MILLISECONDS,
  SECONDS,
  MINUTES,
  HOURS,
  DAYS,
  WEEKS,
  MONTHS,
  YEARS;

  public java.util.concurrent.TimeUnit toJavaUnit() {
    switch (this) {
      case MILLISECONDS:
        return java.util.concurrent.TimeUnit.MILLISECONDS;
      case SECONDS:
        return java.util.concurrent.TimeUnit.SECONDS;
      case MINUTES:
        return java.util.concurrent.TimeUnit.MINUTES;
      case HOURS:
        return java.util.concurrent.TimeUnit.HOURS;
      case DAYS:
        return java.util.concurrent.TimeUnit.DAYS;
      default:
        throw new IllegalArgumentException(String.format("com.linkedin.thirdeye.api.TimeUnit '%s' not supported", this));
    }
  }

  public static TimeUnit valueOf(java.util.concurrent.TimeUnit unit) {
    switch (unit) {
      case MILLISECONDS:
        return MILLISECONDS;
      case SECONDS:
        return SECONDS;
      case MINUTES:
        return MINUTES;
      case HOURS:
        return HOURS;
      case DAYS:
        return DAYS;
      default:
        throw new IllegalArgumentException(String.format("java.util.concurrent.TimeUnit '%s' not supported", unit));
    }
  }
}
