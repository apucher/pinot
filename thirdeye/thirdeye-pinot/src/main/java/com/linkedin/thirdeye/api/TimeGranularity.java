/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;


public class TimeGranularity implements Comparable<TimeGranularity> {
  private final int size;
  private final TimeUnit unit;

  public TimeGranularity() {
    this(1, TimeUnit.HOURS);
  }

  public TimeGranularity(int size, TimeUnit unit) {
    this.size = size;
    this.unit = unit;
  }

  /**
   * Copy constructor
   * @param that to be copied
   */
  public TimeGranularity(TimeGranularity that) {
    this(that.getSize(), that.getUnit());
  }

  @JsonProperty
  public int getSize() {
    return size;
  }

  @JsonProperty
  public TimeUnit getUnit() {
    return unit;
  }

  /**
   * Returns the equivalent milliseconds of this time granularity.
   *
   * @return the equivalent milliseconds of this time granularity.
   */
  public long toMillis() {
    // fix semantics
    throw new IllegalStateException("not implemented yet");
  }

  /**
   * Returns the equivalent milliseconds of the specified number of this time granularity,
   * given a time zone.
   *
   * @param epochOffset the specified number of this time granularity.
   * @param timeZone the time zone to base the timestamp off of
   * @return the timestamp in millis
   */
  public long toTimestamp(long epochOffset, DateTimeZone timeZone) {
    if (epochOffset > Integer.MAX_VALUE) {
      // special handling for large epoch offsets
      switch (this.getUnit()) {
        case MILLISECONDS:
          return epochOffset;
        case SECONDS:
          return epochOffset * 1000;
        case MINUTES:
          return epochOffset * 60000;
        default:
          throw new IllegalArgumentException("epoch offset too large");
      }
    }

    return new DateTime(0, timeZone).plus(this.toPeriod((int) epochOffset)).getMillis();
  }

  /**
   * Returns an equivalent Period object of this time granularity.
   *
   * @return an equivalent Period object of this time granularity.
   */
  public Period toPeriod() {
    return toPeriod(1);
  }

  /**
   * Returns an equivalent Period object of the specified number of this time granularity.
   *
   * @param epochOffset the specified number of this time granularity.
   *
   * @return an equivalent Period object of the specified number of this time granularity.
   */
  public Period toPeriod(int epochOffset) {
    int size = this.size * epochOffset;
    switch (unit) {
      case YEARS:
        return new Period().withPeriodType(PeriodType.years()).withField(DurationFieldType.years(), size);
      case MONTHS:
        return new Period().withPeriodType(PeriodType.months()).withField(DurationFieldType.months(), size);
      case WEEKS:
        return new Period().withPeriodType(PeriodType.weeks()).withField(DurationFieldType.weeks(), size);
      case DAYS:
        return new Period().withPeriodType(PeriodType.days()).withField(DurationFieldType.days(), size);
      case HOURS:
        return new Period().withPeriodType(PeriodType.hours()).withField(DurationFieldType.hours(), size);
      case MINUTES:
        return new Period().withPeriodType(PeriodType.minutes()).withField(DurationFieldType.minutes(), size);
      case SECONDS:
        return new Period().withPeriodType(PeriodType.seconds()).withField(DurationFieldType.seconds(), size);
      case MILLISECONDS:
        return new Period().withPeriodType(PeriodType.millis()).withField(DurationFieldType.millis(), size);
    }
    throw new IllegalArgumentException(String.format("Unsupported unit type %s", this.unit));
  }

  /**
   * Converts millis to time unit
   * e.g. If TimeGranularity is defined as 1 HOURS,
   * and we invoke fromTimestamp(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return HOURS.convert(1458284400000, MILLISECONDS)/1 = 405079 hoursSinceEpoch
   * If TimeGranularity is defined as 10 MINUTES,
   * and we invoke fromTimestamp(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return MINUTES.convert(1458284400000, MILLISECONDS)/10 = 2430474
   * tenMinutesSinceEpoch
   * @param datetime timestamp with time zone information
   * @return
   */
  public long fromTimestamp(DateTime datetime) {
    Period period = this.toPeriod();
    DateTime origin = new DateTime(0, datetime.getZone());
    while(!origin.plus(period).isAfter(datetime)) {
      origin = origin.plus(period);
    }
    return origin.getMillis();
  }

  /**
   * Initialize time granularity from its aggregation string representation, in which duration and unit are separated
   * by "_". For instance, "5_MINUTES" initialize a time granularity with size = 5 and TimeUnit = "MINUTES".
   *
   * @param timeGranularityString the aggregation string representation of the time granularity.
   *
   * @return time granularity that is initialized from the given aggregation string representation.
   */
  public static TimeGranularity fromString(String timeGranularityString) {
    if (timeGranularityString.contains("_")) {
      String[] split = timeGranularityString.split("_");
      return new TimeGranularity(Integer.parseInt(split[0]), TimeUnit.valueOf(split[1]));
    } else {
      return new TimeGranularity(1, TimeUnit.valueOf(timeGranularityString));
    }
  }

  /**
   * Return the string representation of this time granularity, in which duration and unit are separated by "_".
   *
   * @return the string representation of this time granularity, in which duration and unit are separated by "_".
   */
  public String toAggregationGranularityString() {
    return size + "_" + unit;
  }

  /**
   * Return the string representation of this time granularity, in which duration and unit are separated by "-".
   *
   * @return the string representation of this time granularity, in which duration and unit are separated by "-".
   */
  @Override
  public String toString() {
    // clean up later, like toAggregationGranularityString()
    return size + "-" + unit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, unit);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TimeGranularity)) {
      return false;
    }
    TimeGranularity other = (TimeGranularity) obj;
    return Objects.equals(other.size, this.size) && Objects.equals(other.unit, this.unit);
  }

  @Override
  public int compareTo(TimeGranularity o) {
    return (int) Math.signum(this.toPeriod().getMillis() - o.toPeriod().getMillis());
  }

  public static int compare(TimeGranularity a, TimeGranularity b) {
    Preconditions.checkNotNull(a);
    Preconditions.checkNotNull(b);
    return a.compareTo(b);
  }
}
