package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public final class LongSeries extends Series {
  public static final long NULL_VALUE = Long.MIN_VALUE;

  long[] values;

  @FunctionalInterface
  public interface LongFunction {
    long apply(long value);
  }

  @FunctionalInterface
  public interface LongConditional {
    boolean apply(long value);
  }

  @FunctionalInterface
  public interface LongBatchFunction {
    long apply(long[] values);
  }

  public static class LongBatchSum implements LongBatchFunction {
    @Override
    public long apply(long[] values) {
      long sum = 0;
      for(long l : values)
        sum += l;
      return sum;
    }
  }

  public static class LongBatchMean implements LongBatchFunction {
    @Override
    public long apply(long[] values) {
      assertNotEmpty(values);
      long sum = 0;
      for(long l : values)
        sum += l;
      return sum / values.length;
    }
  }

  public static class LongBatchLast implements LongBatchFunction {
    @Override
    public long apply(long[] values) {
      return values[values.length - 1];
    }
  }

  LongSeries(long... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries toDoubles() {
    return DataFrame.toDoubles(this);
  }

  @Override
  public LongSeries toLongs() {
    return DataFrame.toLongs(this);
  }

  @Override
  public BooleanSeries toBooleans() {
    return DataFrame.toBooleans(this);
  }

  @Override
  public StringSeries toStrings() {
    return DataFrame.toStrings(this);
  }

  @Override
  public LongSeries copy() {
    return new LongSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.LONG;
  }

  public long[] values() {
    return this.values;
  }

  public LongSeries unique() {
    if(this.values.length <= 0)
      return new LongSeries(new long[0]);

    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);

    // first is always unique
    int uniqueCount = 1;

    for(int i=1; i<values.length; i++) {
      if(values[i-1] != values[i]) {
        values[uniqueCount] = values[i];
        uniqueCount++;
      }
    }

    return new LongSeries(Arrays.copyOf(values, uniqueCount));
  }

  public long first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public long last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public LongSeries slice(int from, int to) {
    return new LongSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public LongSeries head(int n) {
    return (LongSeries)super.head(n);
  }

  @Override
  public LongSeries tail(int n) {
    return (LongSeries)super.tail(n);
  }

  @Override
  public LongSeries reverse() {
    return (LongSeries)super.reverse();
  }

  public LongSeries map(LongFunction function) {
    long[] newValues = new long[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new LongSeries(newValues);
  }

  public BooleanSeries map(LongConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  @Override
  LongSeries reorder(int[] toIndex) {
    int len = this.values.length;
    if(toIndex.length != len)
      throw new IllegalArgumentException("toIndex size does not equal series size");

    long[] values = new long[len];
    for(int i=0; i<len; i++) {
      values[toIndex[i]] = this.values[i];
    }
    return new LongSeries(values);
  }

  @Override
  int[] sortedIndex() {
    List<LongSortTuple> tuples = new ArrayList<>();
    for(int i=0; i<this.values.length; i++) {
      tuples.add(new LongSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<LongSortTuple>() {
      @Override
      public int compare(LongSortTuple a, LongSortTuple b) {
        return a.value == b.value ? 0 : a.value <= b.value ? -1 : 1;
      }
    });

    int[] toIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      toIndex[tuples.get(i).index] = i;
    }
    return toIndex;
  }

  @Override
  public LongSeries sort() {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return new LongSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("LongSeries{");
    for(long l : this.values) {
      builder.append(l);
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  public List<Bucket> bucketsByInterval(long interval) {
    if(this.size() <= 0)
      return Collections.EMPTY_LIST;

    // sort series and find ranges
    LongSeries series = this.sort();

    long startOffset = series.first() / interval * interval; // align with interval
    List<Range> ranges = new ArrayList<>();

    int j = 0;
    for(int bucket=0; bucket * interval <= series.last() - startOffset; bucket++) {
      long lower = series.values[j];
      while(j < series.size() && series.values[j] - startOffset < (bucket + 1) * interval) {
        j++;
      }

      long upper;
      if(j < series.size()) {
        upper = series.values[j];
      } else {
        upper = series.values[j - 1] + 1;
      }

      ranges.add(new Range(lower, upper));
    }

    // turn ranges into buckets from original series
    // TODO use nlogm solution to find matching range, e.g. ordered tree
    List<Bucket> buckets = new ArrayList<>();

    for(Range r : ranges) {
      ArrayList<Integer> ind = new ArrayList<>();
      for(int i=0; i<this.size(); i++) {
        if(this.values[i] >= r.lower && this.values[i] < r.upper) {
          ind.add(i);
        }
      }

      int[] fromIndex = new int[ind.size()];
      for(int i=0; i<ind.size(); i++) {
        fromIndex[i] = ind.get(i);
      }

      buckets.add(new Bucket(fromIndex));
    }

    return buckets;
  }

  public List<Bucket> bucketsByCount(int count) {
    if(this.size() <= 0)
      return Collections.EMPTY_LIST;

    List<Bucket> buckets = new ArrayList<>();
    for(int i=0; i<this.size(); i+=count) {

      // last (and potentially smaller) bucket
      int effective = Math.min(count, this.size() - i);

      int[] fromIndex = new int[effective];
      for(int j=0; j<effective; j++) {
        fromIndex[j] = i + j;
      }
      buckets.add(new Bucket(fromIndex));
    }

    return buckets;
  }

  // TODO buckets by bucket count - for deciles, etc.

  public LongSeries groupBy(List<Bucket> buckets, LongBatchFunction grouper) {
    return this.groupBy(buckets, NULL_VALUE, grouper);
  }

  public LongSeries groupBy(List<Bucket> buckets, long nullValue, LongBatchFunction grouper) {
    long[] values = new long[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      Bucket b = buckets.get(i);

      // no elements in group
      if(b.fromIndex.length <= 0) {
        values[i] = nullValue;
        continue;
      }

      // group
      long[] gvalues = new long[b.fromIndex.length];
      for(int j=0; j<gvalues.length; j++) {
        gvalues[j] = this.values[b.fromIndex[j]];
      }
      values[i] = grouper.apply(gvalues);
    }
    return new LongSeries(values);
  }

  public long min() {
    assertNotEmpty(this.values);
    long m = this.values[0];
    for(long n : this.values) {
      m = Math.min(m, n);
    }
    return m;
  }

  public long max() {
    assertNotEmpty(this.values);
    long m = this.values[0];
    for(long n : this.values) {
      m = Math.max(m, n);
    }
    return m;
  }

  public long mean() {
    return new LongBatchMean().apply(this.values);
  }

  public long sum() {
    return new LongBatchSum().apply(this.values);
  }

  @Override
  LongSeries filter(int[] fromIndex) {
    long[] values = new long[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
    }
    return new LongSeries(values);
  }

  public LongSeries fillNull(long value) {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return new LongSeries(values);
  }

  @Override
  public LongSeries shift(int offset) {
    long[] values = new long[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), Math.min(-offset, values.length), NULL_VALUE);
    }
    return new LongSeries(values);
  }

  @Override
  public boolean hasNull() {
    for(long v : this.values)
      if(isNull(v))
        return true;
    return false;
  }

  public static boolean isNull(long value) {
    return value == NULL_VALUE;
  }

  private static long[] assertNotEmpty(long[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static final class LongSortTuple {
    final long value;
    final int index;

    public LongSortTuple(long value, int index) {
      this.value = value;
      this.index = index;
    }
  }

  static class Range {
    final long lower;
    final long upper; // exclusive

    public Range(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }
}
