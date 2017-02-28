package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


public final class StringSeries extends Series {
  public static final String NULL_VALUE = null;

  String[] values;

  @FunctionalInterface
  public interface StringFunction {
    String apply(String value);
  }

  @FunctionalInterface
  public interface StringConditional {
    boolean apply(String value);
  }

  @FunctionalInterface
  public interface StringBatchFunction {
    String apply(String[] values);
  }

  public static class StringBatchConcat implements StringBatchFunction {
    final String delimiter;

    public StringBatchConcat() {
      this.delimiter = "|";
    }

    public StringBatchConcat(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public String apply(String[] values) {
      StringBuilder builder = new StringBuilder();
      for(int i=0; i<values.length - 1; i++) {
        builder.append(values[i]);
        builder.append(this.delimiter);
      }
      builder.append(values[values.length - 1]);
      return builder.toString();
    }
  }

  public static class StringBatchLast implements StringBatchFunction {
    @Override
    public String apply(String[] values) {
      return values[values.length-1];
    }
  }

  public StringSeries(String... values) {
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
  public StringSeries copy() {
    return new StringSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.STRING;
  }

  public String[] values() {
    return this.values;
  }

  public StringSeries unique() {
    Set<String> uniques = new HashSet<>(Arrays.asList(this.values));
    String[] values = new String[uniques.size()];
    return new StringSeries(uniques.toArray(values));
  }

  public List<String> toList() {
    return Arrays.asList(this.values);
  }

  public String first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public String last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public StringSeries slice(int from, int to) {
    return new StringSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public StringSeries head(int n) {
    return (StringSeries) super.head(n);
  }

  @Override
  public StringSeries tail(int n) {
    return (StringSeries) super.tail(n);
  }

  @Override
  public StringSeries reverse() {
    return (StringSeries) super.reverse();
  }

  public StringSeries map(StringFunction function) {
    String[] newValues = new String[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new StringSeries(newValues);
  }

  public BooleanSeries map(StringConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  @Override
  public StringSeries sort() {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return new StringSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StringSeries{");
    for(String s : this.values) {
      builder.append("'");
      builder.append(s);
      builder.append("' ");
    }
    builder.append("}");
    return builder.toString();
  }

  // TODO bucketsBy...

  public StringSeries groupBy(List<Bucket> buckets, StringBatchFunction grouper) {
    return this.groupBy(buckets, NULL_VALUE, grouper);
  }

  public StringSeries groupBy(List<Bucket> buckets, String nullValue, StringBatchFunction grouper) {
    String[] values = new String[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      Bucket b = buckets.get(i);

      // no elements in group
      if(b.fromIndex.length <= 0) {
        values[i] = nullValue;
        continue;
      }

      // group
      String[] gvalues = new String[b.fromIndex.length];
      for(int j=0; j<gvalues.length; j++) {
        gvalues[j] = this.values[b.fromIndex[j]];
      }
      values[i] = grouper.apply(gvalues);
    }
    return new StringSeries(values);
  }

  public StringSeries fillNull(String value) {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return new StringSeries(values);
  }

  @Override
  public StringSeries shift(int offset) {
    String[] values = new String[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), Math.min(-offset, values.length), NULL_VALUE);
    }
    return new StringSeries(values);
  }

  @Override
  StringSeries filter(int[] fromIndex) {
    String[] values = new String[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
    }
    return new StringSeries(values);
  }

  @Override
  StringSeries reorder(int[] toIndex) {
    int len = this.values.length;
    if(toIndex.length != len)
      throw new IllegalArgumentException("toIndex size does not equal series size");

    String[] values = new String[len];
    for(int i=0; i<len; i++) {
      values[toIndex[i]] = this.values[i];
    }
    return new StringSeries(values);
  }

  @Override
  int[] sortedIndex() {
    List<StringSortTuple> tuples = new ArrayList<>();
    for(int i=0; i<this.values.length; i++) {
      tuples.add(new StringSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<StringSortTuple>() {
      @Override
      public int compare(StringSortTuple a, StringSortTuple b) {
        if(a.value == null)
          return b.value == null ? 0 : -1;
        return a.value.compareTo(b.value);
      }
    });

    int[] toIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      toIndex[tuples.get(i).index] = i;
    }
    return toIndex;
  }

  @Override
  public boolean hasNull() {
    for(String v : this.values)
      if(isNull(v))
        return true;
    return false;
  }

  public static boolean isNull(String value) {
    return Objects.equals(value, NULL_VALUE);
  }


  private static String[] assertNotEmpty(String[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static final class StringSortTuple {
    final String value;
    final int index;

    public StringSortTuple(String value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
