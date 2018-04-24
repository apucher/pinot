package com.linkedin.thirdeye.detection;

import scala.collection.mutable.MultiMap;


class EventSlice {
  final long start;
  final long end;
  final MultiMap<String, String> filters;

  public EventSlice(long start, long end, MultiMap<String, String> filters) {
    this.start = start;
    this.end = end;
    this.filters = filters;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public MultiMap<String, String> getFilters() {
    return filters;
  }
}
