package com.linkedin.thirdeye.rootcause.entities;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import java.util.HashMap;
import java.util.Map;


public final class EventEntity {
  public static final String EVENT_ID = "EVENT_ID";
  public static final String EVENT_START = "EVENT_START";
  public static final String EVENT_END = "EVENT_END";
  public static final String EVENT_TYPE = "EVENT_TYPE";

  public static EventEntity loadFromDTO(RootCauseEntityDTO base) {
    if(!RootCauseEntityBean.EntityType.METRIC.equals(base.getType()))
      throw new IllegalArgumentException(String.format("Found type '%s' but requires '%s'",
          base.getType(), RootCauseEntityBean.EntityType.METRIC));

    long eventId = Long.parseLong(base.getProperties().get(EVENT_ID));
    long start = Long.parseLong(base.getProperties().get(EVENT_START));
    long end = Long.parseLong(base.getProperties().get(EVENT_END));
    String type = base.getProperties().get(EVENT_TYPE);

    return new EventEntity(base, eventId, start, end, type);
  }

  public RootCauseEntityDTO saveToDTO() {
    return this.saveToDTO(this.base);
  }

  public RootCauseEntityDTO saveToDTO(RootCauseEntityDTO base) {
    Map<String, String> properties = base.getProperties();
    if(properties == null)
      properties = new HashMap<>();
    properties.put(EVENT_ID, String.valueOf(this.eventId));
    properties.put(EVENT_START, String.valueOf(this.start));
    properties.put(EVENT_END, String.valueOf(this.end));
    return this.base;
  }

  long eventId;
  long start;
  long end;
  String type;
  RootCauseEntityDTO base;

  public EventEntity() {
    // left blank
  }

  public EventEntity(RootCauseEntityDTO base, long eventId, long start, long end, String type) {
    this.eventId = eventId;
    this.start = start;
    this.end = end;
    this.base = base;
    this.type = type;
  }

  public long getEventId() {
    return eventId;
  }

  public void setEventId(long eventId) {
    this.eventId = eventId;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public RootCauseEntityDTO getBase() {
    return base;
  }

  public void setBase(RootCauseEntityDTO base) {
    this.base = base;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
