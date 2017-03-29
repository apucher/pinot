package com.linkedin.thirdeye.datalayer.pojo;

public class RootCauseRelationBean extends AbstractBean {
  public enum RelationType {
    METRIC_CORRELATION,
    SYSTEM_METRIC,
    USER_DEFINED,
    UNKNOWN
  }

  RelationType type;
  long fromId;
  long toId;
  double weight;

  public RelationType getType() {
    return type;
  }

  public void setType(RelationType type) {
    this.type = type;
  }

  public long getFromId() {
    return fromId;
  }

  public void setFromId(long fromId) {
    this.fromId = fromId;
  }

  public long getToId() {
    return toId;
  }

  public void setToId(long toId) {
    this.toId = toId;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }
}
