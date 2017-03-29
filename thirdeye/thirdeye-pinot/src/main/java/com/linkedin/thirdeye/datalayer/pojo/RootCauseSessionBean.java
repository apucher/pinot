package com.linkedin.thirdeye.datalayer.pojo;

import java.util.HashSet;
import java.util.Set;


public class RootCauseSessionBean extends AbstractBean {
  Set<Long> entityIds = new HashSet<>();
  long timeWindowStart;
  long timeWindowEnd;

  public Set<Long> getEntityIds() {
    return entityIds;
  }

  public void setEntityIds(Set<Long> entityIds) {
    this.entityIds = entityIds;
  }

  public long getTimeWindowStart() {
    return timeWindowStart;
  }

  public void setTimeWindowStart(long timeWindowStart) {
    this.timeWindowStart = timeWindowStart;
  }

  public long getTimeWindowEnd() {
    return timeWindowEnd;
  }

  public void setTimeWindowEnd(long timeWindowEnd) {
    this.timeWindowEnd = timeWindowEnd;
  }
}
