package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;


public final class ScoredEntity {
  final double score;
  final RootCauseEntityDTO entity;

  public double getScore() {
    return score;
  }

  public RootCauseEntityDTO getEntity() {
    return entity;
  }

  public ScoredEntity(RootCauseEntityDTO entity, double score) {
    this.entity = entity;
    this.score = score;
  }
}
