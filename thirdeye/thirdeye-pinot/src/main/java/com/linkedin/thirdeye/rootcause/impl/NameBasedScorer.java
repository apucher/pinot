package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.ScoredEntity;
import com.linkedin.thirdeye.rootcause.Scorer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class NameBasedScorer implements Scorer {
  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public List<ScoredEntity> score(Collection<RootCauseEntityDTO> entities, Context ignore) {
    List<RootCauseEntityDTO> sorted = new ArrayList<>(entities);
    Collections.sort(sorted, new Comparator<RootCauseEntityDTO>() {
      @Override
      public int compare(RootCauseEntityDTO o1, RootCauseEntityDTO o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    int counter = 0;
    List<ScoredEntity> scored = new ArrayList<>(sorted.size());
    for(RootCauseEntityDTO e : sorted) {
      scored.add(new ScoredEntity(e, sorted.size() - counter));
      counter++;
    }

    return scored;
  }
}
