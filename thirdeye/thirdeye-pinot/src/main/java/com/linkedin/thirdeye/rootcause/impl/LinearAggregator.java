package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.ScoredEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LinearAggregator implements Aggregator {
  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public List<ScoredEntity> aggregate(Map<String, Collection<ScoredEntity>> pipelineResults, Context ignore) {
    Map<Long, ScoredEntity> merged = new HashMap<>();
    for(Map.Entry<String, Collection<ScoredEntity>> r : pipelineResults.entrySet()) {
      for(ScoredEntity e : r.getValue()) {
        long eid = e.getEntity().getId();
        if(!merged.containsKey(eid))
          merged.put(eid, new ScoredEntity(e.getEntity(), 0.0));

        ScoredEntity existing = merged.get(eid);
        merged.put(eid, new ScoredEntity(existing.getEntity(), existing.getScore() + e.getScore()));
      }
    }

    List<ScoredEntity> sorted = new ArrayList<>(merged.size());
    for(Map.Entry<Long, ScoredEntity> e : merged.entrySet()) {
      sorted.add(e.getValue());
    }
    Collections.sort(sorted, new Comparator<ScoredEntity>() {
      @Override
      public int compare(ScoredEntity o1, ScoredEntity o2) {
        return -Double.compare(o1.getScore(), o2.getScore());
      }
    });

    return sorted;
  }
}
