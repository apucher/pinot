package com.linkedin.thirdeye.rootcause;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface Aggregator {
  String getName();
  List<ScoredEntity> aggregate(Map<String, Collection<ScoredEntity>> pipelineResults, Context context);
}
