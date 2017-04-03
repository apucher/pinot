package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Framework {
  static final Logger LOG = LoggerFactory.getLogger(Framework.class);

  final String name;
  final List<Rewriter> rewriters = new ArrayList<>();
  final Collection<Pipeline> pipelines = new ArrayList<>();
  final Aggregator aggregator;

  public Framework(String name, Aggregator aggregator) {
    this.name = name;
    this.aggregator = aggregator;
  }

  public Framework addRewriter(Rewriter rewriter) {
    this.rewriters.add(rewriter);
    return this;
  }

  public Framework addPipeline(Pipeline pipeline) {
    this.pipelines.add(pipeline);
    return this;
  }

  public String getName() {
    return this.name;
  }

  public Collection<Pipeline> getPipelines() {
    return this.pipelines;
  }

  public Aggregator getAggregator() {
    return this.aggregator;
  }

  List<ScoredEntity> execute(Context context) {
    Map<String, Collection<ScoredEntity>> results = new HashMap<>();

    for(Rewriter r : this.rewriters) {
      LOG.info("Applying rewriter '{}'", r.getName());
      context = r.rewrite(context);
    }

    for(Pipeline p : this.pipelines) {
      LOG.info("Executing pipeline '{}'", p.getName());
      results.put(p.getName(), p.execute(context));
    }

    LOG.info("Using aggregator '{}'", this.aggregator.getName());
    return this.aggregator.aggregate(results, context);
  }
}
