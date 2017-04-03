package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Pipeline {
  static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

  final String name;
  final List<Rewriter> rewriters = new ArrayList<>();
  final List<Loader> loaders = new ArrayList<>();
  final List<Filter> filters = new ArrayList<>();
  final Scorer scorer;

  public Pipeline(String name, Scorer scorer) {
    this.name = name;
    this.scorer = scorer;
  }

  public Pipeline addRewriter(Rewriter rewriter) {
    this.rewriters.add(rewriter);
    return this;
  }

  public Pipeline addFilter(Filter filter) {
    this.filters.add(filter);
    return this;
  }

  public Pipeline addLoader(Loader loader) {
    this.loaders.add(loader);
    return this;
  }

  public String getName() {
    return this.name;
  }

  public List<Rewriter> getRewriters() {
    return this.rewriters;
  }

  public List<Loader> getLoaders() {
    return this.loaders;
  }

  public List<Filter> getFilters() {
    return this.filters;
  }

  public Scorer getScorer() {
    return this.scorer;
  }

  public Collection<ScoredEntity> execute(Context context) {
    for(Rewriter r : this.rewriters) {
      LOG.info("Applying rewriter '{}'", r.getName());
      context = r.rewrite(context);
    }

    Collection<RootCauseEntityDTO> entities = new ArrayList<>();
    for(Loader l : this.loaders) {
      LOG.info("Using loader '{}'", l.getName());
      entities.addAll(l.load(context));
    }

    for(Filter f : this.filters) {
      LOG.info("Applying filter '{}'", f.getName());
      entities = f.filter(entities, context);
    }

    LOG.info("Using scorer '{}'", this.scorer.getName());
    return this.scorer.score(entities, context);
  }
}
