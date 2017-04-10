package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean.EntityType;
import com.linkedin.thirdeye.rootcause.impl.ContextLoader;
import com.linkedin.thirdeye.rootcause.impl.LinearAggregator;
import com.linkedin.thirdeye.rootcause.impl.MetricDimensionRewriter;
import com.linkedin.thirdeye.rootcause.impl.TypeExclusionFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class FrameworkTest {
  static class MockLoader implements Loader {
    final Collection<RootCauseEntityDTO> entities;

    public MockLoader(Collection<RootCauseEntityDTO> entities) {
      this.entities = entities;
    }

    @Override
    public String getName() {
      return "MockLoader";
    }

    @Override
    public Collection<RootCauseEntityDTO> load(Context context) {
      return this.entities;
    }
  }

  static class MockScorer implements Scorer {
    @Override
    public String getName() {
      return "MockScorer";
    }

    @Override
    public List<ScoredEntity> score(Collection<RootCauseEntityDTO> entities, Context ignore) {
      List<ScoredEntity> scored = new ArrayList<>(entities.size());
      int i = 0;
      for(RootCauseEntityDTO e : entities) {
        scored.add(new ScoredEntity(e, entities.size() - i));
        i++;
      }
      return scored;
    }
  }

  List<RootCauseEntityDTO> always;
  List<RootCauseEntityDTO> first;
  List<RootCauseEntityDTO> second;
  List<RootCauseEntityDTO> metricDimensions;

  Rewriter mdr;
  Framework fwk;
  Pipeline p1;
  Pipeline p2;

  @BeforeMethod
  void beforeMethod() {
    this.always = Arrays.asList(
        makeEntity(0, EntityType.DIMENSION, "country=us"),
        makeEntity(1, EntityType.DIMENSION, "country=uk"),
        makeEntity(2, EntityType.UNKNOWN, "unknown"));

    this.first = Arrays.asList(
        makeEntity(10, EntityType.METRIC, "metric_a"),
        makeEntity(11, EntityType.METRIC, "metric_b"),
        makeEntity(12, EntityType.SYSTEM, "system_c"));

    this.second = Arrays.asList(
        makeEntity(20, EntityType.DIMENSION, "dimension=a"),
        makeEntity(21, EntityType.DIMENSION, "dimension=b"),
        makeEntity(22, EntityType.EVENT, "event_a"));

    this.metricDimensions = Arrays.asList(
        makeEntity(100, EntityType.METRIC_DIMENSION, "metric_a|country=us"),
        makeEntity(101, EntityType.METRIC_DIMENSION, "metric_a|country=uk"),
        makeEntity(102, EntityType.METRIC_DIMENSION, "metric_b|country=us"));
        // metric_b need not have country=uk dimension

    this.mdr = new MetricDimensionRewriter(new MockEntityManager(metricDimensions));

    this.p1 = new Pipeline("p1", new MockScorer())
        .addLoader(new MockLoader(always))
        .addLoader(new MockLoader(first))
        .addFilter(new TypeExclusionFilter(EntityType.UNKNOWN));

    this.p2 = new Pipeline("p2", new MockScorer())
        .addLoader(new MockLoader(always))
        .addLoader(new MockLoader(second))
        .addFilter(new TypeExclusionFilter(EntityType.UNKNOWN));

    this.fwk = new Framework("test", new LinearAggregator())
        .addRewriter(this.mdr)
        .addPipeline(this.p1)
        .addPipeline(this.p2);
  }

  @Test
  public void testPipeline() {
    Context context = new Context(Collections.<RootCauseEntityDTO>emptyList());
    List<ScoredEntity> res = fromPipelineResult(this.p1.execute(context));

    Assert.assertEquals(res.size(), 5);
    Assert.assertEquals(res.get(0).score, 5.0d);
    Assert.assertEquals(res.get(0).entity.getId(), Long.valueOf(0));
    Assert.assertEquals(res.get(1).score, 4.0d);
    Assert.assertEquals(res.get(1).entity.getId(), Long.valueOf(1));
    Assert.assertEquals(res.get(2).score, 3.0d);
    Assert.assertEquals(res.get(2).entity.getId(), Long.valueOf(10));
    Assert.assertEquals(res.get(3).score, 2.0d);
    Assert.assertEquals(res.get(3).entity.getId(), Long.valueOf(11));
    Assert.assertEquals(res.get(4).score, 1.0d);
    Assert.assertEquals(res.get(4).entity.getId(), Long.valueOf(12));
  }

  @Test
  public void testFramework() {
    Context context = new Context(Collections.<RootCauseEntityDTO>emptyList());
    List<ScoredEntity> res = this.fwk.execute(context);

    Assert.assertEquals(res.size(), 8);
    Assert.assertEquals(res.get(0).score, 10.0d);
    Assert.assertEquals(res.get(0).entity.getId(), Long.valueOf(0));
    Assert.assertEquals(res.get(1).score, 8.0d);
    Assert.assertEquals(res.get(1).entity.getId(), Long.valueOf(1));
    Assert.assertEquals(res.get(2).score, 3.0d);
    Assert.assertEquals(res.get(3).score, 3.0d);
    Assert.assertEquals(res.get(4).score, 2.0d);
    Assert.assertEquals(res.get(5).score, 2.0d);
    Assert.assertEquals(res.get(6).score, 1.0d);
    Assert.assertEquals(res.get(7).score, 1.0d);
  }

  @Test
  public void testContextRewriter() {
    Collection<RootCauseEntityDTO> entities = new ArrayList<>();
    entities.addAll(this.always);
    entities.addAll(this.first);

    Context c = new Context(entities);
    Context out = this.mdr.rewrite(c);

    List<RootCauseEntityDTO> res = new ArrayList<>(out.getEntities());

    Assert.assertEquals(res.size(), 9);
    Assert.assertEquals(res.get(0).getId().longValue(), 0);
    Assert.assertEquals(res.get(1).getId().longValue(), 1);
    Assert.assertEquals(res.get(2).getId().longValue(), 2);
    Assert.assertEquals(res.get(3).getId().longValue(), 10);
    Assert.assertEquals(res.get(4).getId().longValue(), 11);
    Assert.assertEquals(res.get(5).getId().longValue(), 12);
    Assert.assertEquals(res.get(6).getId().longValue(), 100);
    Assert.assertEquals(res.get(7).getId().longValue(), 101);
    Assert.assertEquals(res.get(8).getId().longValue(), 102);
  }

  static RootCauseEntityDTO makeEntity(long id, EntityType type, String name) {
    RootCauseEntityDTO e = new RootCauseEntityDTO();
    e.setId(id);
    e.setType(type);
    e.setName(name);
    return e;
  }

  static List<ScoredEntity> fromPipelineResult(Collection<ScoredEntity> entities) {
    List<ScoredEntity> sorted = new ArrayList<>(entities);
    Collections.sort(sorted, new Comparator<ScoredEntity>() {
      @Override
      public int compare(ScoredEntity o1, ScoredEntity o2) {
        return -Double.compare(o1.getScore(), o2.getScore());
      }
    });
    return sorted;
  }

}
