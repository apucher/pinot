package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.RootCauseEntityManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.Rewriter;
import java.util.ArrayList;
import java.util.Collection;


public class MetricDimensionRewriter implements Rewriter {
  final RootCauseEntityManager entityDAO;

  public MetricDimensionRewriter(RootCauseEntityManager entityDAO) {
    this.entityDAO = entityDAO;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public Context rewrite(Context context) {
    Collection<RootCauseEntityDTO> metrics = new ArrayList<>();
    Collection<RootCauseEntityDTO> dimensions = new ArrayList<>();

    for(RootCauseEntityDTO e : context.getEntities()) {
      if(RootCauseEntityBean.EntityType.METRIC.equals(e.getType())) {
        metrics.add(e);
      }
      if(RootCauseEntityBean.EntityType.DIMENSION.equals(e.getType())) {
        dimensions.add(e);
      }
    }

    Collection<RootCauseEntityDTO> results = new ArrayList<>(context.getEntities());
    for(RootCauseEntityDTO m : metrics) {
      for(RootCauseEntityDTO d : dimensions) {
        RootCauseEntityDTO e = this.entityDAO.findByName(String.format("%s|%s", m.getName(), d.getName()));
        if(e != null && RootCauseEntityBean.EntityType.METRIC_DIMENSION.equals(e.getType()))
          results.add(e);
      }
    }

    return context.withEntities(results);
  }
}
