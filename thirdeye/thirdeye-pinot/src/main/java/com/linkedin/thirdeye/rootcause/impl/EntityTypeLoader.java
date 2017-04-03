package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.RootCauseEntityManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.Loader;
import java.util.Collection;


public class EntityTypeLoader implements Loader {
  final RootCauseEntityBean.EntityType type;
  final RootCauseEntityManager entityDAO;

  public EntityTypeLoader(RootCauseEntityBean.EntityType type, RootCauseEntityManager entityDAO) {
    this.type = type;
    this.entityDAO = entityDAO;
  }

  @Override
  public String getName() {
    return String.format("%s(%s)", this.getClass().getSimpleName(), this.type);
  }

  @Override
  public Collection<RootCauseEntityDTO> load(Context ignore) {
    return entityDAO.findByType(type);
  }
}
