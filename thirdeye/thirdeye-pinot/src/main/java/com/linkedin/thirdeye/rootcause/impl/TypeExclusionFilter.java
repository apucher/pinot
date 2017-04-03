package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.Filter;
import java.util.ArrayList;
import java.util.Collection;


public class TypeExclusionFilter implements Filter {
  final RootCauseEntityBean.EntityType type;

  public TypeExclusionFilter(RootCauseEntityBean.EntityType type) {
    this.type = type;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Collection<RootCauseEntityDTO> filter(Collection<RootCauseEntityDTO> entities, Context ignore) {
    Collection<RootCauseEntityDTO> results = new ArrayList<>();
    for(RootCauseEntityDTO e : entities) {
      if(!this.type.equals(e.getType()))
        results.add(e);
    }
    return results;
  }
}
