package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.Filter;
import java.util.ArrayList;
import java.util.Collection;


public class ContextExclusionFilter implements Filter {
  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public Collection<RootCauseEntityDTO> filter(Collection<RootCauseEntityDTO> entities, Context context) {
    Collection<RootCauseEntityDTO> filtered = new ArrayList<>();
    for(RootCauseEntityDTO e : entities) {
      if(!context.getEntities().contains(e))
        filtered.add(e);
    }
    return filtered;
  }
}
