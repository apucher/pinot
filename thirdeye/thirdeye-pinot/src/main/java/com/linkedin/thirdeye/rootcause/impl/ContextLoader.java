package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.rootcause.Context;
import com.linkedin.thirdeye.rootcause.Loader;
import java.util.Collection;


public class ContextLoader implements Loader {
  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public Collection<RootCauseEntityDTO> load(Context context) {
    return context.getEntities();
  }
}
