package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import java.util.Collection;


public final class Context {
  final Collection<RootCauseEntityDTO> entities;

  public Context(Collection<RootCauseEntityDTO> entities) {
    this.entities = entities;
  }

  public Collection<RootCauseEntityDTO> getEntities() {
    return entities;
  }

  public Context withEntities(Collection<RootCauseEntityDTO> entities) {
    return new Context(entities);
  }
}
