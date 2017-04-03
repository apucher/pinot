package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.bao.RootCauseEntityManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import java.util.ArrayList;
import java.util.Collection;


public class MockEntityManager extends MockManager<RootCauseEntityDTO> implements RootCauseEntityManager {
  final Collection<RootCauseEntityDTO> entities;

  public MockEntityManager(Collection<RootCauseEntityDTO> entities) {
    this.entities = entities;
  }

  @Override
  public RootCauseEntityDTO findByName(String name) {
    for(RootCauseEntityDTO e : this.entities) {
      if(name.equals(e.getName()))
        return e;
    }
    return null;
  }

  @Override
  public Collection<RootCauseEntityDTO> findByType(RootCauseEntityBean.EntityType type) {
    Collection<RootCauseEntityDTO> results = new ArrayList<>();
    for(RootCauseEntityDTO e : this.entities) {
      if(type.equals(e.getType()))
        results.add(e);
    }
    return results;
  }

  @Override
  public Collection<RootCauseEntityDTO> findById(Collection<Long> ids) {
    Collection<RootCauseEntityDTO> results = new ArrayList<>();
    for(RootCauseEntityDTO e : this.entities) {
      if(ids.contains(e.getId()))
        results.add(e);
    }
    return results;
  }
}
