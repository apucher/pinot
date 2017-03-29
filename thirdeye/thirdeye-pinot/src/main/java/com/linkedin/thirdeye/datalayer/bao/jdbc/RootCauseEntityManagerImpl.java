package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.RootCauseEntityManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class RootCauseEntityManagerImpl extends AbstractManagerImpl<RootCauseEntityDTO> implements RootCauseEntityManager {
  protected RootCauseEntityManagerImpl() {
    super(RootCauseEntityDTO.class, RootCauseEntityBean.class);
  }

  @Override
  public RootCauseEntityDTO findByName(String name) {
    List<RootCauseEntityDTO> entities = super.findByParams(Collections.singletonMap("name", (Object)name));
    if(entities.isEmpty())
      return null;
    return entities.iterator().next();
  }

  @Override
  public Collection<RootCauseEntityDTO> findByType(RootCauseEntityBean.EntityType type) {
    return super.findByParams(Collections.singletonMap("type", (Object)type.toString()));
  }

  @Override
  public Collection<RootCauseEntityDTO> findById(Collection<Long> ids) {
    Predicate predicate = Predicate.IN("baseId", ids.toArray(new Long[ids.size()]));
    return super.findByPredicate(predicate);
  }
}
