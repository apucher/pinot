package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import java.util.Collection;


public interface RootCauseEntityManager extends AbstractManager<RootCauseEntityDTO> {

  RootCauseEntityDTO findById(Long id);

  RootCauseEntityDTO findByName(String name);

  Collection<RootCauseEntityDTO> findByType(RootCauseEntityBean.EntityType type);

  Collection<RootCauseEntityDTO> findById(Collection<Long> ids);

}
