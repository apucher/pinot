package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.RootCauseRelationDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseRelationBean;
import java.util.Collection;


public interface RootCauseRelationManager extends AbstractManager<RootCauseRelationDTO> {

  RootCauseRelationDTO findById(Long id);

  Collection<RootCauseRelationDTO> findByType(RootCauseRelationBean.RelationType type);

  Collection<RootCauseRelationDTO> findOutgoingFrom(Long entityId);

  Collection<RootCauseRelationDTO> findOutgoingFrom(Collection<Long> entityIds);

  Collection<RootCauseRelationDTO> findIncomingTo(Long entityId);

  Collection<RootCauseRelationDTO> findIncomingTo(Collection<Long> entityIds);

}
