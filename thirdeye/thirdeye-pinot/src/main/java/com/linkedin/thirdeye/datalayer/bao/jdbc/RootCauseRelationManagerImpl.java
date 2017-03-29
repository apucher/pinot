package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.RootCauseRelationManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseRelationDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseRelationBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.Collection;
import java.util.Collections;


public class RootCauseRelationManagerImpl extends AbstractManagerImpl<RootCauseRelationDTO> implements RootCauseRelationManager {
  protected RootCauseRelationManagerImpl() {
    super(RootCauseRelationDTO.class, RootCauseRelationBean.class);
  }

  @Override
  public Collection<RootCauseRelationDTO> findByType(RootCauseRelationBean.RelationType type) {
    Predicate predicate = Predicate.EQ("type", type.toString());
    return super.findByPredicate(predicate);
  }

  @Override
  public Collection<RootCauseRelationDTO> findOutgoingFrom(Long entityId) {
    return this.findOutgoingFrom(Collections.singleton(entityId));
  }

  @Override
  public Collection<RootCauseRelationDTO> findOutgoingFrom(Collection<Long> entityIds) {
    Predicate predicate = Predicate.IN("fromId", entityIds.toArray(new Long[entityIds.size()]));
    return super.findByPredicate(predicate);
  }

  @Override
  public Collection<RootCauseRelationDTO> findIncomingTo(Long entityId) {
    return this.findIncomingTo(Collections.singleton(entityId));
  }

  @Override
  public Collection<RootCauseRelationDTO> findIncomingTo(Collection<Long> entityIds) {
    Predicate predicate = Predicate.IN("toId", entityIds.toArray(new Long[entityIds.size()]));
    return super.findByPredicate(predicate);
  }
}
