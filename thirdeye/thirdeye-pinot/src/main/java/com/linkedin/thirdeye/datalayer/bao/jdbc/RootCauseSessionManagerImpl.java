package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.RootCauseSessionManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseSessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseSessionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.Collection;


public class RootCauseSessionManagerImpl extends AbstractManagerImpl<RootCauseSessionDTO> implements RootCauseSessionManager {
  protected RootCauseSessionManagerImpl() {
    super(RootCauseSessionDTO.class, RootCauseSessionBean.class);
  }

}
