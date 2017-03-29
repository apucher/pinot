package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.RootCauseSessionDTO;


public interface RootCauseSessionManager extends AbstractManager<RootCauseSessionDTO> {

  RootCauseSessionDTO findById(Long id);

}
