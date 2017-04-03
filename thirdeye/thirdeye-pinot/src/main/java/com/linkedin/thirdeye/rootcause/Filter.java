package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import java.util.Collection;


public interface Filter {
  String getName();
  Collection<RootCauseEntityDTO> filter(Collection<RootCauseEntityDTO> entities, Context context);
}
