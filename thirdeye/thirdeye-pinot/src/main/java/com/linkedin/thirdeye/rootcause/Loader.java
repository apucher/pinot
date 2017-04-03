package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import java.util.Collection;


public interface Loader {
  String getName();
  Collection<RootCauseEntityDTO> load(Context context);
}
