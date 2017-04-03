package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import java.util.Collection;
import java.util.List;


public interface Scorer {
  String getName();
  List<ScoredEntity> score(Collection<RootCauseEntityDTO> entities, Context context);
}
