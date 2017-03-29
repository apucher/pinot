package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.bao.RootCauseEntityManager;
import com.linkedin.thirdeye.datalayer.bao.RootCauseRelationManager;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.dto.RootCauseRelationDTO;
import com.linkedin.thirdeye.datalayer.dto.RootCauseSessionDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SearchFramework {
  RootCauseSessionDTO context;
  RootCauseEntityManager entityDAO;
  RootCauseRelationManager relationDAO;

  public SearchFramework(RootCauseSessionDTO context, RootCauseEntityManager entityDAO,
      RootCauseRelationManager relationDAO) {
    this.context = context;
    this.entityDAO = entityDAO;
    this.relationDAO = relationDAO;
  }

  public static RootCauseSessionDTO startFrom(RootCauseEntityDTO entity) {
    RootCauseSessionDTO context = new RootCauseSessionDTO();
    context.getEntityIds().add(entity.getId());
    return context;
  }

  public List<Long> generateCandidateIds() {
    // NOTE: method can be single SQL expression

    // get all relations associated with entities in the context
    Set<Long> entityIds = new HashSet<>(this.context.getEntityIds());
    entityIds.addAll(context.getEntityIds());
    Set<RootCauseRelationDTO> relations = new HashSet<>(this.relationDAO.findOutgoingFrom(entityIds));

    // rank candidate ids by cumulative strength of relationship
    Map<Long, Ranking> rankings = new HashMap<>();
    for(RootCauseRelationDTO r : relations) {
      long id = r.getToId();

      // ignore entity ids that are part of the search context already
      if(entityIds.contains(id))
        continue;

      if(!rankings.containsKey(id))
        rankings.put(id, new Ranking(id, 0.0d));
      double currentWeight = rankings.get(id).weight;
      rankings.put(id, new Ranking(id, currentWeight + r.getWeight()));
    }

    List<Ranking> ordered = new ArrayList<>(rankings.values());
    Collections.sort(ordered);
    Collections.reverse(ordered);

    // extract candidate ids
    List<Long> candidateIds = new ArrayList<>(ordered.size());
    for(Ranking r : ordered)
      candidateIds.add(r.entityId);

    return candidateIds;
  }

  static final class Ranking implements Comparable<Ranking> {
    final long entityId;
    final double weight;

    public Ranking(long entityId, double weight) {
      this.entityId = entityId;
      this.weight = weight;
    }

    @Override
    public int compareTo(Ranking o) {
      return Double.compare(this.weight, o.weight);
    }
  }
}
