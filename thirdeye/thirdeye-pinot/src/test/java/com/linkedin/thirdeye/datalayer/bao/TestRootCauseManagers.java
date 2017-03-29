package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.dto.RootCauseRelationDTO;
import com.linkedin.thirdeye.datalayer.dto.RootCauseSessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean.EntityType;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseRelationBean.RelationType;
import com.linkedin.thirdeye.rootcause.SearchFramework;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestRootCauseManagers extends AbstractManagerTestBase {
  static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  RootCauseEntityManager entityDAO;
  RootCauseRelationManager relationDAO;
  RootCauseSessionManager sessionDAO;

  long e1;
  long e2;
  long e3;
  long e4;
  long e5;

  long r1;
  long r2;
  long r3;
  long r4;
  long r5;
  long r6;
  long r7;

  long s1;
  long s2;
  long s3;

  @BeforeMethod
  void beforeMethod() {
    super.init();
    this.entityDAO = DAO_REGISTRY.getRootCauseEntityDAO();
    this.relationDAO = DAO_REGISTRY.getRootCauseRelationDAO();
    this.sessionDAO = DAO_REGISTRY.getRootCauseSessionDAO();

    e1 = makeEntity("metric_a", EntityType.METRIC);
    e2 = makeEntity("metric_b", EntityType.METRIC);
    e3 = makeEntity("system_a", EntityType.SYSTEM);
    e4 = makeEntity("system_b", EntityType.SYSTEM);
    e5 = makeEntity("user_defined", EntityType.UNKNOWN);

    r1 = makeRelation(RelationType.METRIC_CORRELATION, e1, e2, 0.5);
    r2 = makeRelation(RelationType.METRIC_CORRELATION, e2, e1, 0.5);
    r3 = makeRelation(RelationType.SYSTEM_METRIC, e3, e1, 1.0);
    r4 = makeRelation(RelationType.SYSTEM_METRIC, e1, e3, 0.6);
    r5 = makeRelation(RelationType.SYSTEM_METRIC, e4, e2, 1.0);
    r6 = makeRelation(RelationType.SYSTEM_METRIC, e2, e4, 0.3);
    r7 = makeRelation(RelationType.USER_DEFINED, e4, e5, 0.2);

    s1 = makeSession(Collections.<Long>emptySet());
    s2 = makeSession(Collections.singleton(e1));
    s3 = makeSession(Arrays.asList(e1, e4));
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    super.cleanup();
  }

  @Test
  public void testFindEntityById() {
    RootCauseEntityDTO dto = this.entityDAO.findById(e1);
    Assert.assertEquals(dto.getId(), Long.valueOf(e1));
    Assert.assertEquals(dto.getName(), "metric_a");
    Assert.assertEquals(dto.getType(), EntityType.METRIC);
  }

  @Test
  public void testFindEntityByName() {
    RootCauseEntityDTO dto = this.entityDAO.findByName("metric_a");
    Assert.assertEquals(dto.getId(), Long.valueOf(e1));
    Assert.assertEquals(dto.getName(), "metric_a");
    Assert.assertEquals(dto.getType(), EntityType.METRIC);
  }

  @Test
  public void testFindEntityByIds() {
    Collection<RootCauseEntityDTO> unordered = this.entityDAO.findById(Arrays.asList(e3, e1));
    List<RootCauseEntityDTO> ordered = sortedEntities(unordered);

    Assert.assertEquals(ordered.size(), 2);

    Assert.assertEquals(ordered.get(0).getId(), Long.valueOf(e1));
    Assert.assertEquals(ordered.get(0).getName(), "metric_a");
    Assert.assertEquals(ordered.get(0).getType(), EntityType.METRIC);

    Assert.assertEquals(ordered.get(1).getId(), Long.valueOf(e3));
    Assert.assertEquals(ordered.get(1).getName(), "system_a");
    Assert.assertEquals(ordered.get(1).getType(), EntityType.SYSTEM);
  }

  @Test
  public void testFindEntityByType() {
    Collection<RootCauseEntityDTO> unordered = this.entityDAO.findByType(EntityType.SYSTEM);
    List<RootCauseEntityDTO> ordered = sortedEntities(unordered);

    Assert.assertEquals(ordered.size(), 2);

    Assert.assertEquals(ordered.get(0).getId(), Long.valueOf(e3));
    Assert.assertEquals(ordered.get(0).getName(), "system_a");
    Assert.assertEquals(ordered.get(0).getType(), EntityType.SYSTEM);

    Assert.assertEquals(ordered.get(1).getId(), Long.valueOf(e4));
    Assert.assertEquals(ordered.get(1).getName(), "system_b");
    Assert.assertEquals(ordered.get(1).getType(), EntityType.SYSTEM);
  }

  @Test
  public void testFindRelationByType() {
    Collection<RootCauseRelationDTO> unordered = this.relationDAO.findByType(RelationType.SYSTEM_METRIC);
    List<RootCauseRelationDTO> ordered = sortedRelations(unordered);

    Assert.assertEquals(ordered.size(), 4);

    Assert.assertEquals(ordered.get(0).getId(), Long.valueOf(r3));
    Assert.assertEquals(ordered.get(0).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(0).getFromId(), e3);
    Assert.assertEquals(ordered.get(0).getToId(), e1);
    Assert.assertEquals(ordered.get(0).getWeight(), 1.0);

    Assert.assertEquals(ordered.get(1).getId(), Long.valueOf(r4));
    Assert.assertEquals(ordered.get(1).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(1).getFromId(), e1);
    Assert.assertEquals(ordered.get(1).getToId(), e3);
    Assert.assertEquals(ordered.get(1).getWeight(), 0.6);

    Assert.assertEquals(ordered.get(2).getId(), Long.valueOf(r5));
    Assert.assertEquals(ordered.get(2).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(2).getFromId(), e4);
    Assert.assertEquals(ordered.get(2).getToId(), e2);
    Assert.assertEquals(ordered.get(2).getWeight(), 1.0);

    Assert.assertEquals(ordered.get(3).getId(), Long.valueOf(r6));
    Assert.assertEquals(ordered.get(3).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(3).getFromId(), e2);
    Assert.assertEquals(ordered.get(3).getToId(), e4);
    Assert.assertEquals(ordered.get(3).getWeight(), 0.3);
  }

  @Test
  public void testFindRelationsByOutgoingFrom() {
    Collection<RootCauseRelationDTO> unordered = this.relationDAO.findOutgoingFrom(e1);
    List<RootCauseRelationDTO> ordered = sortedRelations(unordered);

    Assert.assertEquals(ordered.size(), 2);

    Assert.assertEquals(ordered.get(0).getId(), Long.valueOf(r1));
    Assert.assertEquals(ordered.get(0).getType(), RelationType.METRIC_CORRELATION);
    Assert.assertEquals(ordered.get(0).getFromId(), e1);
    Assert.assertEquals(ordered.get(0).getToId(), e2);
    Assert.assertEquals(ordered.get(0).getWeight(), 0.5);

    Assert.assertEquals(ordered.get(1).getId(), Long.valueOf(r4));
    Assert.assertEquals(ordered.get(1).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(1).getFromId(), e1);
    Assert.assertEquals(ordered.get(1).getToId(), e3);
    Assert.assertEquals(ordered.get(1).getWeight(), 0.6);
  }

  @Test
  public void testFindRelationsByIncomingTo() {
    Collection<RootCauseRelationDTO> unordered = this.relationDAO.findIncomingTo(e2);
    List<RootCauseRelationDTO> ordered = sortedRelations(unordered);

    Assert.assertEquals(ordered.size(), 2);

    Assert.assertEquals(ordered.get(0).getId(), Long.valueOf(r1));
    Assert.assertEquals(ordered.get(0).getType(), RelationType.METRIC_CORRELATION);
    Assert.assertEquals(ordered.get(0).getFromId(), e1);
    Assert.assertEquals(ordered.get(0).getToId(), e2);
    Assert.assertEquals(ordered.get(0).getWeight(), 0.5);

    Assert.assertEquals(ordered.get(1).getId(), Long.valueOf(r5));
    Assert.assertEquals(ordered.get(1).getType(), RelationType.SYSTEM_METRIC);
    Assert.assertEquals(ordered.get(1).getFromId(), e4);
    Assert.assertEquals(ordered.get(1).getToId(), e2);
    Assert.assertEquals(ordered.get(1).getWeight(), 1.0);
  }

  @Test
  public void testFindSessionById() {
    RootCauseSessionDTO dto = this.sessionDAO.findById(s1);
    Assert.assertEquals(dto.getId(), Long.valueOf(s1));
  }

  @Test
  public void testInsertEntityNameDuplicateFail() {
    Assert.assertNull(makeEntity("metric_a", EntityType.UNKNOWN));
  }

  @Test
  public void testSearchFrameworkCandidatesEmpty() {
    RootCauseSessionDTO session = this.sessionDAO.findById(s1);

    SearchFramework framework = new SearchFramework(session, this.entityDAO, this.relationDAO);

    List<Long> ids = framework.generateCandidateIds();

    Assert.assertTrue(ids.isEmpty());
  }

  @Test
  public void testSearchFrameworkCandidatesSingle() {
    RootCauseSessionDTO session = this.sessionDAO.findById(s2);

    SearchFramework framework = new SearchFramework(session, this.entityDAO, this.relationDAO);

    List<Long> ids = framework.generateCandidateIds();

    Assert.assertEquals(ids.size(), 2);
    Assert.assertEquals(ids.get(0), Long.valueOf(e3));
    Assert.assertEquals(ids.get(1), Long.valueOf(e2));
  }

  @Test
  public void testSearchFrameworkCandidatesMultiple() {
    RootCauseSessionDTO session = this.sessionDAO.findById(s3);

    SearchFramework framework = new SearchFramework(session, this.entityDAO, this.relationDAO);

    List<Long> ids = framework.generateCandidateIds();

    Assert.assertEquals(ids.size(), 3);
    Assert.assertEquals(ids.get(0), Long.valueOf(e2));
    Assert.assertEquals(ids.get(1), Long.valueOf(e3));
    Assert.assertEquals(ids.get(2), Long.valueOf(e5));
  }

  Long makeEntity(String name, EntityType type) {
    RootCauseEntityDTO dto = new RootCauseEntityDTO();
    dto.setName(name);
    dto.setType(type);
    return this.entityDAO.save(dto);
  }

  Long makeRelation(RelationType type, long fromId, long toId, double weight) {
    RootCauseRelationDTO dto = new RootCauseRelationDTO();
    dto.setType(type);
    dto.setFromId(fromId);
    dto.setToId(toId);
    dto.setWeight(weight);
    return this.relationDAO.save(dto);
  }

  Long makeSession(Collection<Long> entityIds) {
    RootCauseSessionDTO dto = new RootCauseSessionDTO();
    dto.setEntityIds(new HashSet<>(entityIds));
    return this.sessionDAO.save(dto);
  }

  static List<RootCauseEntityDTO> sortedEntities(Collection<RootCauseEntityDTO> unordered) {
    List<RootCauseEntityDTO> ordered = new ArrayList<>(unordered);
    Collections.sort(ordered, new Comparator<RootCauseEntityDTO>() {
      @Override
      public int compare(RootCauseEntityDTO o1, RootCauseEntityDTO o2) {
        return Long.compare(o1.getId(), o2.getId());
      }
    });
    return ordered;
  }

  static List<RootCauseRelationDTO> sortedRelations(Collection<RootCauseRelationDTO> unordered) {
    List<RootCauseRelationDTO> ordered = new ArrayList<>(unordered);
    Collections.sort(ordered, new Comparator<RootCauseRelationDTO>() {
      @Override
      public int compare(RootCauseRelationDTO o1, RootCauseRelationDTO o2) {
        return Long.compare(o1.getId(), o2.getId());
      }
    });
    return ordered;
  }
}
