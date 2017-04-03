package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import java.util.Map;


public class MockManager<E extends AbstractDTO> implements AbstractManager<E> {
  @Override
  public Long save(E entity) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public int update(E entity) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public E findById(Long id) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public void delete(E entity) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public void deleteById(Long id) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public List<E> findAll() {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public List<E> findByParams(Map<String, Object> filters) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public List<E> findByPredicate(Predicate predicate) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public int update(E entity, Predicate predicate) {
    throw new IllegalStateException("not implemented");
  }
}
