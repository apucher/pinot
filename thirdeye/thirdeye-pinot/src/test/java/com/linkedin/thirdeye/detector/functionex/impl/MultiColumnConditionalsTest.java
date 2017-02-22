package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MultiColumnConditionalsTest {
  static final Logger LOG = LoggerFactory.getLogger(MultiColumnConditionalsTest.class);

  static class MockDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
    @Override
    public DataFrame query(String query, AnomalyFunctionExContext context) {
      DataFrame df = new DataFrame(5);
      df.addSeries("long", 3, 4, 5, 6, 7);
      df.addSeries("double", 1.2, 3.5, 2.8, 6.4, 4.9);
      df.addSeries("stable", 1, 1, 1, 1, 1);
      return df;
    }
  }

  Map<String, String> config;
  MultiColumnConditionals func;

  @BeforeMethod
  public void before() {
    config = new HashMap<>();
    config.put("datasource", "mock");
    config.put("query", "select * from my_table");

    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
    context.setConfig(config);
    context.setDataSources(Collections.singletonMap("mock", new MockDataSource()));

    func = new MultiColumnConditionals();
    func.setContext(context);
  }

  @Test
  public void testSingleConditionPass() throws Exception {
    config.put("conditions", "long >= 3");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("all should pass. {}", result.getMessage());
    Assert.assertFalse(result.isAnomaly());
    Assert.assertTrue(result.getMessage().contains("true"));
    Assert.assertFalse(result.getMessage().contains("false"));
  }

  @Test
  public void testSingleConditionFail() throws Exception {
    config.put("conditions", "long > 3");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("none should pass. {}", result.getMessage());
    Assert.assertTrue(result.isAnomaly());
    Assert.assertFalse(result.getMessage().contains("true"));
    Assert.assertTrue(result.getMessage().contains("false"));
  }

  @Test
  public void testMultipleConditionsPass() throws Exception {
    config.put("conditions", "long >= 3, long > 2, long <= 7, long < 8, stable == 1, double != 3");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("all should pass. {}", result.getMessage());
    Assert.assertFalse(result.isAnomaly());
    Assert.assertTrue(result.getMessage().contains("true"));
    Assert.assertFalse(result.getMessage().contains("false"));
  }

  @Test
  public void testMultipleConditionsFail() throws Exception {
    config.put("conditions", "long < 3, long <= 2, long > 7, long >= 8, stable != 1, double == 3");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("none should pass. {}", result.getMessage());
    Assert.assertTrue(result.isAnomaly());
    Assert.assertFalse(result.getMessage().contains("true"));
    Assert.assertTrue(result.getMessage().contains("false"));
  }

  @Test
  public void testMultipleConditionsSingleFail() throws Exception {
    config.put("conditions", "long >= 3, long > 2, long <= 7, long < 8, stable == 1, double != 3, stable != 1");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("one should not pass. {}", result.getMessage());
    Assert.assertTrue(result.isAnomaly());
    Assert.assertTrue(result.getMessage().contains("true"));
    Assert.assertTrue(result.getMessage().contains("false"));
  }

}