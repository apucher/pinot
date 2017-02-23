package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.dataframe.DoubleSeries;


public class MinMaxThreshold extends AnomalyFunctionEx {

  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    String column = getConfig("column");
    String datasource = getConfig("datasource");
    String query = getConfig("query");

    double min = Double.parseDouble(getConfig("min"));
    double max = Double.parseDouble(getConfig("max"));

    DataFrame df = queryDataSource(datasource, query);
    DoubleSeries series = df.toDoubles(column);

    boolean pass_min = series.map(new DoubleSeries.DoubleConditional() {
      public boolean apply(double value) {
        return value >= min;
      }
    }).allTrue();

    boolean pass_max = series.map(new DoubleSeries.DoubleConditional() {
      public boolean apply(double value) {
        return value <= max;
      }
    }).allTrue();

    AnomalyFunctionExResult result = new AnomalyFunctionExResult();

    if(!pass_min) {
      result.addAnomaly(getContext().getMonitoringWindowStart(), getContext().getMonitoringWindowEnd(),
          String.format("encountered values below %f", min));
    }
    if(!pass_max) {
      result.addAnomaly(getContext().getMonitoringWindowStart(), getContext().getMonitoringWindowEnd(),
          String.format("encountered values above %f", max));
    }

    return result;
  }
}