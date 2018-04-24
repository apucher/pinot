package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Objects;


/**
 * ConfigBean holds namespaced key-value configuration values.  Values are serialized into the
 * database using the default object mapper.  ConfigBean serves as a light-weight
 * alternative to existing configuration mechanisms to (a) allow at-runtime changes to configuration
 * traditionally stored in config files, and (b) alleviate the need for introducing new bean classes
 * to handle simple configuration tasks.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionConfigBean extends AbstractBean {
  String className;
  String cron;
  Map<String, Object> properties;

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
}
