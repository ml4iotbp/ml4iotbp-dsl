package es.upv.pros.ml4iotbp.domain.datasources.process;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessContext {
  public static final String CAMUNDA = "Camunda";
  public static final String BONITA = "Bonita";
  public static final String ACTIVITI = "Activiti";
  public static final String BIZAGI = "Bizagi";
  public static final String FLOWABLE = "Flowable";
  public static final String JBPM = "jBPM";
  public static final String SIGNAVIO = "Signavio";

  private String engine;
  private String host;
  private String format;

  @JsonProperty("process-id")
  private String processId;

  @JsonProperty("time-stamp")
  private String timeStampPattern;

  @JsonProperty("start-event")
  private String startEvent;

  @JsonProperty("end-event")
  private String endEvent;

  @JsonProperty("initial-delay")
  private String initialDelay;

  @JsonProperty("instance-time")
  private String instanceTime;

  // getters/setters
  public String getEngine() { return engine; }
  public void setEngine(String engine) { this.engine = engine; }
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  public String getProcessId() { return processId; }
  public void setProcessId(String processId) { this.processId = processId; }
  public String getFormat() {return format;}
  public void setFormat(String format) {this.format = format;}
  public String getTimeStampPattern() {return timeStampPattern;}
  public void setTimeStampPattern(String timeStamp) {this.timeStampPattern = timeStamp;}
  public String getStartEvent() {return startEvent;}
  public void setStartEvent(String startEvent) {this.startEvent = startEvent;}
  public String getEndEvent() {return endEvent;}
  public void setEndEvent(String endEvent) {this.endEvent = endEvent;}
  public String getInitialDelay() {return initialDelay;}
  public void setInitialDelay(String initialDelay) {this.initialDelay = initialDelay;}
  public String getInstanceTime() {return instanceTime;}
  public void setInstanceTime(String instanceTime) {this.instanceTime = instanceTime;}



}
