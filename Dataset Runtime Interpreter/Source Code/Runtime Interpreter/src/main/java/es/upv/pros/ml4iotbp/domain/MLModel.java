package es.upv.pros.ml4iotbp.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class MLModel {

  private String algorithm;

  @JsonProperty("type")
  private String type; 

  private String dataset;

  private Training training;

  /** hyperparameters: { <k>: <string|number> } */
  private Map<String, Object> hyperparameters;

  // getters/setters
  public String getAlgorithm() { return algorithm; }
  public void setAlgorithm(String algorithm) { this.algorithm = algorithm; }

  public String getLearningType() { return type; }
  public void setLearningType(String type) { this.type = type; }

  public String getDataset() { return dataset; }
  public void setDataset(String dataset) { this.dataset = dataset; }

  public Training getTraining() { return training; }
  public void setTraining(Training training) { this.training = training; }

  public Map<String, Object> getHyperparameters() { return hyperparameters; }
  public void setHyperparameters(Map<String, Object> hyperparameters) { this.hyperparameters = hyperparameters; }

  public static class Training {
    private String type; // Batch|Incremental
    private List<String> metrics;
    private Schedule schedule;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<String> getMetrics() { return metrics; }
    public void setMetrics(List<String> metrics) { this.metrics = metrics; }
    public Schedule getSchedule() { return schedule; }
    public void setSchedule(Schedule schedule) { this.schedule = schedule; }
  }

  public static class Schedule {
    private String type; // Periodic|Event-Based
    private String every;
    private List<EventTrigger> events;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getEvery() { return every; }
    public void setEvery(String every) { this.every = every; }
    public List<EventTrigger> getEvents() { return events; }
    public void setEvents(List<EventTrigger> events) { this.events = events; }
  }

  public static class EventTrigger {
    @JsonProperty("element-id")
    private String elementId;
    private String event;

    public String getElementId() { return elementId; }
    public void setElementId(String elementId) { this.elementId = elementId; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
  }
}
