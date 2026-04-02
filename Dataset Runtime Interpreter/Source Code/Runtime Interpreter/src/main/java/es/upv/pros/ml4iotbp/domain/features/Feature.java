package es.upv.pros.ml4iotbp.domain.features;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import es.upv.pros.ml4iotbp.io.FeatureFromDeserializer;

public class Feature {

  @JsonIgnore
  private String name;

  @JsonDeserialize(using = FeatureFromDeserializer.class)
  private FeatureFrom from;
  
  private String label;
  private String operation; // avg|min|max|...

  private List<String> fields;
  private String field;

  private String window;
  private Anchor anchor;

  @JsonProperty("target-type")
  private String targetType; // binary|multiclass|regression

  @JsonProperty("positive-class")
  private Object positiveClass; // boolean|string|number

  @JsonProperty("missing-value")
  private String missingValue;

  // getters/setters
  public FeatureFrom getFrom() { return from; }
  public void setFrom(FeatureFrom from) { this.from = from; }

  public String getLabel() { return label; }
  public void setLabel(String label) { this.label = label; }

  public String getOperation() { return operation; }
  public void setOperation(String operation) { this.operation = operation; }

  public List<String> getFields() { return fields; }
  public void setFields(List<String> fields) { this.fields = fields; }

  public String getField() { return field; }
  public void setField(String field) { this.field = field; }

  public String getWindow() { return window; }
  public void setWindow(String window) { this.window = window; }

  public Anchor getAnchor() { return anchor; }
  public void setAnchor(Anchor anchor) { this.anchor = anchor; }

  public String getTargetType() { return targetType; }
  public void setTargetType(String targetType) { this.targetType = targetType; }

  public Object getPositiveClass() { return positiveClass; }
  public void setPositiveClass(Object positiveClass) { this.positiveClass = positiveClass; }

  public String getName() {return name;}
  public void setName(String name) {this.name = name;}

  public String getMissingValue() {return missingValue;}
  public void setMissingValue(String missingValue) {this.missingValue = missingValue;}
  

  public static class Anchor {
    private String element;
    private String event;
     @JsonProperty("correlation-key")
    private String correlationKey;

    public String getElement() { return element; }
    public void setElement(String element) { this.element = element; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    public String getCorrelationKey() {return correlationKey;}
    public void setCorrelationKey(String correlationKey) {this.correlationKey = correlationKey;}
    
  }






}