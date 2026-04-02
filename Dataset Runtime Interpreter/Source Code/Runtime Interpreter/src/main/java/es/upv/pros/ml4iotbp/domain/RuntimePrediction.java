package es.upv.pros.ml4iotbp.domain;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RuntimePrediction {

  private String model;
  private Trigger trigger;
  private List<String> inputFeatures;
  private Condition condition;

  // getters/setters
  public String getModel() { return model; }
  public void setModel(String model) { this.model = model; }

  public Trigger getTrigger() { return trigger; }
  public void setTrigger(Trigger trigger) { this.trigger = trigger; }

  public List<String> getInputFeatures() { return inputFeatures; }
  public void setInputFeatures(List<String> inputFeatures) { this.inputFeatures = inputFeatures; }

  public Condition getCondition() { return condition; }
  public void setConditionr(Condition condition) { this.condition = condition; }

  public static class Trigger {
    private String type;
    private String every;

    private Map<String, String> from;
    private Map<String, String> to;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getEvery() { return every; }
    public void setEvery(String every) { this.every = every; }

    public Map<String, String> getFrom() { return from; }
    public void setFrom(Map<String, String> from) { this.from = from; }

    public Map<String, String> getTo() { return to; }
    public void setTo(Map<String, String> to) { this.to = to; }
  }

  public static class Condition {
    private String operator;
    private Float threshold;
    @JsonProperty("positive-class")
    private String positiveClass;
    private OnCondition onTrue;
    private OnCondition onFalse;

    public String getOperator() {
      return operator;
    }
    public void setOperator(String operator) {
      this.operator = operator;
    }
    public Float getThreshold() {
      return threshold;
    }
    public void setThreshold(Float threshold) {
      this.threshold = threshold;
    }
    public String getPositiveClass() {
      return positiveClass;
    }
    public void setPositiveClass(String positiveClass) {
      this.positiveClass = positiveClass;
    }
    public OnCondition getOnTrue() {
      return onTrue;
    }
    public void setOnTrue(OnCondition onTrue) {
      this.onTrue = onTrue;
    }
    public OnCondition getOnFalse() {
      return onFalse;
    }
    public void setOnFalse(OnCondition onFalse) {
      this.onFalse = onFalse;
    }

  }

  public static class OnCondition {
    @JsonProperty("native")
    private List<NativeAction> nativeActions;
    
    @JsonProperty("administration")
    private List<AdminAction> adminActions;
   
    @JsonProperty("direct")
    private List<DirectAction> directActions;

    public List<NativeAction> getNativeActions() {
      return nativeActions;
    }

    public void setNativeActions(List<NativeAction> nativeActions) {
      this.nativeActions = nativeActions;
    }

    public List<AdminAction> getAdminActions() {
      return adminActions;
    }

    public void setAdminActions(List<AdminAction> adminActions) {
      this.adminActions = adminActions;
    }

    public List<DirectAction> getDirectActions() {
      return directActions;
    }

    public void setDirectActions(List<DirectAction> directActions) {
      this.directActions = directActions;
    } 

    
  }


  public static class NativeAction{
      private String action; 
      private String name;
      private String value;
      @JsonProperty("process-id")
      private String processId;

      public String getAction() {
        return action;
      }
      public void setAction(String action) {
        this.action = action;
      }
      public String getName() {
        return name;
      }
      public void setName(String name) {
        this.name = name;
      }
      public String getValue() {
        return value;
      }
      public void setValue(String value) {
        this.value = value;
      }
      public String getProcessId() {
        return processId;
      }
      public void setProcessId(String processId) {
        this.processId = processId;
      }
  }

  public static class AdminAction{
    private String task;

    public String getTask() {
      return task;
    }

    public void setTask(String task) {
      this.task = task;
    }
    
    
  }

  public static class DirectAction{
    private String operation; 
    @JsonProperty("element-id")
    private String elementId;

    public String getOperation() {
      return operation;
    }
    public void setOperation(String operation) {
      this.operation = operation;
    }
    public String getElementId() {
      return elementId;
    }
    public void setElementId(String elmentId) {
      this.elementId = elmentId;
    }

    
  }



}