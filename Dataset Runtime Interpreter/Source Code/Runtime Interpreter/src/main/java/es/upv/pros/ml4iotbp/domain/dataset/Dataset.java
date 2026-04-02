package es.upv.pros.ml4iotbp.domain.dataset;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import es.upv.pros.ml4iotbp.domain.features.Feature;
import es.upv.pros.ml4iotbp.io.DataItemDeserializer;

public class Dataset {

  /** label: { <SomethingCamelCase>: <featureName> } */
  private Map<String, String> label;

  @JsonProperty("sampling-point")
  private Feature.Anchor samplingPoint;

  @JsonDeserialize(contentUsing = DataItemDeserializer.class)
  private List<DataItem> data;

  @JsonProperty("train-ratio")
  private Double trainRatio;

  @JsonProperty("val-ratio")
  private Double valRatio;

  @JsonProperty("test-ratio")
  private Double testRatio;

  @JsonProperty("num-rows")
  private Double numRows;

  // getters/setters
  public Map<String, String> getLabel() { return label; }
  public void setLabel(Map<String, String> label) { this.label = label; }

  public Feature.Anchor getSamplingPoint() { return samplingPoint; }
  public void setSamplingPoint(Feature.Anchor samplingPoint) { this.samplingPoint = samplingPoint; }

  public List<DataItem> getData() { return data; }
  public void setData(List<DataItem> data) { this.data = data; }

  public Double getTrainRatio() { return trainRatio; }
  public void setTrainRatio(Double trainRatio) { this.trainRatio = trainRatio; }

  public Double getValRatio() { return valRatio; }
  public void setValRatio(Double valRatio) { this.valRatio = valRatio; }

  public Double getTestRatio() { return testRatio; }
  public void setTestRatio(Double testRatio) { this.testRatio = testRatio; }
  
  public Double getNumRows() {return numRows;}
  public void setNumRows(Double numRows) {this.numRows = numRows;}

  
}