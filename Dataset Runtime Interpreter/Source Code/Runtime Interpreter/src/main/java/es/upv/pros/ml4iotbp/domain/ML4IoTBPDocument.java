package es.upv.pros.ml4iotbp.domain;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import es.upv.pros.ml4iotbp.domain.dataset.Dataset;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessContext;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessDataSource;
import es.upv.pros.ml4iotbp.domain.features.Feature;

/**
 * Representa el documento YAML completo.
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public class ML4IoTBPDocument {

  @JsonProperty("process-context")
  private ProcessContext processContext;

  @JsonProperty("process-data-sources")
  private Map<String, ProcessDataSource> processDataSources;

  @JsonProperty("iot-data-sources")
  private Map<String, IoTDataSource> iotDataSources;

  private Map<String, Feature> features;

  private Map<String, Dataset> dataset;

  private Map<String, MLModel> models;

  @JsonProperty("runtime-predictions")
  private Map<String, RuntimePrediction> runtimePredictions;

  // getters/setters

  public ProcessContext getProcessContext() { return processContext; }
  public void setProcessContext(ProcessContext processContext) { this.processContext = processContext; }

  public Map<String, ProcessDataSource> getProcessDataSources() {return processDataSources;}
  public void setProcessDataSources(Map<String, ProcessDataSource> sources) {this.processDataSources = sources;}

  public Map<String, IoTDataSource> getIotDataSources() { return iotDataSources; }
  public void setIotDataSources(Map<String, IoTDataSource> iotDataSources) { this.iotDataSources = iotDataSources; }

  public Map<String, Feature> getFeatures() { return features; }
  public void setFeatures(Map<String, Feature> features) { this.features = features; }

  public Map<String, Dataset> getDataset() { return dataset; }
  public void setDataset(Map<String, Dataset> dataset) { this.dataset = dataset; }

  public Map<String, MLModel> getModels() { return models; }
  public void setModels(Map<String, MLModel> models) { this.models = models; }

  public Map<String, RuntimePrediction> getRuntimePredictions() { return runtimePredictions; }
  public void setRuntimePredictions(Map<String, RuntimePrediction> runtimePredictions) { this.runtimePredictions = runtimePredictions; }
}