package es.upv.pros.ml4iotbp.domain.datasources.iot;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import es.upv.pros.ml4iotbp.domain.datasources.Variable;

public class IoTDataSourceBase{

  private String paradigm;
  private String protocol;
  private String broker;
  private String method;
  private String format;
  private String host;

  @JsonProperty("time-stamp")
  private String timeStamp;

  @JsonProperty("sampling-time")
  private String samplingTime;

  @JsonProperty("end-point")
  private String endPoint;

  private String topic;
  private String exchange;


  private List<Variable> schema = new ArrayList<>();

  // getters/setters
  public String getParadigm() { return paradigm; }
  public void setParadigm(String paradigm) { this.paradigm = paradigm; }

  public String getProtocol() { return protocol; }
  public void setProtocol(String protocol) { this.protocol = protocol; }

  public String getBroker() { return broker; }
  public void setBroker(String broker) { this.broker = broker; }

  public String getMethod() { return method; }
  public void setMethod(String method) { this.method = method; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getSamplingTime() { return samplingTime; }
  public void setSamplingTime(String samplingTime) { this.samplingTime = samplingTime; }

  public String getEndPoint() { return endPoint; }
  public void setEndPoint(String endPoint) { this.endPoint = endPoint; }

  public String getTopic() { return topic; }
  public void setTopic(String topic) { this.topic = topic; }

  public String getExchange() { return exchange; }
  public void setExchange(String exchange) { this.exchange = exchange; }

  public String getFormat() { return format; }
  public void setFormat(String format) { this.format = format; }

  public String getTimeStamp() { return timeStamp; }
  public void setTimeStamp(String timeStamp) { this.timeStamp = timeStamp; }

  public List<Variable> getSchema() { return schema; }
  public void setSchema(List<Variable> schema) { this.schema = schema; }
}