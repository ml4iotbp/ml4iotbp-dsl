package es.upv.pros.ml4iotbp.domain.datasources.iot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import es.upv.pros.ml4iotbp.domain.datasources.Variable;

@JsonDeserialize(using = es.upv.pros.ml4iotbp.io.IoTDataSourceDeserializer.class)
public class IoTDataSource{

  //Paradigm constants
  public static final String MESSAGE_ORIENTED="MessageOriented";
  public static final String HTTP_COMPLIANT="HTTPCompliant";
  
  //Message-Oriented Protocol constants
  public static final String MQTT ="MQTT"; 
  public static final String AMQP ="AMQP"; 
  public static final String STOMP ="STOMP"; 
  public static final String XMPP ="XMPP"; 

  //Broker Constants
  public static final String RABBITMQ="RabbitMQ";
  public static final String KAFKA="Kafka";
  public static final String MOSQUITTO="Mosquitto";
  public static final String EMQX="EMQX";
  public static final String HIVEMQ="HiveMQ";

  //HTTP Method constants
  public static final String GET="GET";
  public static final String POST="POST";
  public static final String PUT="PUT";
  public static final String PATCH="PATCH";
  public static final String DELETE="DELETE";

  @JsonIgnore
  private String id;

  private String paradigm;   // MessageOriented | HTTPCompliant
  private String protocol;   // AMQP|MQTT|...
  private String broker;     // Kafka|RabbitMQ|...
  private String method;     // GET|POST|...
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

  /** Sensores como propiedades dinámicas (p.ej. tempSensor:, pressureSensor:) */
  private final Map<String, IoTDataSource> sensors = new LinkedHashMap<>();

  // getters/setters
  public String getId() {return id;}
  public void setId(String id) {this.id = id;}

  public String getParadigm() { return paradigm; }
  public void setParadigm(String paradigm) { this.paradigm = paradigm; }

  public String getProtocol() { return protocol; }
  public void setProtocol(String protocol) { this.protocol = protocol; }

  public String getBroker() { return broker; }
  public void setBroker(String broker) { this.broker = broker; }

  public String getMethod() { return method; }
  public void setMethod(String method) { this.method = method; }

  public String getFormat() { return format; }
  public void setFormat(String format) { this.format = format; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getTimeStamp() { return timeStamp; }
  public void setTimeStamp(String timeStamp) { this.timeStamp = timeStamp; }

  public String getSamplingTime() { return samplingTime; }
  public void setSamplingTime(String samplingTime) { this.samplingTime = samplingTime; }

  public String getEndPoint() { return endPoint; }
  public void setEndPoint(String endPoint) { this.endPoint = endPoint; }

  public String getTopic() { return topic; }
  public void setTopic(String topic) { this.topic = topic; }

  public String getExchange() { return exchange; }
  public void setExchange(String exchange) { this.exchange = exchange; }

  public List<Variable> getSchema() { return schema; }
  public void setSchema(List<Variable> schema) { this.schema = schema; }

  public Map<String, IoTDataSource> getSensors() { return sensors; }


  /** Helper para añadir sensores desde el deserializador. */
  public void putSensor(String name, IoTDataSource sensor) { sensors.put(name, sensor); }

 
}
