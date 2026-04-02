package es.upv.pros.ml4iotbp.connectors.iot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import es.upv.pros.ml4iotbp.connectors.iot.rabbitmq.RabbitMQConfig;
import es.upv.pros.ml4iotbp.connectors.iot.rabbitmq.RabbitMQConnector;
import es.upv.pros.ml4iotbp.domain.datasources.Variable;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;

public class MessageOrientedConnector extends IoTRepositoryManager{
 
    private RabbitMQConnector rabittConnector=null;

    public MessageOrientedConnector(IoTDataSource ds){

        final String baseDataSourceId=ds.getId();
        final String baseBroker=ds.getBroker();
        final String baseProtocol=ds.getProtocol();
        final String baseHost=ds.getHost();
        final String baseFormat=ds.getFormat();
        final String baseTopic=ds.getTopic();
        final List<Variable> baseSchema=ds.getSchema();
        final String baseTimeStampPattern=ds.getTimeStamp();
        final String baseExchange=ds.getExchange();

        
        switch(baseBroker){
                case IoTDataSource.RABBITMQ:  
                                            try {
                                                RabbitMQConfig cfg=new RabbitMQConfig(baseHost,null,null,null,null);
                                                rabittConnector= new RabbitMQConnector(cfg);
                                            } catch (IOException | TimeoutException e) { 
                                                e.printStackTrace();
                                            } 
                                              break;

            }
       
       
        if (!ds.getSensors().isEmpty()) { // With config
            
            ds.getSensors().forEach((sensorName, sensor) -> {
                String effDataSourceId=sensorName;
                String effBroker = sensor.getBroker() != null ? sensor.getBroker() : baseBroker;
                String effProtocol = sensor.getProtocol() != null ? sensor.getProtocol() : baseProtocol;
                String effHost = sensor.getHost() != null ? sensor.getHost() : baseHost;
                String effFormat = sensor.getFormat() != null ? sensor.getFormat() : baseFormat;
                String effTopic = sensor.getTopic() != null ? sensor.getTopic() : baseTopic;
                List<Variable> effSchema = sensor.getSchema() != null ? sensor.getSchema() : baseSchema;
                String effTimeStampPattern = sensor.getTimeStamp() != null ? sensor.getTimeStamp() : baseTimeStampPattern;
                String effExchange = sensor.getExchange() != null ? sensor.getExchange() : baseExchange;                  

                this.connect(effDataSourceId,
                            effBroker,
                            effProtocol,
                            effHost,
                            effFormat,
                            effTopic,
                            effSchema,
                            effTimeStampPattern,
                            effExchange);
            });
        }else{
            this.connect(baseDataSourceId,
                            baseBroker,
                            baseProtocol,
                            baseHost,
                            baseFormat,
                            baseTopic,
                            baseSchema,
                            baseTimeStampPattern,
                            baseExchange);
        }
    }

    private void connect(String dataSourceId,
                            String broker,
                            String protocol,
                            String host,
                            String format,
                            String topic,
                            List<Variable> schema,
                            String timeStampPattern,
                            String exchange){
       switch(broker){
            case IoTDataSource.RABBITMQ: connectRabbitMQ(dataSourceId,
                            broker,
                            protocol,
                            host,
                            format,
                            topic,
                            schema,
                            timeStampPattern,
                            exchange);
        }
    }

    private void connectRabbitMQ(String dataSourceId,
                            String broker,
                            String protocol,
                            String host,
                            String format,
                            String topic,
                            List<Variable> schema,
                            String timeStampPattern,
                            String exchange){
        
        try {
                 
            rabittConnector.consume(null, exchange, topic, 

                payload -> {
                    this.log(dataSourceId, schema, timeStampPattern, format, payload);
                }  
            );

        } catch (IOException e) {
            e.printStackTrace();
        } 
    }


    
}
