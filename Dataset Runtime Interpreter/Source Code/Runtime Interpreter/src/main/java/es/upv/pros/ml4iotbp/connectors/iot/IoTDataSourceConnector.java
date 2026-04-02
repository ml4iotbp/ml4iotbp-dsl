package es.upv.pros.ml4iotbp.connectors.iot;

import java.util.Map;

import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;

public class IoTDataSourceConnector {

    public void connect(Map<String, IoTDataSource> iotDs){
            for (Map.Entry<String, IoTDataSource> entry : iotDs.entrySet()) {
                String name = entry.getKey();
                IoTDataSource ds = entry.getValue();
                String paradigm=ds.getParadigm();
                switch(paradigm){
                    case IoTDataSource.HTTP_COMPLIANT: new HTTPCompliantConnector(ds);
                                                        break;
                    case IoTDataSource.MESSAGE_ORIENTED: new MessageOrientedConnector(ds);
                }
            }
    }
    
}
