package es.upv.pros.ml4iotbp.connectors.process.camunda;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import es.upv.pros.ml4iotbp.connectors.process.ProcessEvent;
import es.upv.pros.ml4iotbp.datasets.DatasetConstructor;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessContext;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessDataSource;
import es.upv.pros.ml4iotbp.utils.DurationParser;

public class ProcessDataSourceConnector {

    private ProcessContext processContext;
    private DatasetConstructor datasetConstructor;
    Map<String, ProcessDataSource> processDs;
    private final ScheduledExecutorService scheduler;

    public ProcessDataSourceConnector(){
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "process-data-source");
            t.setDaemon(true);
            return t;
        });
    }

    public void connect(ProcessContext processContext, Map<String, ProcessDataSource> processDs, DatasetConstructor datasetConstructor){

        Duration initalDelay=Duration.ofMillis(0);
        if(processContext.getInitialDelay()!=null) initalDelay=DurationParser.parseTime(processContext.getInitialDelay());

        scheduler.schedule((Runnable) () ->   {
            this.processContext=processContext;
            this.datasetConstructor=datasetConstructor;
            this.processDs=processDs;

            /*for (Map.Entry<String, ProcessDataSource> entry : processDs.entrySet()) {
                        String dsName = entry.getKey();
                        ProcessDataSource ds = entry.getValue(); 

                        dsList.add(ds);              
            }*/

            String engine=processContext.getEngine();
            switch(engine){
                case ProcessContext.CAMUNDA: connectCamunda(); break;  
            }
        }, initalDelay.toMillis(), TimeUnit.MILLISECONDS);
    }


    private void connectCamunda(){

        CamundaConnector connector = new CamundaConnector(processContext);
        
        connector.start(processDs,
                        datasetConstructor.getAnchoredFeatures(),
                        Duration.ofSeconds(15),
                        ev -> {
                            this.processEvent(ev);
                        },
                        err -> {
                            System.err.println("❌ Camunda error: " + err.getMessage());
                            err.printStackTrace();
                        }
        );
       

    }
        
    private void processEvent(ProcessEvent event){
        datasetConstructor.compute(event);
    }

    
}
