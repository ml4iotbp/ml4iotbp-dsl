package es.upv.pros.ml4iotbp.app;

import java.util.List;
import java.util.Map;

import es.upv.pros.ml4iotbp.domain.ML4IoTBPDocument;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessContext;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessDataSource;
import es.upv.pros.ml4iotbp.domain.features.CompositeFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.SimpleFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.SourceSelection;

public class Logger {

    public static void showRuntimePredictions(ML4IoTBPDocument doc){
        System.out.println("\n=== Runtime Predictions ===");
            if (doc.getRuntimePredictions() != null) {
                doc.getRuntimePredictions().forEach((name, rp) -> {
                    System.out.println("- " + name);
                    System.out.println("  model: " + rp.getModel());
                    System.out.println("  trigger type: " + rp.getTrigger().getType());
                    System.out.println("  input features: " + rp.getInputFeatures());
                });
            }
    }

    public static void showMLModels(ML4IoTBPDocument doc){
        System.out.println("\n=== ML Models ===");
            if (doc.getModels() != null) {
                doc.getModels().forEach((name, model) -> {
                    System.out.println("- " + name);
                    System.out.println("  algorithm: " + model.getAlgorithm());
                    System.out.println("  dataset: " + model.getDataset());
                    System.out.println("  training type: "
                            + (model.getTraining() != null ? model.getTraining().getType() : "N/A"));
                });
            }
    }

    public static void showFeatures(ML4IoTBPDocument doc){
        System.out.println("\n=== Features ===");
            if (doc.getFeatures() != null) {
                doc.getFeatures().forEach((name, feature) -> {
                    System.out.println("- " + name);
                    if(feature.getFrom() instanceof SimpleFeatureFrom){
                        System.out.println("  ds: " + ((SimpleFeatureFrom)feature.getFrom()).getSource());

                        if(feature.getField()!=null) System.out.println("  field: " + feature.getField());
                        if(feature.getFields()!=null){
                            System.out.println("  fields: ");
                            feature.getFields().forEach((f)-> {System.out.println("     - " + f);}); 
                        }
                    }else{
                            CompositeFeatureFrom from=((CompositeFeatureFrom)feature.getFrom());

                            for(Map.Entry<String, SourceSelection> entry: from.getSources().entrySet()){
                                    System.out.println("  ds: " + entry.getKey());
                                    SourceSelection source=entry.getValue();
                                    if(source.getField()!=null) System.out.println("    field: " + source.getField());
                                    if(source.getFields()!=null){
                                        System.out.println("    fields: ");
                                        source.getFields().forEach((f)-> {System.out.println("     - " + f);}); 
                                    }
                            }
                    }
                    
                    System.out.println("  operation: " + feature.getOperation());
                    if(feature.getAnchor()!=null){
                        System.out.println("  anchor: ");
                        System.out.println("    element: " + feature.getAnchor().getElement());
                        System.out.println("    event: " + feature.getAnchor().getEvent());
                        if(feature.getAnchor().getCorrelationKey()!=null) System.out.println("    correlation-key: " + feature.getAnchor().getCorrelationKey());
                    }
                });
            }
    }

    public static void showDataSets(ML4IoTBPDocument doc){
        System.out.println("\n=== Datasets ===");
            if (doc.getDataset() != null) {
                doc.getDataset().forEach((name, dataset) -> {
                    System.out.println("- " + name);
                    System.out.println("  label: " + dataset.getLabel());
                    System.out.println("  train/val/test: "
                            + dataset.getTrainRatio() + " / "
                            + dataset.getValRatio() + " / "
                            + dataset.getTestRatio());
                    System.out.println("  data: ");
                    dataset.getData().forEach((element)->{
                        System.out.println("    - "+element);
                    });;
                });
            }
    }

    public static void showIoTDataSources(ML4IoTBPDocument doc){
        System.out.println("=== IoT Data Sources ===");
            for (Map.Entry<String, IoTDataSource> entry : doc.getIotDataSources().entrySet()) {
            String name = entry.getKey();
            IoTDataSource ds = entry.getValue();

            System.out.println("- " + name);
            System.out.println("  paradigm: " + ds.getParadigm());
            if(ds.getProtocol()!=null) System.out.println("  protocol: " + ds.getProtocol());
            if(ds.getBroker()!=null) System.out.println("  broker: " + ds.getBroker());
            if(ds.getFormat()!=null) System.out.println("  format: " + ds.getFormat());
            if(ds.getHost()!=null) System.out.println("  host: " + ds.getHost());
            if(ds.getTimeStamp()!=null) System.out.println("  timeStamp: " + ds.getTimeStamp());
            if(ds.getExchange()!=null) System.out.println("  exchange: " + ds.getExchange());
            if(ds.getTopic()!=null) System.out.println("  topic: " + ds.getTopic());
            if(ds.getSchema()!=null) {
            System.out.println("  schema: ");
                ds.getSchema().forEach(variable->{
                        System.out.println("    name: " + variable.getVarName());
                        System.out.println("    type: " + variable.getVarType());
                        System.out.println("    internalId: " + ((variable.getInternalId()==null)?"":variable.getInternalId()));
                    });
            }


            if (!ds.getSensors().isEmpty()) {
                System.out.println("  sensors:");
                ds.getSensors().forEach((sensorName, sensor) -> {
                    System.out.println("    • " + sensorName);
                    if(sensor.getProtocol()!=null) System.out.println("     protocol: " + sensor.getProtocol());
                    if(sensor.getBroker()!=null) System.out.println("       broker: " + sensor.getBroker());
                    if(sensor.getFormat()!=null) System.out.println("       format: " + sensor.getFormat());
                    if(sensor.getHost()!=null) System.out.println("     host: " + sensor.getHost());
                    if(sensor.getTimeStamp()!=null) System.out.println("    timeStamp: " + sensor.getTimeStamp());
                    if(sensor.getExchange()!=null) System.out.println("    exchange: " + sensor.getExchange());
                    if(sensor.getTopic()!=null) System.out.println("    topic: " + sensor.getTopic());
                    if(sensor.getSchema().size()>0) {
                        System.out.println("     schema: ");
                        sensor.getSchema().forEach(variable->{
                                System.out.println("            name: " + variable.getVarName());
                                System.out.println("            type: " + variable.getVarType());
                                System.out.println("            internalId: " + ((variable.getInternalId()==null)?"":variable.getInternalId()));
                            });
                    }
                });
            }
        }
    }

    public static void showProcessContext(ML4IoTBPDocument doc){
        System.out.println("=== Process Context ===");
            ProcessContext pc=doc.getProcessContext();
            System.out.println("engine: " + pc.getEngine());
            System.out.println("host: " + pc.getHost());
            System.out.println("processId: " + pc.getProcessId());
            System.out.println("format: " + pc.getFormat());
            System.out.println("timestamp: " + pc.getTimeStampPattern());
            System.out.println("startEvent: " + pc.getStartEvent());
            System.out.println("endEvent: " + pc.getEndEvent());
    }

    public static void showProcessDataSources(ML4IoTBPDocument doc){
        System.out.println("=== Process Data Sources ===");
            for (Map.Entry<String, ProcessDataSource> entry : doc.getProcessDataSources().entrySet()) {
                    String name = entry.getKey();
                    System.out.println("ID: " + name);

                    ProcessDataSource ds=entry.getValue();

                    System.out.println("  Element ID: " + ds.getId());
                    System.out.println("  Type: " + ds.getElementType());
                    System.out.println("  Events: ");
                    ds.getEvents().forEach((event, eventDef) -> {
                        System.out.println("    "+event+":");
                        System.out.println("      Variables: ");
                        eventDef.getVariables().forEach(variable -> {
                            System.out.println("            name: " + variable.getVarName());
                            System.out.println("            type: " + variable.getVarType());
                            System.out.println("            internalId: " + ((variable.getInternalId()==null)?"":variable.getInternalId()));
                        });

                    });
            }
    }
}
