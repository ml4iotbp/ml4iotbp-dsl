package es.upv.pros.ml4iotbp.datasets;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import es.upv.pros.ml4iotbp.connectors.DataVar;
import es.upv.pros.ml4iotbp.connectors.process.ProcessEvent;
import es.upv.pros.ml4iotbp.domain.ML4IoTBPDocument;
import es.upv.pros.ml4iotbp.domain.dataset.DataItem;
import es.upv.pros.ml4iotbp.domain.dataset.Dataset;
import es.upv.pros.ml4iotbp.domain.dataset.FeatureMapping;
import es.upv.pros.ml4iotbp.domain.dataset.FeatureRef;
import es.upv.pros.ml4iotbp.domain.datasources.Variable;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;
import es.upv.pros.ml4iotbp.domain.features.CompositeFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.Feature;
import es.upv.pros.ml4iotbp.domain.features.SimpleFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.SourceSelection;
import es.upv.pros.ml4iotbp.domain.features.Feature.Anchor;
import es.upv.pros.ml4iotbp.runtimedata.IoTRepository;
import es.upv.pros.ml4iotbp.utils.DurationParser;

public class DatasetConstructor {

        private Map<String,Map<String,List<Feature>>> anchoredFeatures;
        private Map<String, Dataset> datasetMap;
        private Map<String, Object> row;
        private List<String> fieldsInOrder;
        private Map<String, String> featureRowMap;
        private Map<String, Map<String, DataVar>> instanceVars;
        private ML4IoTBPDocument doc;
        
        private FileWriter out;
        private CSVPrinter printer;
        private int numRows;
        private int addedRows=0;

        public DatasetConstructor(ML4IoTBPDocument doc) {
            this.doc=doc;

            this.row=new Hashtable<String, Object>();
            this.instanceVars=new Hashtable<String, Map<String, DataVar>>();
            this.fieldsInOrder=new ArrayList<String>();
            this.featureRowMap= new Hashtable<String, String>();
            this.datasetMap=doc.getDataset();
            this.anchoredFeatures=new HashMap<String,Map<String,List<Feature>>>();

            doc.getFeatures().forEach((name, feature) -> {
                    feature.setName(name);
                    if(feature.getAnchor()!=null){
                        Map<String,List<Feature>> elements=getElementMap(feature);
                        addEventFeature(elements, feature);
                    }
            });

            //Here we add Process DS that include variables that must be captured in non-anchored events
            doc.getProcessDataSources().forEach((dsName, pds) ->{  
                pds.getEvents().forEach((eventName, eventDef) ->{
                    Feature f=new Feature();
                    f.setName(dsName);
                    f.setOperation(null);
                    Anchor a=new Anchor();
                    a.setElement(pds.getElementId());
                    a.setEvent(eventName);
                    f.setAnchor(a);
                    Map<String,List<Feature>> elements=getElementMap(f);
                    if(elements.get(eventName)==null)
                        addEventFeature(elements, f);
                });
            });

            //showFeature();

            
            try {
                initDataSet();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        public void compute(ProcessEvent event){
         
            if(event.getEventName().equals(ProcessEvent.END_INSTANCE)){
                addRow(event.getProcessInstanceId());
            }else if(!event.getEventName().equals(ProcessEvent.START_INSTANCE)){
                addInstanceVars(event.getProcessInstanceId(), event.getVariables());
                List<Feature> features=anchoredFeatures.get(event.getElementId()).get(event.getEventName());
                for(Feature f:features){
                    if(isProcessFeatured(f)){
                        if(f.getTargetType()!=null) f.setOperation("future_event_exists");
                        calculateProcessFeature(event.getProcessInstanceId(),f);
                    }else{
                         calculateIoTFeature(event.getProcessInstanceId(),event.getTimeStamp(),f);
                    }
                }
            }
        }

        private void initDataSet() throws IOException{
    
            Dataset dataSet=datasetMap.entrySet().iterator().next().getValue();
            this.numRows=(int)dataSet.getNumRows().doubleValue();
            
            String header="INST,";
            for(DataItem column: dataSet.getData()){
                if(column instanceof FeatureRef){
                    FeatureRef col=(FeatureRef )column;  
                    header+=col.getAlias()+",";
                    row.put(col.getAlias(), "");
                    featureRowMap.put(col.getFeatureId(), col.getAlias());
                    fieldsInOrder.add(col.getAlias());
                }else{
                    FeatureMapping col=(FeatureMapping )column;
                    for(Map.Entry<String,String> mapping: col.getFields().entrySet()){
                        String label=mapping.getKey();
                        header+=label+",";
                        row.put(label, "");
                        featureRowMap.put(col.getFeatureId()+"."+mapping.getValue(), label);
                        fieldsInOrder.add(label);
                    };
                }
            };

            if(dataSet.getLabel()!=null){
                String label=dataSet.getLabel().keySet().iterator().next();
                header+=label+",";
                row.put(label, "");
                featureRowMap.put(dataSet.getLabel().get(label), label);
                fieldsInOrder.add(label);
            }

            String[] columns = header.split(",");//.split("\\s*,\\s*");
            this.out = new FileWriter("output.csv");
            this.printer = new CSVPrinter(out,
                     CSVFormat.DEFAULT.builder()
                             .setHeader(columns)
                             .build());

            //header=header.substring(0, header.length()-1);
        }

        private void addRow(String instanceId){
            String rowText=instanceId+",";
            for(String label: fieldsInOrder){
                rowText+=row.get(label)+",";
            }
            rowText=rowText.substring(0, rowText.length()-1);
            //System.out.println(rowText);
            try {
                if(addedRows<numRows){
                    String[] columns = rowText.split(",");
                    printer.printRecord((Object[])columns);
                    printer.flush();
                    addedRows++;
                }else{
                    printer.close();
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }

        private Map<String,List<Feature>> getElementMap(Feature feature){
            String anchor=feature.getAnchor().getElement();
            if(anchoredFeatures.get(anchor)==null){
                Map<String,List<Feature>> elements = new HashMap<String,List<Feature>>();
                anchoredFeatures.put(anchor,elements);
                return elements;
            }else{
                return anchoredFeatures.get(anchor);
            }
        }

        private void addEventFeature(Map<String,List<Feature>> elements, Feature feature){
            String anchor=feature.getAnchor().getEvent();
            if(elements.get(anchor)==null){
                List<Feature> featureList=new ArrayList<Feature>();
                featureList.add(feature);
                elements.put(anchor, featureList);
            }else{
                elements.get(anchor).add(feature);
            }
        }

        private void calculateProcessFeature(String instance, Feature f){
            List<String> featureFields=getFeatureFields(f);
            if(f.getOperation()!=null){
                switch(f.getOperation()){
                    case "include": 
                                    featureFields.forEach((field)->{
                                        String label=featureRowMap.get(f.getName()+"."+field);
                                        if(label!=null){
                                            DataVar var=this.instanceVars.get(instance).get(field);
                                            row.put(label, var.getValue());
                                        }
                                    }); 
                                    break;
                    case "future_event_exists":
                                    String label=featureRowMap.get(f.getName());
                                    if(label!=null){
                                        DataVar var=this.instanceVars.get(instance).get(f.getField());
                                        row.put(label, var.getValue());
                                    }
                                    break;
                    default:
                                    String label2=featureRowMap.get(f.getName());
                                    if(label2!=null){
                                        List<DataVar> vars=new ArrayList<DataVar>();
                                        for (String field : featureFields) 
                                            vars.add(this.instanceVars.get(instance).get(field));

                                        if(instance.equals("b8619822-3444-11f1-89ad-92ec3147fd6f") &&
                                            f.getName().equals("f_manual_inspection_delay")){
                                                System.out.println("Paro");
                                        }
                                        double result=operate(f.getOperation(),vars);          
                                        row.put(label2, result);
                                    }
                                    break;
                }
            }
        }

        private void calculateIoTFeature(String processInstance, long eventTimeStamp, Feature f){
            
            String dsName=((SimpleFeatureFrom)f.getFrom()).getSource();

            IoTDataSource ds= doc.getIotDataSources().get(dsName);
            if(ds==null){
                for(IoTDataSource ds2:doc.getIotDataSources().values()){
                    for(Map.Entry<String, IoTDataSource> ds3: ds2.getSensors().entrySet()){
                        if(ds3.getKey().equals(dsName)) ds=ds3.getValue();
                    };
                }
            }
            if(ds!=null){
                Map<String,DataVar> varMap=new Hashtable<String,DataVar>();
                for(Variable var:ds.getSchema()){
                    DataVar v=new DataVar();
                    v.setName(var.getVarName());
                    v.setType("string");
                    varMap.put(v.getName(), v);
                }
                
                IoTRepository ioTRepository=IoTRepository.getCurrentInstance();
                List<String> featureFields=getFeatureFields(f);
                try {
                    long initInstant=0;
                    try{
                        initInstant=eventTimeStamp-DurationParser.parseTime(f.getWindow()).toMillis();
                        /*System.out.println("---------");
                        System.out.println("--------->"+initInstant);
                        System.out.println("--------->"+eventTimeStamp);
                        System.out.println("--------->"+DurationParser.parseTime(f.getWindow()).toMillis());
                        System.out.println("---------");*/
                    }catch(IllegalArgumentException e){
                        //Comprobamos si es una resta de processVars
                        if(f.getWindow().indexOf("-")>0){
                            String values[]=f.getWindow().split("-");
                            long value1=Long.parseLong(this.instanceVars.get(processInstance).get(values[0].trim()).getValue().toString());
                            long value2=Long.parseLong(this.instanceVars.get(processInstance).get(values[1].trim()).getValue().toString());
                            initInstant=eventTimeStamp-(value1-value2);
                            
                            /*if(processInstance.equals("b8619822-3444-11f1-89ad-92ec3147fd6f") &&
                                f.getName().equals("f_manual_inspection_delay")){
                                System.out.println("End: "+eventTimeStamp);
                                System.out.println("Values: "+value1+"-"+value2);
                                initInstant=eventTimeStamp-(value1-value2);
                                System.out.println("Init: "+initInstant);
                            }*/
                        }
                    }


                    ObjectMapper mapper = new ObjectMapper();
                    ArrayNode list=mapper.createArrayNode();
                    long window=eventTimeStamp-initInstant;
                    int attemps=0;
                    while(list.size()==0 && initInstant>=0 && attemps<5){
                        attemps++;
                        String jsonData=ioTRepository.getCurrentInstance().selectWindow(dsName, initInstant, eventTimeStamp);
                        list = (ArrayNode)mapper.readTree(jsonData);
                        initInstant-=window;
                        /*if(f.getName().equals("f_avg_refrigerator_temp") || f.getName().equals("f_avg_refrigerator_humidity")){
                            System.out.println("-----");
                            System.out.println(f.getName());
                            System.out.println(processInstance);
                            System.out.println(initInstant+"-"+ eventTimeStamp);
                            System.out.println("-----");
                        }*/
                    }
                    
                    
                    List<DataVar> varsToProcess=new ArrayList<DataVar>();
                    for(int i=0;i<list.size();i++){
                        ObjectNode iotData= (ObjectNode) list.get(i);
                        for(String field:featureFields){
                            DataVar v=varMap.get(field);
                            DataVar newV=new DataVar();
                            newV.setName(v.getName());
                            newV.setType(v.getType());
                            newV.setValue(iotData.findValue(field).textValue());
                            varsToProcess.add(newV);
                        }
                    }
                    double result=operate(f.getOperation(),varsToProcess);
                    String label=featureRowMap.get(f.getName());
                    row.put(label, result);
                } catch (JsonProcessingException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private double operate(String op, List<DataVar> vars){
            double result = Double.NaN;
            for (DataVar var : vars) {
                double value = toDouble(var);
                if (Double.isNaN(result)) {
                    result = value;
                } else {
                    switch (op) {
                        case "avg":
                        case "sum": result += value; break;
                        case "substract": result -= value; break;
                        case "min": if(value<result) result=value; break;
                        case "max": if(value>result) result=value; break;
                    }
                }
            }

            if(Double.isNaN(result)) return result;
            else{
                if(op.equals("avg")) result=result/(float)vars.size();
                //BigDecimal bd = BigDecimal.valueOf(result).setScale(2, RoundingMode.HALF_UP);
                double valorRedondeado = Math.round(result * 100.0) / 100.0;
                return valorRedondeado;
            }

            
        }

        private double toDouble(DataVar var) {
            Object v = var.getValue();
            String type = var.getType().toLowerCase();
            return switch (type) {
                case "int", "integer" -> ((Number) v).intValue();
                case "timestamp"     -> ((Number) v).longValue();
                case "long"          -> ((Number) v).longValue();
                case "float"         -> ((Number) v).floatValue();
                case "double"        -> ((Number) v).doubleValue();
                case "string" -> Double.parseDouble(v.toString());
                default -> throw new IllegalArgumentException(
                        "Unsupported numeric type for subtract: " + var.getType()
                );
            };
        }

        private List<String> getFeatureFields(Feature f){
            List<String> fields=new ArrayList<String>();
            if(f.getFrom()==null){ // This is a feature artificially created to add Process DS events
                doc.getProcessDataSources().get(f.getName()).getEvents().forEach((name, eventDef)->{
                    eventDef.getVariables().forEach((var)->{
                        fields.add(var.getInternalId());
                    });
                });
            }
            else if(f.getFrom() instanceof SimpleFeatureFrom){
                if(f.getField()!=null) fields.add(f.getField());
                else{
                    f.getFields().forEach((field)->{
                        fields.add(field);
                    });
                }
            }else{
                CompositeFeatureFrom from=((CompositeFeatureFrom)f.getFrom());
                for(Map.Entry<String, SourceSelection> entry: from.getSources().entrySet()){
                    SourceSelection source=entry.getValue();
                    if(source.getField()!=null) fields.add(source.getField());
                    if(source.getFields()!=null){
                        source.getFields().forEach((field)-> {fields.add(field);}); 
                    }
                }
            }
            return fields;
        }

        private void addInstanceVars(String instanceId, List<DataVar> variables){
            Map<String,DataVar> vars=this.instanceVars.get(instanceId);
            if(vars==null) vars=new Hashtable<String,DataVar>();
            for(DataVar v: variables){
                vars.put(v.getName(),v);
            }
            this.instanceVars.put(instanceId, vars);
        }

        public Map<String, Map<String, List<Feature>>> getAnchoredFeatures() {
            return anchoredFeatures;
        }

        private boolean isProcessFeatured(Feature f){
            if(f.getFrom()==null){ // This is a feature artificially created to add Process DS events
                return true;
            }
            else if(f.getFrom() instanceof SimpleFeatureFrom){
                SimpleFeatureFrom from=(SimpleFeatureFrom)f.getFrom();
                return doc.getProcessDataSources().get(from.getSource())!=null;
            }else{
                CompositeFeatureFrom from=(CompositeFeatureFrom)f.getFrom();
                for(String source:from.getSources().keySet()){
                    if(doc.getProcessDataSources().get(source)==null) return false;
                }
                return true;
            }
        }

        private void showFeature(){
             anchoredFeatures.forEach((element, events) -> {
                    System.out.println(element+": ");
                    events.forEach((event, featureList) -> {
                        System.out.print("  "+event+": ");
                        featureList.forEach((f)->{System.out.print(" "+f.getName());});
                        System.out.println(" ");
                    });
            });
        }

}
