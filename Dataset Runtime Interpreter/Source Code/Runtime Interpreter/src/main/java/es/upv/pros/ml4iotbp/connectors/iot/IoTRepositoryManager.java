package es.upv.pros.ml4iotbp.connectors.iot;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import es.upv.pros.ml4iotbp.domain.datasources.DataSource;
import es.upv.pros.ml4iotbp.domain.datasources.Variable;
import es.upv.pros.ml4iotbp.runtimedata.IoTRepository;

public abstract class IoTRepositoryManager{

    protected void log(String dataSourceId, List<Variable> schema, String timeStampPattern, String format, String payload){
        String logRow=null;
        Instant instant=null;
        try {
            switch(format){
                    case "application/csv": instant=getInstantFromCSV(schema,timeStampPattern, payload);
                                            logRow=fromCSV2JSON(schema,payload);
                                            
                                            break;
                    case "application/json": instant=getInstantFromJSON(schema,timeStampPattern, payload); 
                                            logRow=fromJSON2JSON(schema,payload);
                                            break;
            }

            
            IoTRepository iotRepository = IoTRepository.getCurrentInstance();
            iotRepository.log(instant, dataSourceId, logRow);
            //System.out.println(iotRepository.lastOneAsJSON());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Instant getInstantFromCSV(List<Variable> schema, String timeStampPattern, String payload) throws IOException{
        String timeStampVar=getTimeStampVar(schema);

        Instant instant=null;
        if(timeStampVar!=null){
            CSVFormat format = CSVFormat.DEFAULT.builder()
                    .setHeader()              // detecta la cabecera automáticamente
                    .setSkipHeaderRecord(true)
                    .build();

            CSVParser parser = format.parse(new StringReader(payload));
            CSVRecord record = parser.getRecords().get(0);
            String timeStamp = record.get(timeStampVar);

            instant= DataSource.parseTimeStamp(timeStampPattern, timeStamp);
        }
        return instant;
    }

    private Instant getInstantFromJSON(List<Variable> schema, String timeStampPattern, String payload) throws IOException{
        String timeStampVar=getTimeStampVar(schema);
    
        Instant instant=null;
        if(timeStampVar!=null){
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json= mapper.readTree(payload);
            String timeStamp = json.get(timeStampVar).asText();

            /*DateTimeFormatter fmt =DateTimeFormatter.ofPattern(timeStampPattern, Locale.ENGLISH);
            LocalDateTime ldt = LocalDateTime.parse(timeStamp, fmt);
            instant = ldt.atZone(ZoneId.of("Europe/Madrid")).toInstant();*/

            instant= DataSource.parseTimeStamp(timeStampPattern, timeStamp);
   
        }
        return instant;
    }

    private String getTimeStampVar(List<Variable> schema){
        String timeStampVar=null;
        for(Variable v:schema){
            if(v.getVarType().equals("timeStamp")) timeStampVar=v.getVarName();
        }
        return timeStampVar;
    }

    private String fromCSV2JSON(List<Variable> schema, String payload) throws IOException{
        CSVFormat format = CSVFormat.DEFAULT.builder()
                    .setHeader()              // detecta la cabecera automáticamente
                    .setSkipHeaderRecord(true)
                    .build();
        CSVParser parser = format.parse(new StringReader(payload));
        CSVRecord record = parser.getRecords().get(0);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json= mapper.createObjectNode();
       
        for(Variable v:schema){
            if(!(v.getVarType().equals("timeStamp"))){
                String stringValue = record.get(v.getVarName());
                Object value=null;
                switch(v.getVarType()){
                    case "string":
                    case "date":
                    case "dateTime": value=stringValue;break;
                    case "double":
                    case "number": 
                    case "float": value = Double.parseDouble(stringValue);break;
                    case "integer": value = Integer.parseInt(stringValue);break;
                    case "boolean": value = Boolean.parseBoolean(stringValue);break;
                }
                addValueToJson(json, v.getVarName(), v.getVarType(), value);
            }
        }
        return mapper.writeValueAsString(json);
    }

    private String fromJSON2JSON(List<Variable> schema, String payload) throws IOException{
        
        ObjectMapper mapper = new ObjectMapper();
        JsonNode oldJson= mapper.readTree(payload);
        ObjectNode newJson= mapper.createObjectNode();
  
        for(Variable v:schema){
            if(!v.getVarType().equals("timeStamp")){
                Object value = null;
                switch(v.getVarType()){
                    case "string":
                    case "date":
                    case "dateTime": value = oldJson.get(v.getVarName()).asText();break;
                    case "double":
                    case "number": 
                    case "float": value = oldJson.get(v.getVarName()).asDouble();break;
                    case "integer": value = oldJson.get(v.getVarName()).asInt();break;
                    case "boolean": value = oldJson.get(v.getVarName()).asBoolean();break;
                }
                addValueToJson(newJson, v.getVarName(), v.getVarType(), value);
            }
        }
        
        return mapper.writeValueAsString(newJson);
    }

    private void addValueToJson(ObjectNode json, String varName, String type, Object value){
        switch(type){
            case "string":
            case "date":
            case "dateTime": json.put(varName,value.toString());break;
            case "double":
            case "number": 
            case "float": json.put(varName,(Double)value); break;
            case "integer": json.put(varName,(Integer)value); break;
            case "boolean": json.put(varName,(Boolean)value); break;
        }
    }
}
