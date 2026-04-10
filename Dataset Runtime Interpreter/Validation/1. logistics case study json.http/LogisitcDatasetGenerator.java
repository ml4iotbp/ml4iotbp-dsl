//236 líneas de código efectivo.
package dataset.generator;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class App {

    private static final String PROCESS_DEFINITION_KEY = "DistributionCenterTest";
    private static final String CAMUNDA_TS = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private HashMap<Long,Float> containerTemp=new HashMap<Long,Float>();
    private HashMap<Long,Float> containerHum=new HashMap<Long,Float>();
    private HashMap<Long,Float> refrTemp=new HashMap<Long,Float>();
    private HashMap<Long,Float> refrHum=new HashMap<Long,Float>();

    public void generateDataSet() {
         
         try{
            
            InputStream camunda_logs = App.class
                .getClassLoader()
                .getResourceAsStream("camunda.json");

            if (camunda_logs == null) {
                throw new RuntimeException("Camunda File not found in resources");
            }

            InputStream iot_logs = App.class
                .getClassLoader()
                .getResourceAsStream("iot_data.json");  

            if (iot_logs == null) {
                throw new RuntimeException("IoT File not found in resources");
            }

            // Parse Camunda JSON
            String jsonText = new String(camunda_logs.readAllBytes(), StandardCharsets.UTF_8);
            JSONArray camundaJson = new JSONArray(jsonText);

            // Parse IoT JSON
            String jsonText2 = new String(iot_logs.readAllBytes(), StandardCharsets.UTF_8);
            JSONArray sensorData = new JSONArray(jsonText2);

            FileWriter file = new FileWriter("processedIoT.csv");
            for(int i=0;i<sensorData.length();i++){
                JSONObject data=sensorData.getJSONObject(i);
                long timeSt=TimeUtils.parseTimeStamp("dd/MM/yyyy HH:mm:ss", data.getString("timeStamp")).toEpochMilli();
                String row=timeSt+","; 
                switch(data.getString("id")){
                    case "sn-hum-538474":refrHum.put(timeSt, data.getFloat("humidity"));
                                         row+="refrigerator_humidity_sensor,"+data.getFloat("humidity")+"\n";
                                         break;
                    case "sn-tmp-142037":refrTemp.put(timeSt, data.getFloat("temperature"));
                                        row+="refrigerator_temperature_sensor,"+data.getFloat("temperature")+"\n";
                                        break;
                    case "cont-142037": containerTemp.put(timeSt, data.getFloat("temperature"));
                                        row+="container_temperature_sensor,"+data.getFloat("temperature")+"\n";
                                        break;
                    case "cont-538474": containerHum.put(timeSt, data.getFloat("humidity"));
                                        row+="container_humidity_sensor,"+data.getFloat("humidity")+"\n";
                                        break;
                }
                file.write(row);
                file.flush();
            }
            file.close();

            String instanceId="";
            HashMap<String,ArrayList<JSONObject>> instances= new HashMap<String,ArrayList<JSONObject>>();
            for(int i=0;i<camundaJson.length();i++){
                JSONObject camundaActivity=camundaJson.getJSONObject(i);

                if(camundaActivity.getString("processDefinitionKey").equals(PROCESS_DEFINITION_KEY)){
                    if(!camundaActivity.getString("processInstanceId").equals(instanceId)){
                        instanceId=camundaActivity.getString("processInstanceId");

                        ArrayList<JSONObject> activities= new ArrayList<JSONObject>();
                        activities.add(camundaActivity);
                        instances.put(instanceId, activities);

                    }else{
                         instances.get(instanceId).add(camundaActivity);
                    }
                }
            }
            
            processInstances(instances);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    private double avgTemp(String source, long starting, long end){
        
        float sum=0;
        int num=0;

        long window=end-starting;
        while(num==0){
            switch(source){
                case "container":   for(Map.Entry<Long,Float> values: containerTemp.entrySet()){
                                        long timestamp=values.getKey();
                                        if(timestamp>=starting && timestamp<=end){
                                            sum+=values.getValue();
                                            num++;
                                        } 
                                    }
                                    break;
                case "refrigerator": for(Map.Entry<Long,Float> values: refrTemp.entrySet()){
                                        long timestamp=values.getKey();
                                        if(timestamp>=starting && timestamp<=end){
                                            sum+=values.getValue();
                                            num++;
                                        }  
                                    }   
                                    break;
            }
            starting-=window;
        }
    
        return Math.round((sum/num) * 100.0) / 100.0;
    }

    private double avgHum(String source, long starting, long end){
        
        float sum=0;
        int num=0;

        long window=end-starting;
        while(num==0){
            switch(source){
                case "container":   for(Map.Entry<Long,Float> values: containerHum.entrySet()){
                                        long timestamp=values.getKey();
                                        if(timestamp>=starting && timestamp<=end){
                                            sum+=values.getValue();
                                            num++;
                                        } 
                                    }
                                    break;
                case "refrigerator": for(Map.Entry<Long,Float> values: refrHum.entrySet()){
                                        long timestamp=values.getKey();
                                        if(timestamp>=starting && timestamp<=end){
                                            sum+=values.getValue();
                                            num++;
                                        }  
                                    }   
                                    break;
            }
            starting-=window;
        }
    
        return Math.round((sum/num) * 100.0) / 100.0;
    }


    private void processInstances(HashMap<String,ArrayList<JSONObject>> instances){
        System.out.println("INST,PI,QF,QC,D,QED,ST,CTB,CHB,CTD,CHD,RT,RH,B");
        for(String instance: instances.keySet()){
            HashMap<String,Object> row=new  HashMap<String,Object>();
            ArrayList<JSONObject> activities=instances.get(instance);

            Instant evaluationCompleted=null;

            for(JSONObject activity: activities){
                String activityId=activity.getString("activityId");

                if(activityId.indexOf("evaluateQualityTask")>=0){
                    String activityInstanceId=activity.getString("id");
                    //crearJSON(activityInstanceId);
                    JSONArray activityVars=getJSONActivityVars(activityInstanceId);

                    for(int i=0;i<activityVars.length();i++){
                        JSONObject var=activityVars.getJSONObject(i);
                        if(var.getString("type").equals("variableUpdate")){
                            if(var.getString("variableName").equals("palletId")){
                                row.put("PI", var.getString("value"));
                            }else if(var.getString("variableName").equals("qualityFirmness")){
                                row.put("QF", var.getString("value"));
                            }else if(var.getString("variableName").equals("qualityColor")){
                                row.put("QC", var.getString("value"));
                            }else if(var.getString("variableName").equals("qualityDamages")){
                                row.put("D", var.getString("value"));
                            }
                        }
                    }

                    Instant startTime=TimeUtils.parseTimeStamp(CAMUNDA_TS, activity.getString("startTime"));
                    Instant endTime=TimeUtils.parseTimeStamp(CAMUNDA_TS, activity.getString("endTime"));

                    double duration=Math.round((endTime.toEpochMilli()-startTime.toEpochMilli()) * 100.0) / 100.0;

                    row.put("QED", duration);

                   double avgTempBefore=avgTemp("container",startTime.toEpochMilli()-60000,startTime.toEpochMilli());
                   row.put("CTB", avgTempBefore);

                   double avgHumBefore=avgHum("container",startTime.toEpochMilli()-60000,startTime.toEpochMilli());
                   row.put("CHB", avgHumBefore);

                   double avgTempDuring=avgTemp("container",startTime.toEpochMilli(),endTime.toEpochMilli());
                   row.put("CTD", avgTempDuring);

                   double avgHumDuring=avgHum("container",startTime.toEpochMilli(),endTime.toEpochMilli());
                   row.put("CHD", avgHumDuring);

                    evaluationCompleted=endTime;
                }
                else if(activityId.indexOf("selectSampleMessage")>=0){
                    if(evaluationCompleted!=null){
                        Instant startTime=TimeUtils.parseTimeStamp(CAMUNDA_TS, activity.getString("startTime"));
                        double duration=Math.round((startTime.toEpochMilli()-evaluationCompleted.toEpochMilli()) * 100.0) / 100.0;
                        row.put("ST", duration);

                        double refrTemp=avgTemp("refrigerator",evaluationCompleted.toEpochMilli(),startTime.toEpochMilli());
                        row.put("RT", refrTemp);

                        double refrHum=avgHum("refrigerator",evaluationCompleted.toEpochMilli(),startTime.toEpochMilli());
                        row.put("RH", refrHum);
                       
                    }
                }else if(activityId.indexOf("bacteriaCondition")>=0){

                    String activityInstanceId=activity.getString("id");
        
                    JSONArray activityVars=getJSONActivityVars(activityInstanceId);

                    for(int i=0;i<activityVars.length();i++){
                        JSONObject var=activityVars.getJSONObject(i);
                        if(var.getString("type").equals("variableUpdate")){
                            row.put("B", var.getString("value"));
                        }
                    }            

                }
            }
            System.out.println(instance+","+row.get("PI")+","+row.get("QF")+","+row.get("QC")+","+row.get("D")+","+row.get("QED")+","+row.get("ST")+","+row.get("CTB")+","+row.get("CHB")+","+row.get("CTD")+","+row.get("CHD")+","+row.get("RT")+","+row.get("RH")+","+row.get("B"));
        }

        System.out.println(instances.keySet().size());
    }


    private JSONArray getJSONActivityVars(String activityInstanceId){
        try (InputStream is = App.class
                .getClassLoader()
                .getResourceAsStream("activities/"+activityInstanceId+".json")) {

            if (is == null) {
                throw new RuntimeException("Activity File not found in resources");
            }

            String jsonText = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JSONArray activityJson = new JSONArray(jsonText);

            return activityJson;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
       
    }

class TimeUtils {
    public static Instant parseTimeStamp(String pattern, String value){
        DateTimeFormatter fmt =
        DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);

        TemporalAccessor ta = fmt.parse(value);

        // If the timestamp contains an offset / zone → preserve it
        if (ta.isSupported(ChronoField.OFFSET_SECONDS)) {
            return OffsetDateTime.from(ta).toInstant();
        }

        // Otherwise, treat it as local time in Europe/Madrid
        LocalDateTime ldt = LocalDateTime.from(ta);
        return ldt.atZone(ZoneId.of("Europe/Madrid")).toInstant();
    }

}
