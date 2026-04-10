package es.upv.pros.ml4iotbp.connectors.process.camunda;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import es.upv.pros.ml4iotbp.connectors.DataVar;
import es.upv.pros.ml4iotbp.connectors.process.ProcessEvent;
import es.upv.pros.ml4iotbp.domain.datasources.DataSource;
import es.upv.pros.ml4iotbp.domain.datasources.Variable;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessContext;
import es.upv.pros.ml4iotbp.domain.datasources.process.ProcessDataSource;
import es.upv.pros.ml4iotbp.domain.features.Feature;

public class CamundaConnector{

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final ScheduledExecutorService scheduler;
    private final ProcessContext processContext;

    private ScheduledFuture<?> task;

    // para no emitir duplicados
    private final Set<String> seenActivityInstanceIds = ConcurrentHashMap.newKeySet();

    // Elementos definidos en el DSL
    HashSet<String> definedElementIds = new HashSet<String>();

    private boolean showUri=true;

    public CamundaConnector(ProcessContext pc) {
        this.processContext=pc;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.mapper = new ObjectMapper();
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "camunda-poller");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Empieza a “escuchar” eventos del proceso definido en cfg, filtrando por elementos del ProcessDataSource.
     *
     * @param cfg config REST + process-id (key o id)
     * @param pds ProcessDataSource (con elements a observar)
     * @param pollingEvery periodo de polling
     * @param onEvent callback de eventos detectados
     * @param onError callback de errores
     */
    public void start(Map<String, ProcessDataSource> processDs,
                      Map<String,Map<String,List<Feature>>> anchoredFeatures,
                      Duration pollingEvery,
                      Consumer<ProcessEvent> onEvent,
                      Consumer<Exception> onError) {

        Objects.requireNonNull(processDs);
        Objects.requireNonNull(pollingEvery);
        Objects.requireNonNull(onEvent);
        Objects.requireNonNull(onError);

        stop();

        // elementos que queremos observar (ids BPMN)
        
        anchoredFeatures.forEach((element, events)->{
            definedElementIds.add(element);
        });

        if(!definedElementIds.contains(processContext.getStartEvent())) definedElementIds.add(processContext.getStartEvent());
        if(!definedElementIds.contains(processContext.getEndEvent())) definedElementIds.add(processContext.getEndEvent());
            
        Map<String, ProcessDataSource> pdsByElement= new Hashtable<String, ProcessDataSource>();
        processDs.forEach((name, pds)->{
            pdsByElement.put(pds.getElementId(), pds);
        });

        task = scheduler.scheduleAtFixedRate((Runnable) () -> {
            
            try{
                
                List<JsonNode> newOnes = fetchHistoryActivities();
            
                if(newOnes!=null){
                   //System.out.println("--->Accedo a Camunda: nuevos nodos " + newOnes.size());
                    for (JsonNode hai : newOnes) {

                        //System.out.println(hai);
                       
                        String activityId =  text(hai, "activityId");

                         // filtro por elementos declarados en el ProcessDataSource (si hay lista)
                        if (!definedElementIds.isEmpty() && !definedElementIds.contains(activityId)) {
                            continue;
                        }
     
                        if(activityId.equals(processContext.getStartEvent()) && anchoredFeatures.get(activityId)==null){
                            onEvent.accept(new ProcessEvent(
                                            DataSource.parseTimeStamp(processContext.getTimeStampPattern(),  text(hai, "startTime")).toEpochMilli(),
                                            "",
                                            processContext.getStartEvent(),
                                            ProcessEvent.START_EVENT,
                                            text(hai, "processInstanceId"),
                                            ProcessEvent.START_INSTANCE,
                                            new ArrayList<DataVar>()
                                    ));
                            continue;
                        }

                        if(activityId.equals(processContext.getEndEvent())){
                            onEvent.accept(new ProcessEvent(
                                            DataSource.parseTimeStamp(processContext.getTimeStampPattern(),  text(hai, "startTime")).toEpochMilli(),
                                            "",
                                            processContext.getEndEvent(),
                                            ProcessEvent.END_EVENT,
                                            text(hai, "processInstanceId"),
                                            ProcessEvent.END_INSTANCE,
                                            new ArrayList<DataVar>()
                                    ));
                            continue;
                        }
                        anchoredFeatures.get(activityId).forEach((event, features)->{
 
                            Instant eventTimeStamp=getEventTimeStamp(event,hai);
 
                            if(eventTimeStamp!=null){

                                /*if(event.equals("triggered")){
                                            System.out.println("E:"+eventTimeStamp);
                                            System.out.println(text(hai, "processInstanceId"));
                                };
                                if(activityId.equals("evaluateQualityTask")){
                                            System.out.println("A:"+eventTimeStamp);
                                            System.out.println(text(hai, "processInstanceId"));
                                };*/

                                ProcessDataSource pds=pdsByElement.get(activityId);
                            
                                List<DataVar> variables=new ArrayList<DataVar>();
                                pds.getEvents().forEach((eventDs, eventDef)->{
                                    if(eventDs.equals(event) || isPreviosEvent(eventDs,event)){
                                        for (Variable var : eventDef.getVariables()) {
                                            variables.add(this.createProcessVar(event, var, hai));
                                        }
                                    }
                                });

                            
                                onEvent.accept(new ProcessEvent(
                                                eventTimeStamp.toEpochMilli(),
                                                pds.getPdsName(),
                                                activityId,
                                                text(hai, "activityType"),
                                                text(hai, "processInstanceId"),
                                                event,
                                                variables
                                        ));
                            }
        
                        });
     
                    }
                }else{
                   //System.out.println("--->Accedo a Camunda: sin novedad");
                }
            } catch (Exception e) {
                System.out.println("--->Error en Thread principal");
                // Si el hilo ya está interrumpido, salimos silenciosamente
                if (Thread.currentThread().isInterrupted()) return;
                onError.accept(e);
            }
        },
        0L,
        pollingEvery.toMillis(),
        TimeUnit.MILLISECONDS
        );
    }

    private boolean isPreviosEvent(String eventDs, String event){

        if(eventDs.equals("started")){
            if(event.equals("completed")) return true;
        }

        return false;
    }

    private Instant getEventTimeStamp(String event, JsonNode hai){
        Instant timeStamp=null;
        switch(event){
            case "completed":
            case "cancelled":
            case "interrupted":
            case "triggered":
            case "executed":    String endTime=text(hai, "endTime");
                                if (endTime==null) return null;
                                return DataSource.parseTimeStamp(processContext.getTimeStampPattern(),  endTime);
            case "started":
            case "assigned":
            case "path-evaluation": return DataSource.parseTimeStamp(processContext.getTimeStampPattern(),  text(hai, "startTime"));
        }
        return timeStamp;
    }

    private DataVar createProcessVar(String event, Variable v, JsonNode hai){
        DataVar newVar=new DataVar();
        String varName=v.getVarName();
        if(event.equals("triggered") && varName.equals("startTime")) varName="endTime";
        newVar.setName(v.getInternalId()==null?varName:v.getInternalId());
        newVar.setType(v.getVarType());
        JsonNode userVars=null;

        try{ 
            switch (varName) {
                case "startTime": 
                case "endTime": 
                    Instant instant = parseCamundaTime(text(hai, varName));
                    newVar.setValue(instant.toEpochMilli());
                    break;
                case "selectedFlow": 
                    //userVars puede ser null o contener varias variables --> de ahi la estructura de Ifs
                    if(userVars==null) userVars=fetchUserVars(text(hai, "id"));
                    if(userVars!=null){
                        for (JsonNode n : userVars) {           
                            if(text(n, "type").equals("variableUpdate")){
                                newVar.setValue(text(n, "value"));
                            }
                        }
                    }
                default:
                    if(userVars==null) userVars=fetchUserVars(text(hai, "id"));
                    if(userVars!=null){
                        for (JsonNode n : userVars) {           
                            if(text(n, "type").equals("variableUpdate") && 
                                text(n, "variableName").equals(varName)){
                                newVar.setValue(text(n, "value"));
                            }
                        }
                    }
                    break;
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return newVar;
    }

    public void stop() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
    }

    // ------------------ REST: History Activity Instance ------------------

    private URI buildCamundaUri(String host, String startedAfter) {
    // host = "http://localhost:8080/engine-rest"
    String base = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;

    StringBuilder qs = new StringBuilder();
    qs.append("sortBy=").append(URLEncoder.encode("startTime", StandardCharsets.UTF_8));
    qs.append("&sortOrder=").append(URLEncoder.encode("asc", StandardCharsets.UTF_8));

    if (startedAfter != null && !startedAfter.isBlank()) {
        qs.append("&startedAfter=").append(URLEncoder.encode(startedAfter, StandardCharsets.UTF_8));
        // This will encode + as %2B automatically
    }

    return URI.create(base + "/history/activity-instance?" + qs);
}

    private List<JsonNode> fetchHistoryActivities() throws IOException{
        // Nota: si tu "process-id" es id y no key, usar processDefinitionId.
        String processDefinitionKey = url(processContext.getProcessId());

        URI uri = buildCamundaUri(processContext.getHost(), processContext.getInstanceTime());

        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .build();
      
        if(showUri){
            System.out.println(uri);
            showUri=false;
        }

        try{
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                throw new IllegalStateException("Camunda HTTP " + resp.statusCode() + " calling " + uri + " body=" + resp.body());
            }

            JsonNode arr = mapper.readTree(resp.body());
            if (!arr.isArray()) return List.of();

            List<JsonNode> out = new ArrayList<JsonNode>();

            for (JsonNode n : arr) {   
                String endTime=text(n, "startTime");

                if(text(n, "processDefinitionKey").equals(processDefinitionKey) && endTime!=null){
                    
                    //System.out.println(text(n, "startTime")+"-->"+activityTime.toEpochMilli());
                    
                    String id = text(n, "activityId");
                    String executionId = text(n, "id");
            
                    if (id == null) continue;
                    if (!seenActivityInstanceIds.add(executionId)) continue;
        
                    if(definedElementIds.contains(id)){
                        out.add(n);
                        
                    }

                }

            }
            return out;
        }catch(InterruptedException e){
                System.out.println("--->Conexion Historial interrumpida");
                // NO es error: es parada/cancel
                Thread.currentThread().interrupt();
                return null;
        }
        
    }

    //http://localhost:8080/engine-rest/history/detail?taskId=bf22159c-e72a-11f0-bc35-c6a7313a5f0f
    private JsonNode fetchUserVars(String activityInstanceId) throws IOException {
        URI uri = URI.create(processContext.getHost().toString()
                + "/history/detail?activityInstanceId="+activityInstanceId);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .build();

        try{
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                throw new IllegalStateException("Camunda HTTP " + resp.statusCode() + " calling " + uri + " body=" + resp.body());
            }

            String filePath = "output/"+activityInstanceId+".json";
            File dir = new File("output");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            FileWriter file = new FileWriter(filePath);
            file.write(resp.body()); // indentado bonito
            file.close();

            //System.out.println(resp.body());
            JsonNode arr = mapper.readTree(resp.body());
            if (!arr.isArray()) return null;
            return arr;
        }catch(InterruptedException e){
                // NO es error: es parada/cancel
                System.out.println("--->Conexion Variables interrumpida");
                Thread.currentThread().interrupt();
                return null;
        }

    }

    private static String url(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private Instant parseCamundaTime(String timeStamp) {

        return DataSource.parseTimeStamp(processContext.getTimeStampPattern(), timeStamp);

        //if (s == null || s.isBlank()) return null;
        // Camunda suele devolver ISO-8601 con zona, p.ej. 2020-01-01T10:00:00.000+0100
        // Instant.parse requiere Z o offset con colon; hacemos un parse “tolerante” simple:
       /*  try {
            return Instant.parse(s.replace("+0100", "+01:00").replace("+0200", "+02:00"));
        } catch (Exception ignore) {
            try {
                // si ya viene compatible
                return Instant.parse(s);
            } catch (Exception e) {
                return null;
            }
        }*/
    }

}

