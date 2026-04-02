package es.upv.pros.ml4iotbp.connectors.process;

import java.util.List;

import es.upv.pros.ml4iotbp.connectors.DataVar;

public class ProcessEvent {

    public static final String START_EVENT="StartEvent"; 
    public static final String END_EVENT="EndEvent"; 
    public static final String START_INSTANCE="StartInstance"; 
    public static final String END_INSTANCE="EndInstance";

    private final String pds;       // Process Data Source name
    private final String elementId;       // activityId
    private final String eventName;       // start|end (o tu mapeo)
    private final String elementType;    // userTask, serviceTask...
    private final String processInstanceId;
    private final List<DataVar> variables;
    private final Long timeStamp;

    public ProcessEvent(Long timeStamp,String pds, String elementId, String elementType,
                        String processInstanceId, String eventName, List<DataVar> variables) {
        this.timeStamp=timeStamp;
        this.pds=pds;
        this.elementId = elementId;
        this.eventName = eventName;
        this.elementType = elementType;
        this.processInstanceId = processInstanceId;
        this.variables=variables;
    }

    
    public String getPds() { return pds;}
    public String getElementId() { return elementId; }
    public String getEventName() { return eventName; }
    public String getElementType() { return elementType; }
    public String getProcessInstanceId() { return processInstanceId; }
    public List<DataVar> getVariables() { return variables; }
    public Long getTimeStamp() {return timeStamp;}

    @Override public String toString() {
        String text= "ProcessEvent{" +
                "timeStamp='" + timeStamp + '\'' +
                "pds='" + pds + '\'' +
                "elementId='" + elementId + '\'' +
                ", eventName='" + eventName + '\'' +
                ", activityType='" + elementType + '\'' +
                ", processInstanceId='" + processInstanceId + '\'';
        for(DataVar v: variables){
            text+=", "+ v.getName()+"=" + v.getValue()  + '\'';
        }
                
        text+=  '}';
        return text;
    }

    public String toJSON() {
        String jsonText= "{" +
                "\"timeStamp\":" + timeStamp +
                ",\"pds\":\"" + pds + '"' +
                ",\"elementId\":\"" + elementId + '"' +
                ", \"eventName=\":\"" + eventName + '"' +
                ", \"activityType\":\"" + elementType + '"' +
                ", \"processInstanceId\":\"" + processInstanceId + '"';

        for(DataVar v: variables){
            jsonText+=", \""+v.getName()+"\":\"" + v.getValue()  + '"';
        }

        jsonText+=  '}';

        return jsonText;
    }




}

