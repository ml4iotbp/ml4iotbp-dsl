package es.upv.pros.ml4iotbp.domain.datasources.process;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessDataSource {

    @JsonIgnore
    private String pds_name;

    @JsonIgnore
    private String id;

    @JsonProperty("element-id")
    private String elementId;

    @JsonProperty("type")
    private String elementType;   // User Task | Message | Exclusive Gateway

   
   

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getElementType() {
        return elementType;
    }

    public void setElementType(String elementType) {
        this.elementType = elementType;
    }

    /*private List<Variable> variables;

    public List<Variable> getVariables() {
        return variables;
    }

    public void setVariables(List<Variable> variables) {
        this.variables = variables;
    }*/

    public String getPdsName() {
        return pds_name;
    }

    public void setPdsName(String pds_name) {
        this.pds_name = pds_name;
    }

    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    


    private Map<String, ProcessEventDefinition> events;
 
    public Map<String, ProcessEventDefinition> getEvents() {
        return events;
    }

    public void setEvents(Map<String, ProcessEventDefinition> events) {
        this.events = events;
    }

}

