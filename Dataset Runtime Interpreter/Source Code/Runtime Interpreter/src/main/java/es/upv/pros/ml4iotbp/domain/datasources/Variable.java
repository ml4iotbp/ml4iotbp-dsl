package es.upv.pros.ml4iotbp.domain.datasources;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Variable {
    @JsonProperty("internal-id")
    private String internalId;

    /** { <varName>: <varType> } (propiedades dinámicas) */
    private Map<String, String> typeByName = new LinkedHashMap<>();

    @com.fasterxml.jackson.annotation.JsonAnySetter
    public void addVar(String k, Object v) {
      if (!"internal-id".equals(k) && v != null) typeByName.put(k, String.valueOf(v));
    }

    public String getInternalId() { return internalId; }
    public void setInternalId(String internalId) { this.internalId = internalId; }

    public Map<String, String> getTypeByName() { return typeByName; }
    public void setTypeByName(Map<String, String> typeByName) { this.typeByName = typeByName; }

    public String getVarName(){
      return typeByName.entrySet().iterator().next().getKey();
    }

    public String getVarType(){
      return typeByName.entrySet().iterator().next().getValue();
    }

    @JsonIgnore
    private Object value;
    public Object getValue() {
        return value;
    }
    public void setValue(Object value) {
        this.value = value;
    }
    
  }
