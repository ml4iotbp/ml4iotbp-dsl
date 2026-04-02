package es.upv.pros.ml4iotbp.domain.features;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SourceSelection {

    private String field;
    private List<String> fields;

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public List<String> getFields() { return fields; }
    public void setFields(List<String> fields) { this.fields = fields; }

    @JsonIgnore
    public List<String> asList() {
        if (fields != null && !fields.isEmpty()) return fields;
        if (field != null && !field.isBlank()) return List.of(field);
        return List.of();
    }
}
