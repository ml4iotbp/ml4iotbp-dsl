package es.upv.pros.ml4iotbp.domain.dataset;

import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureMapping implements DataItem {

    private String featureId;

    /**
     * PI -> palletId
     * QF -> qualityFirmness
     */
    private Map<String, String> fields = new LinkedHashMap<>();

    public FeatureMapping() {}

    public FeatureMapping(String featureId, Map<String, String> fields) {
        this.featureId = featureId;
        this.fields = fields;
    }

    public String getFeatureId() { return featureId; }
    public void setFeatureId(String featureId) { this.featureId = featureId; }

    public Map<String, String> getFields() { return fields; }
    public void setFields(Map<String, String> fields) { this.fields = fields; }
}
