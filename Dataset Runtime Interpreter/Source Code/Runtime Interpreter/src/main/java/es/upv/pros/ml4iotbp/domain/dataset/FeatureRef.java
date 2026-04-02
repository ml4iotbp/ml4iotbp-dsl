package es.upv.pros.ml4iotbp.domain.dataset;

public class FeatureRef implements DataItem {
    private String alias;
    private String featureId;

    public FeatureRef() {}

    public FeatureRef(String alias, String featureId) {
        this.alias = alias;
        this.featureId = featureId;
    }

    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }

    public String getFeatureId() { return featureId; }
    public void setFeatureId(String featureId) { this.featureId = featureId; }
}
