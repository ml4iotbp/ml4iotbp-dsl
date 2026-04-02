package es.upv.pros.ml4iotbp.domain.features;

public class SimpleFeatureFrom implements FeatureFrom {
    private String source;

    public SimpleFeatureFrom() {}
    public SimpleFeatureFrom(String source) { this.source = source; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
}
