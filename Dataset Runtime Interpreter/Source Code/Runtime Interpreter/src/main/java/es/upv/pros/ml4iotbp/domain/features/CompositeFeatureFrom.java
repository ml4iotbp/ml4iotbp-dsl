package es.upv.pros.ml4iotbp.domain.features;

import java.util.Map;

public class CompositeFeatureFrom implements FeatureFrom {

    // key = nombre de la fuente (quality_event_source, sample_event_source...)
    // value = selección de campos (field / fields)
    private Map<String, SourceSelection> sources;

    public Map<String, SourceSelection> getSources() { return sources; }
    public void setSources(Map<String, SourceSelection> sources) { this.sources = sources; }
}
