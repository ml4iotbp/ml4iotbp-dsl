package es.upv.pros.ml4iotbp.io;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import es.upv.pros.ml4iotbp.domain.features.CompositeFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.FeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.SimpleFeatureFrom;
import es.upv.pros.ml4iotbp.domain.features.SourceSelection;

public class FeatureFromDeserializer extends JsonDeserializer<FeatureFrom> {

    @Override
    public FeatureFrom deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        // from: quality_event_source
        if (node.isTextual()) {
            return new SimpleFeatureFrom(node.asText());
        }

        // from: { sourceA: {...}, sourceB: {...} }
        if (node.isObject()) {
            Map<String, SourceSelection> sources = new LinkedHashMap<>();
            node.fields().forEachRemaining(e -> {
                SourceSelection sel = mapper.convertValue(e.getValue(), SourceSelection.class);
                sources.put(e.getKey(), sel);
            });

            CompositeFeatureFrom cf = new CompositeFeatureFrom();
            cf.setSources(sources);
            return cf;
        }

        throw JsonMappingException.from(p, "Invalid Feature.from: expected string or object");
    }
}
