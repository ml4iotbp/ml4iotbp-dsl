package es.upv.pros.ml4iotbp.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import es.upv.pros.ml4iotbp.domain.dataset.DataItem;
import es.upv.pros.ml4iotbp.domain.dataset.FeatureMapping;
import es.upv.pros.ml4iotbp.domain.dataset.FeatureRef;

public class DataItemDeserializer extends JsonDeserializer<DataItem> {

    @Override
    public DataItem deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        // Cada item de la lista debe ser un objeto con 1 sola entrada: {KEY: VALUE}
        if (!node.isObject()) {
            throw MismatchedInputException.from(p, DataItem.class, "Each dataset.data item must be an object");
        }

        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        if (!it.hasNext()) {
            throw MismatchedInputException.from(p, DataItem.class, "Empty object in dataset.data item");
        }

        Map.Entry<String, JsonNode> entry = it.next();
        String key = entry.getKey();
        JsonNode value = entry.getValue();

        // Caso A: - QED: f_quality_eval_duration
        if (value.isTextual()) {
            return new FeatureRef(key, value.asText());
        }

        // Caso B: - f_quality_results: [ {PI: palletId}, {QF: qualityFirmness}, ... ]
        if (value.isArray()) {
            Map<String, String> mapping = new LinkedHashMap<>();
            for (JsonNode m : value) {
                if (!m.isObject()) {
                    throw MismatchedInputException.from(p, DataItem.class,
                            "Expected objects inside field mapping array for " + key);
                }
                m.fields().forEachRemaining(e -> mapping.put(e.getKey(), e.getValue().asText()));
            }
            return new FeatureMapping(key, mapping);
        }

        throw MismatchedInputException.from(p, DataItem.class,
                "Invalid dataset.data item for key '" + key + "': expected string or array");
    }
}

