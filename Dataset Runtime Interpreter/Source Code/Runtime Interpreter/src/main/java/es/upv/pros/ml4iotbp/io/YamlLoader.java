package es.upv.pros.ml4iotbp.io;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.*;
import es.upv.pros.ml4iotbp.domain.ML4IoTBPDocument;

import java.io.InputStream;
import java.util.Set;

public class YamlLoader {

    private final ObjectMapper yamlMapper;
    private final ObjectMapper jsonMapper;

    public YamlLoader() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        this.jsonMapper = new ObjectMapper();
    }

    public ML4IoTBPDocument load(InputStream yaml) throws Exception {
        return yamlMapper.readValue(yaml, ML4IoTBPDocument.class);
    }

    public void validate(InputStream yaml, InputStream schemaJson) throws Exception {
        JsonNode yamlAsJson = yamlMapper.readTree(yaml);
        JsonNode schemaNode = jsonMapper.readTree(schemaJson);

        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        JsonSchema schema = factory.getSchema(schemaNode);

        Set<ValidationMessage> errors = schema.validate(yamlAsJson);
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder("YAML no válido según el JSON-Schema:\n");
            for (ValidationMessage e : errors) sb.append(" - ").append(e.getMessage()).append("\n");
            throw new IllegalArgumentException(sb.toString());
        }
    }
}