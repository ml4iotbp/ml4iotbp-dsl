package es.upv.pros.mlgenerator;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

public class Main {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Uso: gradle run --args=\"<spec.yaml>\"   (opcional: <out.py>)");
      System.exit(1);
    }

    Path yamlPath = Path.of(args[0]);
    Path outPath = (args.length >= 2) ? Path.of(args[1]) : null;

    ObjectMapper jsonMapper = new ObjectMapper();
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    InputStream schemaStream = Main.class.getClassLoader().getResourceAsStream("ML4IoTBP-Grammar.json");

    // Leer schema completo y extraer sub-schema de models
    
    JsonNode fullSchema = jsonMapper.readTree(schemaStream);
    JsonNode modelsSchemaRoot = SchemaExtractor.extractModelsDefinitionAsRoot(fullSchema);

    // Leer YAML y tomar sección models
    JsonNode yamlRoot = yamlMapper.readTree(Files.readString(yamlPath, StandardCharsets.UTF_8));
    JsonNode modelsNode = yamlRoot.get("models");
    JsonNode datasetsNode = yamlRoot.get("dataset");

    if (modelsNode == null) {
      throw new IllegalArgumentException("El YAML no contiene la sección obligatoria: 'models'");
    }
    if (datasetsNode == null) {
      throw new IllegalArgumentException("El YAML no contiene la sección obligatoria: 'dataset'");
    }

    // Validar models contra el sub-schema
    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
    SchemaValidatorsConfig cfg = new SchemaValidatorsConfig();
    cfg.setFailFast(false);

    JsonSchema schema = factory.getSchema(modelsSchemaRoot, cfg);
    Set<ValidationMessage> errors = schema.validate(modelsNode);

    if (!errors.isEmpty()) {
      System.err.println("YAML inválido según #/definitions/models:");
      for (ValidationMessage e : errors) {
        System.err.println(" - " + e.getMessage());
      }
      System.exit(2);
    }

    // Generar Python
    //String python = PythonGenerator.generatePython(modelsNode);
    String python = PythonGenerator.generatePython(modelsNode, datasetsNode);

    if (outPath != null) {
      Files.writeString(outPath, python, StandardCharsets.UTF_8);
      System.out.println("Python generado en: " + outPath.toAbsolutePath());
    } else {
      System.out.println(python);
    }
  }
}
