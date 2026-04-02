package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaExtractor {

  /**
   * Extrae el sub-schema #/definitions/models y lo convierte en un schema raíz draft-07.
   */
  public static JsonNode extractModelsDefinitionAsRoot(JsonNode fullSchema) {
    JsonNode models = fullSchema.path("definitions").path("models");
    if (models.isMissingNode() || models.isNull()) {
      throw new IllegalArgumentException("El schema no contiene '#/definitions/models'.");
    }
    if (!(models instanceof ObjectNode)) {
      throw new IllegalArgumentException("'#/definitions/models' no es un objeto JSON.");
    }

    ObjectNode root = (ObjectNode) models.deepCopy();

    // Asegura que el validador lo trate como schema raíz draft-07
    // (No pasa nada si el schema completo original ya tenía $schema).
    if (!root.has("$schema")) {
      root.put("$schema", "http://json-schema.org/draft-07/schema#");
    }

    return root;
  }
}
