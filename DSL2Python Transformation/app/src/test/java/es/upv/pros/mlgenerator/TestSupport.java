package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class TestSupport {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  private TestSupport() {}

  public static JsonNode loadYaml(String resourcePath) throws IOException {
    try (InputStream is = TestSupport.class.getResourceAsStream(resourcePath)) {
      if (is == null) throw new IOException("Resource not found: " + resourcePath);
      return YAML.readTree(is);
    }
  }

  public static String loadText(String resourcePath) throws IOException {
    try (InputStream is = TestSupport.class.getResourceAsStream(resourcePath)) {
      if (is == null) throw new IOException("Resource not found: " + resourcePath);
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}