package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GeneratorGoldenTest {

  @Test
  void classificationGoldenStructure() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/classification_rf.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    assertAll(
        () -> assertTrue(py.contains("from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor")),
        () -> assertTrue(py.contains("def train_one(")),
        () -> assertTrue(py.contains("def evaluate_one(")),
        () -> assertTrue(py.contains("def dataset_specs()")),
        () -> assertTrue(py.contains("def create_rest_app(")),
        () -> assertTrue(py.contains("def save_artifact(")),
        () -> assertTrue(py.contains("def predict_from_csv("))
    );
  }

  @Test
  void periodicScheduleGoldenStructure() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/classification_rf.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    assertTrue(py.contains("BackgroundScheduler"));
    assertTrue(py.contains("parse_duration_seconds"));
    assertTrue(py.contains("scheduler.add_job"));
  }
}