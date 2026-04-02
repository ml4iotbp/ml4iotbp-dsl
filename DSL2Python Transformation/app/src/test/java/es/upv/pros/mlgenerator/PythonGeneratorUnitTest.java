package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PythonGeneratorUnitTest {

  @Test
  void generatesPythonForClassificationModel() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/classification_rf.yaml");
    JsonNode models = yaml.get("models");
    JsonNode datasets = yaml.get("dataset");

    String py = PythonGenerator.generatePython(models, datasets);

    assertNotNull(py);
    assertTrue(py.contains("def build_models()"));
    assertTrue(py.contains("RandomForestClassifier"));
    assertTrue(py.contains("accuracy_score"));
    assertTrue(py.contains("roc_auc_score"));
    assertTrue(py.contains("def dataset_specs()"));
    assertTrue(py.contains("'train_ratio': 0.6"));
    assertTrue(py.contains("'val_ratio': 0.2"));
    assertTrue(py.contains("'test_ratio': 0.2"));
  }

  @Test
  void generatesPythonForRegressionModel() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/regression_svm.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    assertTrue(py.contains("SVR"));
    assertTrue(py.contains("mean_absolute_error"));
    assertTrue(py.contains("mean_squared_error"));
    assertTrue(py.contains("np.sqrt(mean_squared_error"));
  }

  @Test
  void generatesPythonForClusteringModel() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/clustering_dbscan.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    assertTrue(py.contains("DBSCAN"));
    assertTrue(py.contains("silhouette_score"));
    assertTrue(py.contains("davies_bouldin_score"));
    assertTrue(py.contains("calinski_harabasz_score"));
  }

  @Test
  void eventBasedScheduleUsesEventsProperty() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/event_based.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    assertTrue(py.contains("Event-Based") || py.contains("EventBased") || py.contains("EventBased for"));
  }

  @Test
  void invalidMetricCompatibilityFails() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/invalid_metric.yaml");

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"))
    );

    assertTrue(ex.getMessage().contains("no compatible"));
  }

  @Test
  void invalidRatiosFail() throws Exception {
    JsonNode yaml = TestSupport.loadYaml("/specs/invalid_ratios.yaml");

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"))
    );

    assertTrue(ex.getMessage().contains("deben sumar 1.0"));
  }
}