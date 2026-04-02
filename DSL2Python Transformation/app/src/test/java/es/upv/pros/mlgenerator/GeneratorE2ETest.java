package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GeneratorE2ETest {

  private boolean pythonAvailable() {
    try {
      Process p = new ProcessBuilder("python3", "--version").start();
      return p.waitFor() == 0;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  void generatedPythonCompiles() throws Exception {
    Assumptions.assumeTrue(pythonAvailable(), "python3 not available");

    JsonNode yaml = TestSupport.loadYaml("/specs/classification_rf.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    Path dir = Files.createTempDirectory("mlgen-test-");
    Path script = dir.resolve("generated.py");
    Files.writeString(script, py);

    Process p = new ProcessBuilder("python3", "-m", "py_compile", script.toString())
        .redirectErrorStream(true)
        .start();

    String output = new String(p.getInputStream().readAllBytes());
    int code = p.waitFor();

    assertEquals(0, code, output);
  }

  @Test
  void generatedPythonTrainCommandRuns() throws Exception {
    Assumptions.assumeTrue(pythonAvailable(), "python3 not available");

    JsonNode yaml = TestSupport.loadYaml("/specs/classification_rf.yaml");
    String py = PythonGenerator.generatePython(yaml.get("models"), yaml.get("dataset"));

    Path dir = Files.createTempDirectory("mlgen-train-");
    Path script = dir.resolve("generated.py");
    Path outDir = dir.resolve("artifacts");
    Path csv = copyResourceToTemp("/data/classification.csv", dir.resolve("classification.csv"));

    Files.writeString(script, py);

    Process p = new ProcessBuilder(
        List.of(
            "python3",
            script.toString(),
            "train",
            "--out-dir", outDir.toString(),
            "--dataset", csv.toString()
        )
    ).redirectErrorStream(true).start();

    String output = new String(p.getInputStream().readAllBytes());
    int code = p.waitFor();

    assertEquals(0, code, output);
    assertTrue(Files.exists(outDir.resolve("churn_model.joblib")), output);
  }

  private Path copyResourceToTemp(String resource, Path target) throws IOException {
    Files.writeString(target, TestSupport.loadText(resource));
    return target;
  }
}