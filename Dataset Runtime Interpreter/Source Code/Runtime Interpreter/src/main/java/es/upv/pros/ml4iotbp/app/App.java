package es.upv.pros.ml4iotbp.app;

import java.io.InputStream;

import es.upv.pros.ml4iotbp.connectors.iot.IoTDataSourceConnector;
import es.upv.pros.ml4iotbp.connectors.process.camunda.ProcessDataSourceConnector;
import es.upv.pros.ml4iotbp.datasets.DatasetConstructor;
import es.upv.pros.ml4iotbp.domain.ML4IoTBPDocument;
import es.upv.pros.ml4iotbp.io.YamlLoader;

public class App {

    private static String specification="drugbatches_test3.ml4iotbp.dsl.yaml";
    
     public static void main(String[] args) {
        try {
         
            InputStream yamlStream = App.class.getClassLoader().getResourceAsStream(specification);
            InputStream schemaStream = App.class.getClassLoader().getResourceAsStream("ML4IoTBP-Grammar.json");

            if (yamlStream == null || schemaStream == null) {
                throw new IllegalStateException("No se encontraron los recursos YAML o JSON-Schema");
            }

            YamlLoader loader = new YamlLoader();

            // 1) Validación contra JSON-Schema
            System.out.println("🔍 Validating YAML against JSON-Schema...");
            loader.validate(yamlStream, schemaStream);
            System.out.println("✅ Validation OK\n");

            // 2) Parseo YAML -> POJOs
            yamlStream = App.class.getClassLoader().getResourceAsStream(specification);
            ML4IoTBPDocument doc = loader.load(yamlStream);
            System.out.println("📄 YAML parsed successfully\n");

       
            // 3A) Inspección de IoT Data Sources
            if (doc.getIotDataSources() != null) {
                doc.getIotDataSources().forEach((id, ds) -> {
                    ds.setId(id);
                });
            }
            //Logger.showIoTDataSources(doc);
            new IoTDataSourceConnector().connect(doc.getIotDataSources());

            // 3B) Inspección de Process Data Sources
            if (doc.getProcessDataSources() != null) {
                doc.getProcessDataSources().forEach((name, pds) -> {
                        pds.setPdsName(name);
                });
            }
            //Logger.showProcessDataSources(doc);
            DatasetConstructor datasetConstructor=new DatasetConstructor(doc);
            new ProcessDataSourceConnector().connect(doc.getProcessContext(),doc.getProcessDataSources(),datasetConstructor);

            

            // 4) Features
            //Logger.showFeatures(doc);

            // 5) Datasets
            //Logger.showDataSets(doc);

            // 6) Models
            //Logger.showMLModels(doc);

            // 7) Runtime Predictions
            //Logger.showRuntimePredictions(doc);


        } catch (Exception e) {
            System.err.println("❌ Error while parsing YAML:");
            e.printStackTrace();
        }
    }
}
