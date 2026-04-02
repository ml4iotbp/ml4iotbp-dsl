package es.upv.pros.ml4iotbp.connectors.process.camunda;

import java.net.URI;
import java.util.Objects;

public class CamundaConfig {
    private final URI baseUrl;   // p.ej. http://localhost:8080/engine-rest
    private final String processDefinitionKey; // process-id (key)
    private final String timeStampPattern;

    public CamundaConfig(URI baseUrl, String processDefinitionKey,String timeStampPattern) {
        this.baseUrl = Objects.requireNonNull(baseUrl);
        this.processDefinitionKey = Objects.requireNonNull(processDefinitionKey);
        this.timeStampPattern=timeStampPattern;
    }

    public URI getBaseUrl() { return baseUrl; }
    public String getProcessDefinitionKey() { return processDefinitionKey; }
    public String getTimeStampPattern() {return timeStampPattern;}
    
}