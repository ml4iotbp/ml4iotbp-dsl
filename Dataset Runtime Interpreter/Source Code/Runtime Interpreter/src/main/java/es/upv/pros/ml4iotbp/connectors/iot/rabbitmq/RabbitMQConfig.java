package es.upv.pros.ml4iotbp.connectors.iot.rabbitmq;

import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;

public class RabbitMQConfig {
    private String host;
    private Integer port;
    private String username;
    private String password;
    private String virtualHost;

    public RabbitMQConfig(){}

    public RabbitMQConfig(String host, Integer port, String username, String password, String virtualHost) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
    }

    public String getHost() { return host; }
    public Integer getPort() { return port; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getVirtualHost() { return virtualHost; }

    public void setHost(String host) { this.host=host; }
    public void setPort(Integer port) { this.port=port; }
    public void setUsername(String username) { this.username = username; }
    public void setPassword(String password) { this.password = password; }
    public void setVirtualHost(String virtualHost) { this.virtualHost = virtualHost; }

    /**
     * Crea config con defaults si el YAML no define credenciales.
     * Si en tu DSL guardas usuario/clave en otro sitio, adapta aquí.
     */
    public static RabbitMQConfig from(IoTDataSource ds) {
        String host = ds.getHost() != null ? ds.getHost() : "localhost";
        int port = 5672; // default AMQP

        // En tu DSL no aparecen usuario/clave; pongo defaults típicos.
        String user = "guest";
        String pass = "guest";
        String vhost = "/";

        return new RabbitMQConfig(host, port, user, pass, vhost);
    }
}