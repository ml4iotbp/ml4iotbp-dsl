package es.upv.pros.ml4iotbp.connectors.iot.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;

public class RabbitMQConnector implements AutoCloseable {

    private final Connection connection;

    public RabbitMQConnector(RabbitMQConfig cfg) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(cfg.getHost());
        if(cfg.getPort()!=null) factory.setPort(cfg.getPort());
        if(cfg.getUsername()!=null) factory.setUsername(cfg.getUsername());
        if(cfg.getPassword()!=null) factory.setPassword(cfg.getPassword());
        if(cfg.getVirtualHost()!=null) factory.setVirtualHost(cfg.getVirtualHost());
        // factory.setAutomaticRecoveryEnabled(true); // optional
        this.connection = factory.newConnection();
    }

    /**
     * Publica un mensaje usando:
     * - exchange: ds.getExchange() (si null/blank => default exchange "")
     * - routingKey: ds.getTopic() (si null => "")
     */
    public void publish(IoTDataSource ds, String message) throws IOException {
        Objects.requireNonNull(message, "message");
        try (Channel channel = connection.createChannel()) {
            String exchange = safe(ds.getExchange()); // "" = default exchange
            String routingKey = safe(ds.getTopic());

            // Si exchange no es el default, asegúrate de declararlo (aquí tipo topic por defecto)
            if (!exchange.isEmpty()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true);
            }

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2) // persistente
                    .build();

            channel.basicPublish(exchange, routingKey, props, message.getBytes(StandardCharsets.UTF_8));
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Consume de una cola.
     * Si tu DSL no define "queue", puedes:
     * - usar ds.getTopic() como nombre de cola
     * - o crear una cola anónima y bindearla al exchange
     *
     * @param ds IoTDataSource (puede ser config global o un sensor)
     * @param queueName cola a consumir (si null => se crea una cola temporal si hay exchange; si no, usa topic)
     * @param onMessage callback (routingKey, payload)
     */
    public String consume(String queueName,
                          String exchange,
                          String routingKey,
                          Consumer<String> dataProcessor) throws IOException {

       

        Channel channel = connection.createChannel(); // NO autoclose aquí: el consumer vive
        channel.basicQos(50);

        // 1) Determinar cola
        String actualQueue;
        if (queueName != null && !queueName.isBlank()) {
            actualQueue = queueName;
            channel.queueDeclare(actualQueue, true, false, false, null);
        } else if (!exchange.isEmpty()) {
            // cola temporal exclusiva si vamos por exchange
            actualQueue = channel.queueDeclare("", false, false, true, null).getQueue();
        } else if (!routingKey.isEmpty()) {
            // sin exchange, usamos topic como nombre de cola
            actualQueue = routingKey;
            channel.queueDeclare(actualQueue, true, false, false, null);
        } else {
            throw new IllegalArgumentException("Cannot infer queueName (no exchange and no topic).");
        }

        // 2) Bind si hay exchange
        if (!exchange.isEmpty()) {
            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true);
            // routingKey vacío => consume todo
            String bindingKey = routingKey.isEmpty() ? "#" : routingKey;
            channel.queueBind(actualQueue, exchange, bindingKey);
        }

        // 3) Consumer
        DeliverCallback deliver = (consumerTag, delivery) -> {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            dataProcessor.accept(payload);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        CancelCallback cancel = consumerTag -> { System.out.println("-->"+consumerTag); };

        channel.basicConsume(actualQueue, false, deliver, cancel);
        return actualQueue; // devuelvo la cola real usada
    }

    private static String safe(String s) {
        return (s == null) ? "" : s.trim();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
