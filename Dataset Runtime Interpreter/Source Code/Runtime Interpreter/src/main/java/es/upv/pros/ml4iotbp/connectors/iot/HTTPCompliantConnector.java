package es.upv.pros.ml4iotbp.connectors.iot;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import es.upv.pros.ml4iotbp.domain.datasources.Variable;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;
import es.upv.pros.ml4iotbp.utils.DurationParser;

public class HTTPCompliantConnector extends IoTRepositoryManager{
    private final HttpClient client;
    private final ScheduledExecutorService scheduler;
    

    public HTTPCompliantConnector(IoTDataSource ds){
        initWriter();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Cerrando writer...");
            closeWriter();
        }));
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "http-poller");
            t.setDaemon(false);
            return t;
        });

        final String baseHost = ds.getHost();
        final String baseEndPoint = ds.getEndPoint();
        final String baseMethod = ds.getMethod();
        final String baseFormat = ds.getFormat();
        final String baseSamplingTime = ds.getSamplingTime();
        final String baseTimeStamp = ds.getTimeStamp();
        final List<Variable> baseSchema = ds.getSchema();
 
        if (!ds.getSensors().isEmpty()) { // With config

            ds.getSensors().forEach((sensorName, sensor) -> {
                String effId = sensorName;
                String effHost = sensor.getHost() != null ? sensor.getHost() : baseHost;
                String effEndPoint = sensor.getEndPoint() != null ? sensor.getEndPoint() : baseEndPoint;
                String effMethod = sensor.getMethod() != null ? sensor.getMethod() : baseMethod;
                String effFormat = sensor.getFormat() != null ? sensor.getFormat() : baseFormat;
                String effSampling = sensor.getSamplingTime() != null ? sensor.getSamplingTime() : baseSamplingTime;
                String effTs = sensor.getTimeStamp() != null ? sensor.getTimeStamp() : baseTimeStamp;
                List<Variable> effSchema = sensor.getSchema() != null ? sensor.getSchema() : baseSchema;

                connect(effId, effHost, effEndPoint, effMethod, effFormat, effSampling, effTs, effSchema);

            });
        }else{
    
             connect(ds.getId(), baseHost, baseEndPoint, baseMethod, baseFormat, baseSamplingTime, baseTimeStamp, baseSchema);
        }
    }

    private BufferedWriter writer;

    public void initWriter() {
        try {
             writer = new BufferedWriter(new FileWriter("iot_data_http.json", true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void closeWriter() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

     private void connect(String dataSourceId,
                     String host,
                     String endPoint,
                     String method,
                     String format,
                     String samplingTime,
                     String timeStampPattern,
                     List<Variable> schema){
        
        Duration period = DurationParser.parseTime(samplingTime);
        URI uri = buildUri(host, endPoint);
       

        scheduler.scheduleAtFixedRate((Runnable) () ->   {

            try {
               HttpRequest request = buildRequest(method, uri, format);
               HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
               if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                    //System.out.println(resp.body());
                    synchronized (writer) {
                        writer.write(resp.body());
                        writer.newLine();  
                        writer.flush();
                    }
                    this.log(dataSourceId, schema, timeStampPattern, format, resp.body());
                } else {
                    throw new IOException("HTTP " + resp.statusCode() + " calling " + uri + " body=" + resp.body());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0L, period.toMillis(), TimeUnit.MILLISECONDS);
    }

    private String get(String url) throws IOException {
        HttpURLConnection con = null;
        try {
            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(5000);
            con.setReadTimeout(10000);
            con.setRequestProperty("Accept", "application/json");
            con.setRequestProperty("User-Agent", "JavaTest/1.0");

            int status = con.getResponseCode();
            System.out.println("HTTP status = " + status);

            InputStream stream = (status >= 200 && status < 300)
                    ? con.getInputStream()
                    : con.getErrorStream();

            if (stream == null) {
                throw new IOException("No response body. HTTP status = " + status);
            }

            try (BufferedReader in = new BufferedReader(new InputStreamReader(stream))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    response.append(line).append("\n");
                }
                return response.toString();
            }
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }


    private URI buildUri(String host, String endPoint) {
        endPoint = endPoint.trim();

        if(host!=null){
            host = host.trim();
            // Default scheme si no viene
            if (!host.startsWith("http://") && !host.startsWith("https://")) {
                host = "http://" + host;
            }

            if (!endPoint.startsWith("/")) endPoint = "/" + endPoint;

            // Evita doble /
            if (host.endsWith("/")) host = host.substring(0, host.length() - 1);

            return URI.create(host + endPoint);
        }else{
            return URI.create(endPoint);
        }
    }

    private static HttpRequest buildRequest(String method, URI uri, String format) {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .header("Accept", format);

        // Por defecto: GET sin body; POST sin body.
        return switch (method) {
            case IoTDataSource.GET -> b.GET().build();
            case IoTDataSource.POST -> b.POST(HttpRequest.BodyPublishers.noBody())
                          .header("Content-Type", format).build();
            case IoTDataSource.PUT -> b.PUT(HttpRequest.BodyPublishers.noBody())
                         .header("Content-Type", format).build();
            case IoTDataSource.DELETE -> b.DELETE().build();
            default -> throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        };
    }

}
