//268 líneas de código efectivo.
package logistics2.dataset;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Generates a logistics dataset by combining Camunda BPM process history
 * with IoT sensor readings (HTTP and CSV sources).
 */
public class LogisticsDataset {

    // --- Configuration constants ---
    private static final String TARGET_PROCESS_KEY    = "DistributionCenterTest";
    private static final String CAMUNDA_DATE_PATTERN  = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    // --- In-memory sensor buffers: each entry maps a single epoch-ms timestamp -> reading ---
    private final List<Map<Long, Float>> containerTemperatureReadings = new ArrayList<>();
    private final List<Map<Long, Float>> containerHumidityReadings    = new ArrayList<>();
    private final List<Map<Long, Float>> fridgeTemperatureReadings    = new ArrayList<>();
    private final List<Map<Long, Float>> fridgeHumidityReadings       = new ArrayList<>();

    // -----------------------------------------------------------------------------------------
    // Entry point
    // -----------------------------------------------------------------------------------------

    public static void main(String[] args) {
        new LogisticsDataset().run();
    }

    public void run() {
        try {
            JSONArray camundaEvents = loadJsonArray("logistics2/camunda.json",       "Camunda log");
            JSONArray httpSensorData = loadJsonArray("logistics2/iot_data_http.json", "IoT HTTP log");

            String rawTempCsv = loadResourceAsString("logistics2/temperature.csv", "Temperature CSV");
            String rawHumCsv  = loadResourceAsString("logistics2/humidity.csv",    "Humidity CSV");

            processHttpSensorData(httpSensorData);
            parseTemperatureCsv(rawTempCsv);
            parseHumidityCsv(rawHumCsv);

            Map<String, List<JSONObject>> groupedInstances = groupByProcessInstance(camundaEvents);
            buildAndPrintReport(groupedInstances);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // -----------------------------------------------------------------------------------------
    // Resource loading helpers
    // -----------------------------------------------------------------------------------------

    private JSONArray loadJsonArray(String resourcePath, String label) throws IOException {
        InputStream stream = getResourceStream(resourcePath, label);
        String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        return new JSONArray(content);
    }

    private String loadResourceAsString(String resourcePath, String label) throws IOException {
        InputStream stream = getResourceStream(resourcePath, label);
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private InputStream getResourceStream(String path, String label) {
        InputStream is = LogisticsDatasetGenerator2.class.getClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new RuntimeException("Resource not found [" + label + "]: " + path);
        }
        return is;
    }

    // -----------------------------------------------------------------------------------------
    // IoT data parsing
    // -----------------------------------------------------------------------------------------

    /** Reads HTTP-sourced IoT JSON, buffers readings, and writes processedHTTPIoT.csv. */
    private void processHttpSensorData(JSONArray records) throws IOException {
        try (FileWriter out = new FileWriter("processedHTTPIoT.csv")) {
            for (int i = 0; i < records.length(); i++) {
                JSONObject entry = records.getJSONObject(i);
                long epochMs = TimestampParser.toEpochMs("dd/MM/yyyy HH:mm:ss", entry.getString("timeStamp"));
                String sensorId = entry.getString("id");
                String csvRow;

                if ("sn-hum-538474".equals(sensorId)) {
                    float reading = entry.getFloat("humidity");
                    fridgeHumidityReadings.add(singletonMap(epochMs, reading));
                    csvRow = epochMs + ",refrigerator_humidity_sensor," + reading + "\n";

                } else if ("sn-tmp-142037".equals(sensorId)) {
                    float reading = entry.getFloat("temperature");
                    fridgeTemperatureReadings.add(singletonMap(epochMs, reading));
                    csvRow = epochMs + ",refrigerator_temperature_sensor," + reading + "\n";

                } else {
                    csvRow = epochMs + ",unknown_sensor\n";
                }

                out.write(csvRow);
                out.flush();
            }
        }
    }

    /** Reads the container temperature CSV (skips header), buffers readings, writes processedTempCSV.csv. */
    private void parseTemperatureCsv(String csvContent) throws IOException {
        String[] lines = csvContent.split("\n");
        try (FileWriter out = new FileWriter("processedTempCSV.csv")) {
            for (int i = 1; i < lines.length; i++) {
                String[] fields = lines[i].split(",");
                long epochMs     = TimestampParser.toEpochMs("d MMM uuuu HH:mm:ss z", fields[3]);
                float value      = Float.parseFloat(fields[2]);
                containerTemperatureReadings.add(singletonMap(epochMs, value));
                out.write(epochMs + "," + value + "\n");
                out.flush();
            }
        }
    }

    /** Reads the container humidity CSV (skips header), buffers readings, writes processedHumCSV.csv. */
    private void parseHumidityCsv(String csvContent) throws IOException {
        String[] lines = csvContent.split("\n");
        try (FileWriter out = new FileWriter("processedHumCSV.csv")) {
            for (int i = 1; i < lines.length; i++) {
                String[] fields = lines[i].split(",");
                long epochMs     = TimestampParser.toEpochMs("d MMM uuuu HH:mm:ss z", fields[3]);
                float value      = Float.parseFloat(fields[2]);
                containerHumidityReadings.add(singletonMap(epochMs, value));
                out.write(epochMs + "," + value + "\n");
                out.flush();
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Camunda grouping
    // -----------------------------------------------------------------------------------------

    /**
     * Groups Camunda history events by process instance ID,
     * keeping only events that belong to the target process definition.
     */
    private Map<String, List<JSONObject>> groupByProcessInstance(JSONArray events) {
        Map<String, List<JSONObject>> grouped = new HashMap<>();
        String lastInstanceId = "";

        for (int i = 0; i < events.length(); i++) {
            JSONObject event = events.getJSONObject(i);

            if (!TARGET_PROCESS_KEY.equals(event.getString("processDefinitionKey"))) {
                continue;
            }

            String instanceId = event.getString("processInstanceId");

            if (!instanceId.equals(lastInstanceId)) {
                lastInstanceId = instanceId;
                grouped.put(instanceId, new ArrayList<>());
            }
            grouped.get(instanceId).add(event);
        }
        return grouped;
    }

    // -----------------------------------------------------------------------------------------
    // Report generation
    // -----------------------------------------------------------------------------------------

    private void buildAndPrintReport(Map<String, List<JSONObject>> instances) {
        System.out.println("INST,PI,QF,QC,D,QED,ST,CTB,CHB,CTD,CHD,RT,RH,B");

        for (String instanceId : instances.keySet()) {
            Map<String, Object> record = new HashMap<>();
            Instant qualityCheckEnd = null;

            for (JSONObject activity : instances.get(instanceId)) {
                String activityId = activity.getString("activityId");

                if (activityId.contains("evaluateQualityTask")) {
                    qualityCheckEnd = handleQualityEvaluation(activity, record);

                } else if (activityId.contains("selectSampleMessage") && qualityCheckEnd != null) {
                    handleSampleSelection(activity, qualityCheckEnd, record);

                } else if (activityId.contains("bacteriaCondition")) {
                    handleBacteriaCondition(activity, record);
                }
            }

            printRecord(instanceId, record);
        }

        System.out.println(instances.size());
    }

    private Instant handleQualityEvaluation(JSONObject activity, Map<String, Object> record) {
        String activityInstanceId = activity.getString("id");
        JSONArray vars = loadActivityVariables(activityInstanceId);

        for (int i = 0; i < vars.length(); i++) {
            JSONObject var = vars.getJSONObject(i);
            if (!"variableUpdate".equals(var.getString("type"))) continue;

            switch (var.getString("variableName")) {
                case "palletId"        -> record.put("PI", var.getString("value"));
                case "qualityFirmness" -> record.put("QF", var.getString("value"));
                case "qualityColor"    -> record.put("QC", var.getString("value"));
                case "qualityDamages"  -> record.put("D",  var.getString("value"));
            }
        }

        Instant startTs = TimestampParser.toInstant(CAMUNDA_DATE_PATTERN, activity.getString("startTime"));
        Instant endTs   = TimestampParser.toInstant(CAMUNDA_DATE_PATTERN, activity.getString("endTime"));
        long durationMs = endTs.toEpochMilli() - startTs.toEpochMilli();

        record.put("QED", roundTwo(durationMs));

        long evalStart = startTs.toEpochMilli();
        long evalEnd   = endTs.toEpochMilli();
        long oneMinute = 60_000L;

        record.put("CTB", avgTemperature("container",  evalStart - oneMinute, evalStart));
        record.put("CHB", avgHumidity("container",     evalStart - oneMinute, evalStart));
        record.put("CTD", avgTemperature("container",  evalStart, evalEnd));
        record.put("CHD", avgHumidity("container",     evalStart, evalEnd));

        return endTs;
    }

    private void handleSampleSelection(JSONObject activity, Instant qualityCheckEnd, Map<String, Object> record) {
        Instant messageTs  = TimestampParser.toInstant(CAMUNDA_DATE_PATTERN, activity.getString("endTime"));
        long waitMs        = messageTs.toEpochMilli() - qualityCheckEnd.toEpochMilli();

        record.put("ST", roundTwo(waitMs));
        record.put("RT", avgTemperature("refrigerator", qualityCheckEnd.toEpochMilli(), messageTs.toEpochMilli()));
        record.put("RH", avgHumidity("refrigerator",    qualityCheckEnd.toEpochMilli(), messageTs.toEpochMilli()));
    }

    private void handleBacteriaCondition(JSONObject activity, Map<String, Object> record) {
        String activityInstanceId = activity.getString("id");
        JSONArray vars = loadActivityVariables(activityInstanceId);

        for (int i = 0; i < vars.length(); i++) {
            JSONObject var = vars.getJSONObject(i);
            if ("variableUpdate".equals(var.getString("type"))) {
                record.put("B", var.getString("value"));
            }
        }
    }

    private void printRecord(String instanceId, Map<String, Object> rec) {
        System.out.println(
            instanceId    + "," + rec.get("PI")  + "," + rec.get("QF")  + "," +
            rec.get("QC") + "," + rec.get("D")   + "," + rec.get("QED") + "," +
            rec.get("ST") + "," + rec.get("CTB") + "," + rec.get("CHB") + "," +
            rec.get("CTD")+ "," + rec.get("CHD") + "," + rec.get("RT")  + "," +
            rec.get("RH") + "," + rec.get("B")
        );
    }

    // -----------------------------------------------------------------------------------------
    // Sensor average calculations
    // -----------------------------------------------------------------------------------------

    /**
     * Returns the rounded average temperature for the given source and time window.
     * If no readings fall within [from, to], the window expands backwards by its own length.
     */
    private double avgTemperature(String source, long from, long to) {
        List<Map<Long, Float>> dataset = "container".equals(source)
            ? containerTemperatureReadings
            : fridgeTemperatureReadings;
        return computeAverage(dataset, from, to);
    }

    /**
     * Returns the rounded average humidity for the given source and time window.
     * If no readings fall within [from, to], the window expands backwards by its own length.
     */
    private double avgHumidity(String source, long from, long to) {
        List<Map<Long, Float>> dataset = "container".equals(source)
            ? containerHumidityReadings
            : fridgeHumidityReadings;
        return computeAverage(dataset, from, to);
    }

    private double computeAverage(List<Map<Long, Float>> readings, long from, long to) {
        long windowSize = to - from;
        float total = 0f;
        int count   = 0;

        while (count == 0) {
            for (Map<Long, Float> entry : readings) {
                long ts = entry.keySet().iterator().next();
                if (ts >= from && ts <= to) {
                    total += entry.get(ts);
                    count++;
                }
            }
            from -= windowSize;
        }

        return roundTwo(total / count);
    }

    // -----------------------------------------------------------------------------------------
    // Activity variables loader
    // -----------------------------------------------------------------------------------------

    private JSONArray loadActivityVariables(String activityInstanceId) {
        String path = "logistics2/activities/" + activityInstanceId + ".json";
        try (InputStream is = LogisticsDatasetGenerator2.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new RuntimeException("Activity file not found: " + path);
            }
            return new JSONArray(new String(is.readAllBytes(), StandardCharsets.UTF_8));
        } catch (Exception ex) {
            ex.printStackTrace();
            return new JSONArray();
        }
    }

    // -----------------------------------------------------------------------------------------
    // Micro-utilities
    // -----------------------------------------------------------------------------------------

    /** Creates a single-entry map without exposing the HashMap constructor noise inline. */
    private static Map<Long, Float> singletonMap(long key, float value) {
        Map<Long, Float> m = new HashMap<>(2);
        m.put(key, value);
        return m;
    }

    private static double roundTwo(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}


// =============================================================================
// Timestamp parsing utility
// =============================================================================

class TimestampParser {

    private TimestampParser() {}

    /**
     * Parses a formatted date-time string into an {@link Instant}.
     * If the pattern contains timezone offset information, it is preserved;
     * otherwise Europe/Madrid is assumed.
     */
    public static Instant toInstant(String pattern, String value) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);
        TemporalAccessor parsed = formatter.parse(value);

        if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
            return OffsetDateTime.from(parsed).toInstant();
        }

        return LocalDateTime.from(parsed)
            .atZone(ZoneId.of("Europe/Madrid"))
            .toInstant();
    }

    /** Convenience overload that returns epoch milliseconds directly. */
    public static long toEpochMs(String pattern, String value) {
        return toInstant(pattern, value).toEpochMilli();
    }
}
