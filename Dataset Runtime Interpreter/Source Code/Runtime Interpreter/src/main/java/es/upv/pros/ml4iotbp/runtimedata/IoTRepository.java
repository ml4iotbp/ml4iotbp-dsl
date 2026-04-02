package es.upv.pros.ml4iotbp.runtimedata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IoTRepository implements AutoCloseable {

    private String jdbcUrl;
    private Connection conn;
    private PreparedStatement insertStmt;

    private static IoTRepository instance=null;
    private static String dbPath="data/logs/runtime-logs.db";
    
    public static IoTRepository getCurrentInstance(){
        if(instance==null) instance=new IoTRepository(Path.of(dbPath));
        return instance;
    }

    private IoTRepository(Path dbFile){

        Objects.requireNonNull(dbFile, "dbFile");

        try {
            if (dbFile.getParent() != null) Files.createDirectories(dbFile.getParent());
        
            // JDBC URL: jdbc:sqlite:/absolute/path/to/file.db  (o relativo)
            this.jdbcUrl = "jdbc:sqlite:" + dbFile.toString();
            this.conn = DriverManager.getConnection(jdbcUrl);

            // Recomendado para concurrencia (WAL) y robustez básica
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL;");
                st.execute("PRAGMA synchronous=NORMAL;");
                st.execute("PRAGMA foreign_keys=ON;");
            }

            ensureSchema(conn);

            this.insertStmt = conn.prepareStatement(
                    "INSERT INTO IoTlogs(timestamp_ms, datasource, message) VALUES(?,?,?)"
            );
        }catch (Exception e) {
            throw new IllegalStateException("Cannot create directories for DB: " + dbFile, e);
        }
    }

    private static void ensureSchema(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS IoTlogs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp_ms INTEGER NOT NULL,
                  datasource TEXT NOT NULL,
                  message TEXT NOT NULL
                );
            """);

            st.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON IoTlogs(timestamp_ms);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_logs_source ON IoTlogs(datasource);");
        }
    }


    public void log(Instant timestamp, String datasource, String message) {

        Objects.requireNonNull(datasource, "datasource");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(timestamp, "timestamp");

        long ts = timestamp.toEpochMilli();

        try {
            synchronized (insertStmt) { // simple thread-safety
                insertStmt.setLong(1, ts);
                insertStmt.setString(2, datasource);
                insertStmt.setString(3, message);
              
                insertStmt.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("[SQLiteLogger] FAILED to write log: " + e.getMessage());
        }
    }

    /**
     * Limpieza por retención: borra logs anteriores a `cutoffEpochMs`.
     */
    public int deleteOlderThan(long cutoffEpochMs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM IoTlogs WHERE timestamp_ms < ?")) {
            ps.setLong(1, cutoffEpochMs);
            return ps.executeUpdate();
        }
    }

    /**
     * Ejemplo de consulta rápida: últimos N logs.
     */
    public ResultSet lastN(int n) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
                "SELECT id, timestamp_ms, datasource, message FROM IoTlogs ORDER BY timestamp_ms DESC LIMIT ?"
        );
        ps.setInt(1, n);
        return ps.executeQuery(); // OJO: el caller debe cerrar el ResultSet y el PreparedStatement
    }

    public String lastOneAsJSON() throws SQLException, JsonMappingException, JsonProcessingException {
        ResultSet rs=this.lastN(1);
        rs.next();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json= mapper.createObjectNode();
        json.put("timestamp",rs.getString(2));
        json.put("datasource",rs.getString(3));

        JsonNode message= mapper.readTree(rs.getString(4));
        message.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            String value = entry.getValue().asText();

            json.put(name, value);
        });

        rs.close();
        rs.getStatement().close();

        return mapper.writeValueAsString(json);
    }


    public String selectWindow(String ds, long instant1, long instant2) throws SQLException, JsonMappingException, JsonProcessingException {
        PreparedStatement ps = conn.prepareStatement(
                "SELECT id, timestamp_ms, datasource, message FROM IoTlogs WHERE datasource=? AND timestamp_ms>=? AND timestamp_ms<=? ORDER BY timestamp_ms DESC"
        );
        ps.setString(1, ds);
        ps.setLong(2, instant1);
        ps.setLong(3, instant2);
        ResultSet rs= ps.executeQuery();
        
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode list= mapper.createArrayNode();
        while(rs.next()){
            ObjectNode json= mapper.createObjectNode();
            json.put("timestamp",rs.getString(2));
            json.put("datasource",rs.getString(3));

            JsonNode message= mapper.readTree(rs.getString(4));
            message.fields().forEachRemaining(entry -> {
                String name = entry.getKey();
                String value = entry.getValue().asText();

                json.put(name, value);
            });
            list.add(json);
        }

        rs.close();
        rs.getStatement().close();

        return mapper.writeValueAsString(list);
    }

    @Override
    public void close() throws Exception {
        try { insertStmt.close(); } catch (Exception ignore) {}
        try { conn.close(); } catch (Exception ignore) {}
    }
}
