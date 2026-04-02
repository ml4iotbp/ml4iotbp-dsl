package es.upv.pros.ml4iotbp.io;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSource;
import es.upv.pros.ml4iotbp.domain.datasources.iot.IoTDataSourceBase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class IoTDataSourceDeserializer extends JsonDeserializer<IoTDataSource> {

  private static final Set<String> GLOBAL_KEYS = Set.of(
      "format",
      "paradigm",
      "protocol",
      "broker",
      "method",
      "host",
      "sampling-time",
      "end-point",
      "topic",
      "exchange",
      "schema",
      "time-stamp"
  );

  @Override
  public IoTDataSource deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    ObjectCodec codec = p.getCodec();
    JsonNode node = codec.readTree(p);

    if (!node.isObject()) {
      throw MismatchedInputException.from(
          p, IoTDataSource.class,
          "Expected an object for IoTDataSource, but got: " + node.getNodeType()
      );
    }

    ObjectMapper mapper = (ObjectMapper) codec;
    ObjectNode obj = (ObjectNode) node;

    // 1) Extraer globals en un node aparte
    ObjectNode globalsOnly = mapper.createObjectNode();
    Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> e = it.next();
      if (GLOBAL_KEYS.contains(e.getKey())) {
        globalsOnly.set(e.getKey(), e.getValue());
      }
    }

    // 2) Mapear globals a un tipo auxiliar SIN deserializador
    IoTDataSourceBase base = mapper.treeToValue(globalsOnly, IoTDataSourceBase.class);

    // 3) Construir el IoTDataSource final copiando campos
    IoTDataSource ds = new IoTDataSource();
    ds.setFormat(base.getFormat());
    ds.setParadigm(base.getParadigm());
    ds.setProtocol(base.getProtocol());
    ds.setBroker(base.getBroker());
    ds.setMethod(base.getMethod());
    ds.setHost(base.getHost());
    ds.setSamplingTime(base.getSamplingTime());
    ds.setEndPoint(base.getEndPoint());
    ds.setTopic(base.getTopic());
    ds.setExchange(base.getExchange());
    ds.setSchema(base.getSchema());
    ds.setTimeStamp(base.getTimeStamp());
    ds.setEndPoint(base.getEndPoint());

    // 4) Sensores: parseo recursivo CONTROLADO (reutilizando ESTE deserializador manualmente)
    Iterator<Map.Entry<String, JsonNode>> it2 = obj.fields();
    while (it2.hasNext()) {
      Map.Entry<String, JsonNode> e = it2.next();
      String key = e.getKey();
      JsonNode val = e.getValue();

      if (GLOBAL_KEYS.contains(key)) continue;

      if (val != null && val.isObject()) {
        // Crear un parser para el sub-árbol y deserializar recursivamente
        JsonParser subParser = val.traverse(mapper);
        subParser.nextToken(); // posiciona en START_OBJECT
        IoTDataSource sensor = deserialize(subParser, ctxt);
        ds.putSensor(key, sensor);
      }
    }

    return ds;
  }
}