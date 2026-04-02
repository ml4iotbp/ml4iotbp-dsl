package es.upv.pros.ml4iotbp.domain.datasources;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = false)
public abstract class DataSource {

    public static Instant parseTimeStamp(String pattern, String value){
        /*DateTimeFormatter fmt =DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);
        LocalDateTime ldt = LocalDateTime.parse(value, fmt);
        Instant instant = ldt.atZone(ZoneId.of("Europe/Madrid")).toInstant();
        return instant;*/

        DateTimeFormatter fmt =
        DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);

        TemporalAccessor ta = fmt.parse(value);

        // If the timestamp contains an offset / zone → preserve it
        if (ta.isSupported(ChronoField.OFFSET_SECONDS)) {
            return OffsetDateTime.from(ta).toInstant();
        }

        // Otherwise, treat it as local time in Europe/Madrid
        LocalDateTime ldt = LocalDateTime.from(ta);
        return ldt.atZone(ZoneId.of("Europe/Madrid")).toInstant();
    }

}