package es.upv.pros.ml4iotbp.utils;

import java.time.Duration;
import java.time.format.DateTimeParseException;

public class DurationParser {

    public static Duration parseTime(String samplingTime) {
        if (samplingTime == null || samplingTime.isBlank()) return Duration.ofSeconds(10);
        try {
            return Duration.parse(samplingTime.trim());
        } catch (DateTimeParseException ex) {
            // fallback: si alguien pone "5s" o "1000ms"
            String s = samplingTime.trim().toLowerCase();
            if (s.endsWith("ms")) return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
            if (s.endsWith("s"))  return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
            if (s.endsWith("m"))  return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
            throw new IllegalArgumentException("Invalid sampling-time (use ISO-8601 like PT5S): " + samplingTime, ex);
        }
    }

}
