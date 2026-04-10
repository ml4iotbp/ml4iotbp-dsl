#235 líneas de código efectivo.
import json
import csv
import os
import importlib.resources
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROCESS_DEFINITION_KEY = "DrugBatches"
CAMUNDA_TS_PATTERN      = "%Y-%m-%dT%H:%M:%S.%f%z"
CSV_SENSOR_PATTERN      = "%d %b %Y %H:%M:%S %Z"
RESOURCES_BASE          = Path("resources/pharmacy")   # adjust to your layout


# ---------------------------------------------------------------------------
# Timestamp parsing  (mirrors Java's TimeUtils.parseTimeStamp)
# ---------------------------------------------------------------------------

def parse_timestamp(pattern: str, value: str) -> datetime:
    """
    Parse a date-time string and return a timezone-aware datetime.
    If the value carries no timezone info, Europe/Madrid is assumed.
    """
    try:
        dt = datetime.strptime(value.strip(), pattern)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Europe/Madrid"))
        return dt
    except ValueError:
        # Fallback: try without microseconds (Camunda sometimes omits them)
        fallback = pattern.replace(".%f", "")
        dt = datetime.strptime(value.strip(), fallback)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Europe/Madrid"))
        return dt


def to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def round2(value: float) -> float:
    return round(value * 100.0) / 100.0


# ---------------------------------------------------------------------------
# Sensor buffer helpers
# ---------------------------------------------------------------------------

def load_sensor_csv(
    csv_text: str,
    reception_sensor_id: str,
    reception_buf: list,
    refrigerator_buf: list,
    reception_out: str,
    refrigerator_out: str,
) -> None:
    """
    Parse a sensor CSV file (light / temperature / humidity).
    Rows are split between two output files based on the sensor ID in col[0].
    Each buffer entry is a dict {epoch_ms: float_value}.
    """
    lines = csv_text.splitlines()
    with open(reception_out, "w", newline="") as fa, \
         open(refrigerator_out, "w", newline="") as fb:
        for line in lines[1:]:                       # skip header
            cols = line.split(",")
            if len(cols) < 4:
                continue
            epoch_ms = to_epoch_ms(
                parse_timestamp(CSV_SENSOR_PATTERN, cols[3])
            )
            value = float(cols[2])
            row = f"{epoch_ms},{value}\n"
            if cols[0] == reception_sensor_id:
                reception_buf.append({epoch_ms: value})
                fa.write(row)
                fa.flush()
            else:
                refrigerator_buf.append({epoch_ms: value})
                fb.write(row)
                fb.flush()


# ---------------------------------------------------------------------------
# Average calculation  (mirrors Java avgTemp / avgHum / avgLight)
# ---------------------------------------------------------------------------

def compute_average(readings: list, start_ms: int, end_ms: int) -> float:
    """
    Returns the rounded average of readings within [start_ms, end_ms].
    If no readings fall in the window, the window is shifted back by its own
    length until at least one reading is found.
    """
    window = end_ms - start_ms
    total  = 0.0
    count  = 0

    while count == 0:
        for entry in readings:
            ts = next(iter(entry))
            if start_ms <= ts <= end_ms:
                total += entry[ts]
                count += 1
        start_ms -= window

    return round2(total / count)


# ---------------------------------------------------------------------------
# Activity variables loader
# ---------------------------------------------------------------------------

def load_activity_vars(activity_instance_id: str) -> list:
    path = RESOURCES_BASE / "activities" / f"{activity_instance_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Activity file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Camunda instance grouping
# ---------------------------------------------------------------------------

def group_by_instance(camunda_events: list) -> dict:
    """
    Groups Camunda history events by processInstanceId,
    keeping only events for the target process definition.
    Returns an ordered dict: {instance_id: [activity, ...]}
    """
    instances: dict = {}
    last_id = ""
    for event in camunda_events:
        if event.get("processDefinitionKey") != PROCESS_DEFINITION_KEY:
            continue
        instance_id = event["processInstanceId"]
        if instance_id != last_id:
            last_id = instance_id
            instances[instance_id] = []
        instances[instance_id].append(event)
    return instances


# ---------------------------------------------------------------------------
# Instance processing  (mirrors Java processInstances)
# ---------------------------------------------------------------------------

def process_instances(
    instances: dict,
    reception_temp: list,
    reception_hum:  list,
    reception_light: list,
    refr_temp:  list,
    refr_hum:   list,
    refr_light: list,
) -> None:
    print("INST,BI,TH,VI,VC,MIS,SSD,RCT,RCH,RCL,CEW,CET,CEH,CEL,R")

    for instance_id, activities in instances.items():
        row: dict = {}

        eval_completed  = None
        eval_started    = None
        reception_time  = None
        storage_time    = None

        for activity in activities:
            activity_id = activity.get("activityId", "")

            # --- batchArrivalMsg ---
            if "batchArrivalMsg" in activity_id:
                reception_time = parse_timestamp(CAMUNDA_TS_PATTERN, activity["endTime"])

            # --- manualInspectionTask ---
            elif "manualInspectionTask" in activity_id:
                act_vars = load_activity_vars(activity["id"])
                for var in act_vars:
                    if var.get("type") == "variableUpdate":
                        name = var.get("variableName")
                        if name == "batchId":
                            row["BI"] = var["value"]
                        elif name == "hardness":
                            row["TH"] = var["value"]
                        elif name == "integrity":
                            row["VI"] = var["value"]
                        elif name == "visibleContamination":
                            row["VC"] = var["value"]

                start_time = parse_timestamp(CAMUNDA_TS_PATTERN, activity["startTime"])
                end_time   = parse_timestamp(CAMUNDA_TS_PATTERN, activity["endTime"])

                row["MIS"] = round2(
                    to_epoch_ms(end_time) - to_epoch_ms(reception_time)
                )

                eval_completed = end_time
                eval_started   = start_time

            # --- selectSampleTask ---
            elif "selectSampleTask" in activity_id:
                if eval_completed is not None:
                    start_time = parse_timestamp(CAMUNDA_TS_PATTERN, activity["startTime"])
                    end_time   = parse_timestamp(CAMUNDA_TS_PATTERN, activity["endTime"])

                    row["SSD"] = round2(
                        to_epoch_ms(end_time) - to_epoch_ms(start_time)
                    )

                    from_ms = to_epoch_ms(eval_started)
                    to_ms   = to_epoch_ms(end_time)

                    row["RCT"] = compute_average(reception_temp,  from_ms, to_ms)
                    row["RCH"] = compute_average(reception_hum,   from_ms, to_ms)
                    row["RCL"] = compute_average(reception_light, from_ms, to_ms)

            # --- storageTask ---
            elif "storageTask" in activity_id:
                storage_time = parse_timestamp(CAMUNDA_TS_PATTERN, activity["endTime"])

            # --- labReadyMessage ---
            elif "labReadyMessage" in activity_id:
                start_time = parse_timestamp(CAMUNDA_TS_PATTERN, activity["startTime"])
                end_time   = parse_timestamp(CAMUNDA_TS_PATTERN, activity["endTime"])

                row["CEW"] = round2(
                    to_epoch_ms(end_time) - to_epoch_ms(storage_time)
                )

                from_ms = to_epoch_ms(start_time)
                to_ms   = to_epoch_ms(end_time)

                row["CET"] = compute_average(refr_temp,  from_ms, to_ms)
                row["CEH"] = compute_average(refr_hum,   from_ms, to_ms)
                row["CEL"] = compute_average(refr_light, from_ms, to_ms)

            # --- meetRegulationCondition ---
            elif "meetRegulationCondition" in activity_id:
                act_vars = load_activity_vars(activity["id"])
                for var in act_vars:
                    if var.get("type") == "variableUpdate":
                        row["R"] = var["value"]

        print(
            f"{instance_id},{row.get('BI')},{row.get('TH')},{row.get('VI')},"
            f"{row.get('VC')},{row.get('MIS')},{row.get('SSD')},"
            f"{row.get('RCT')},{row.get('RCH')},{row.get('RCL')},"
            f"{row.get('CEW')},{row.get('CET')},{row.get('CEH')},"
            f"{row.get('CEL')},{row.get('R')}"
        )

    print(len(instances))


# ---------------------------------------------------------------------------
# Main  (mirrors Java generateDataSet)
# ---------------------------------------------------------------------------

def generate_dataset() -> None:
    # Load Camunda JSON
    camunda_path = RESOURCES_BASE / "camunda.json"
    if not camunda_path.exists():
        raise FileNotFoundError(f"Camunda file not found: {camunda_path}")
    with open(camunda_path, encoding="utf-8") as f:
        camunda_events = json.load(f)

    # Sensor buffers
    reception_light: list = []
    refr_light:      list = []
    reception_temp:  list = []
    refr_temp:       list = []
    reception_hum:   list = []
    refr_hum:        list = []

    # Parse light CSV
    light_path = RESOURCES_BASE / "light.csv"
    if not light_path.exists():
        raise FileNotFoundError(f"Light CSV not found: {light_path}")
    load_sensor_csv(
        light_path.read_text(encoding="utf-8"),
        reception_sensor_id="iot-sn-light_recep",
        reception_buf=reception_light,
        refrigerator_buf=refr_light,
        reception_out="processedLightReception.csv",
        refrigerator_out="processedLightRefrigerator.csv",
    )

    # Parse temperature CSV
    temp_path = RESOURCES_BASE / "temperature.csv"
    if not temp_path.exists():
        raise FileNotFoundError(f"Temperature CSV not found: {temp_path}")
    load_sensor_csv(
        temp_path.read_text(encoding="utf-8"),
        reception_sensor_id="iot-sn-temp_recep",
        reception_buf=reception_temp,
        refrigerator_buf=refr_temp,
        reception_out="processedTempReception.csv",
        refrigerator_out="processedTempRefrigerator.csv",
    )

    # Parse humidity CSV
    hum_path = RESOURCES_BASE / "humidity.csv"
    if not hum_path.exists():
        raise FileNotFoundError(f"Humidity CSV not found: {hum_path}")
    load_sensor_csv(
        hum_path.read_text(encoding="utf-8"),
        reception_sensor_id="iot-sn-hum_recep",
        reception_buf=reception_hum,
        refrigerator_buf=refr_hum,
        reception_out="processedHumReception.csv",
        refrigerator_out="processedHumRefrigerator.csv",
    )

    # Group Camunda events and build report
    instances = group_by_instance(camunda_events)
    process_instances(
        instances,
        reception_temp, reception_hum, reception_light,
        refr_temp, refr_hum, refr_light,
    )


if __name__ == "__main__":
    generate_dataset()
