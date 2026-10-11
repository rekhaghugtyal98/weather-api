"""Spark schemas used by the pipeline."""
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType
)

HOURLY_SCHEMA = StructType([
    StructField("time", ArrayType(StringType()), True),
    StructField("temperature_2m", ArrayType(DoubleType()), True),
    StructField("relative_humidity_2m", ArrayType(IntegerType()), True),
    StructField("precipitation", ArrayType(DoubleType()), True),
    StructField("wind_speed_10m", ArrayType(DoubleType()), True),
])

API_RESPONSE_SCHEMA = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("hourly", HOURLY_SCHEMA, True),
])

# Contract used to detect drift while Bronze still preserves the complete raw response.
EXPECTED_PAYLOAD_SCHEMA = {
    "latitude": "number",
    "longitude": "number",
    "timezone": "string",
    "hourly": "object",
    "hourly.time": "array<string>",
    "hourly.temperature_2m": "array<number>",
    "hourly.relative_humidity_2m": "array<number>",
    "hourly.precipitation": "array<number>",
    "hourly.wind_speed_10m": "array<number>",
}


def _json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        kinds = sorted({_json_type(item) for item in value if item is not None})
        return "array<{}>".format("|".join(kinds) if kinds else "null")
    return type(value).__name__


def detect_payload_schema_changes(payload):
    """Report missing, newly added, and type-changed fields against our API contract."""
    if not isinstance(payload, dict):
        return [{"field_path": "$", "change_type": "type_changed",
                 "expected_type": "object", "observed_type": _json_type(payload)}]
    observed = dict(payload)
    hourly = payload.get("hourly")
    if isinstance(hourly, dict):
        observed.update({f"hourly.{key}": value for key, value in hourly.items()})
    expected = EXPECTED_PAYLOAD_SCHEMA
    changes = []
    for path, expected_type in expected.items():
        if path not in observed:
            changes.append({"field_path": path, "change_type": "missing",
                            "expected_type": expected_type, "observed_type": None})
            continue
        actual_type = _json_type(observed[path])
        compatible = actual_type == expected_type or (
            expected_type.startswith("array<") and actual_type.startswith("array<")
            and all(kind in expected_type[6:-1].split("|") for kind in actual_type[6:-1].split("|"))
        )
        if actual_type == "null":
            continue
        if not compatible:
            changes.append({"field_path": path, "change_type": "type_changed",
                            "expected_type": expected_type, "observed_type": actual_type})
    for path in sorted(set(observed) - set(expected)):
        changes.append({"field_path": path, "change_type": "new",
                        "expected_type": None, "observed_type": _json_type(observed[path])})
    return changes
