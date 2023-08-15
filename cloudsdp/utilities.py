from google.cloud import bigquery

BIGQUERY_TO_PRIMITIVE_TYPE_MAP = {
    "BOOL": [bool],
    "DATE": [str],
    "INTEGER": [int],
    "JSON": [str],
    "NUMERIC": [float],
    "STRING": [str],
    "TIME": [str],
    "TIMESTAMP": [str],
}
TYPE_CHECKING_SUPPORTED_FIELD_TYPES = list(BIGQUERY_TO_PRIMITIVE_TYPE_MAP.keys())
PANDAS_TO_PRIMITIVE_TYPE_MAP = {
    "object": str,  # Disallowing mixed types that gets represented as object. Can have NaN values if not 'REQUIRED'
    "int64": int,
    "float64": float,
    "bool": bool,
    "datetime64": str,
    "category": str,
}


def is_series_of_dtype(series, dtype):
    """Check if all the non NaN values of a series is of a data type"""
    return series[~series.isna()].apply(lambda x: not isinstance(x, dtype)).sum() == 0


def compare_schema(schema_a, schema_b):
    """
    Compare two schemas
    :param schema_a: first list of schema fields.
    :param schema_b: second list of schema fields.
    :return:      if there is difference between both schema.
    """
    diff = [i for i in schema_a + schema_b if i not in schema_a or i not in schema_b]
    schema_equal = len(diff) == 0
    return schema_equal


def construct_schema_fields(schema_fields):
    if isinstance(schema_fields[0], bigquery.SchemaField):
        return schema_fields

    deserialized_schema = []
    for field in schema_fields:
        deserialized_schema.append(bigquery.SchemaField(field["name"], field["field_type"], field["mode"]))
    return deserialized_schema


def deconstruct_schema_fields(schema_fields):
    if isinstance(schema_fields[0], dict) and (
        set(["name", "field_type", "mode"]).issubset(set(schema_fields[0].keys()))
    ):
        return schema_fields

    serialized_schema = []
    for field in schema_fields:
        serialized_schema.append({"name": field.name, "field_type": field.field_type, "mode": field.mode})

    return serialized_schema


def objectify(array, key):
    return {el[key]: el for el in array}


def schemacheck_series(series, schema_field_type, schema_field_mode):
    has_nans = series.isna().sum() > 0
    nan_check_passed = (schema_field_mode != "NULLABLE" and not has_nans) or schema_field_mode == "NULLABLE"
    type_check_passed = False

    if schema_field_type in BIGQUERY_TO_PRIMITIVE_TYPE_MAP:
        type_that_pandas_match = PANDAS_TO_PRIMITIVE_TYPE_MAP[str(series.dtype)]
        type_check_passed = is_series_of_dtype(series, type_that_pandas_match)

        type_check_passed = (
            type_check_passed and type_that_pandas_match in BIGQUERY_TO_PRIMITIVE_TYPE_MAP[schema_field_type]
        )

        return type_check_passed, nan_check_passed

    return True, nan_check_passed


def clean_dataframe_using_schema(dataframe, schema):
    columns = dataframe.columns

    deconstructed_schema = deconstruct_schema_fields(schema)
    field_map = objectify(deconstructed_schema, "name")

    fields = [field["name"] for field in deconstructed_schema]
    required_fields = [field["name"] for field in deconstructed_schema if field["mode"] == "REQUIRED"]

    if not set(required_fields).issubset(set(columns)):
        missing_required_fields = set(required_fields) - set(columns)
        raise Exception(f"DataFrame is missing required fields from the schema: [{', '.join(missing_required_fields)}]")

    dropped_columns = []
    type_mismatch_columns = []
    nans_not_allowed_columns = []
    for column in columns:
        if column not in fields:
            dropped_columns.append(column)
        else:
            valid_type, valid_nans = schemacheck_series(
                dataframe[column], field_map[column]["field_type"], field_map[column]["mode"]
            )
            if not valid_type:
                type_mismatch_columns.append(column)

            if not valid_nans:
                nans_not_allowed_columns.append(column)

    if type_mismatch_columns:
        raise Exception(f"DataFrame column types do not match with schema: [{', '.join(type_mismatch_columns)}]")

    if nans_not_allowed_columns:
        raise Exception(f"DataFrame has NaNs in non nullable columns: [{', '.join(nans_not_allowed_columns)}]")

    if dropped_columns:
        return dataframe.drop(dropped_columns, axis=1)

    return dataframe
