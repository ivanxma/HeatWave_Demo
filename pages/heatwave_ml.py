import json
import time
from datetime import datetime, timezone

import mysql.connector
from flask import flash, request, session

from app_context import (
    _database_exists,
    _mysql_quote,
    _normalize_modal_cell,
    _quote_identifier,
    _table_exists,
    app,
    close_mysql_connection,
    get_connection_config,
    get_generation_models,
    login_required,
    mysql_connection,
    render_dashboard,
    run_sql,
    run_sql_with_columns,
)

IRIS_NON_PK_COLUMNS = [
    "sepal length",
    "sepal width",
    "petal length",
    "petal width",
    "class",
]

IRIS_TRAIN_ROWS_RAW = """
6.4|2.8|5.6|2.2|Iris-virginica
5.0|2.3|3.3|1.0|Iris-setosa
4.9|2.5|4.5|1.7|Iris-virginica
4.9|3.1|1.5|0.1|Iris-versicolor
5.7|3.8|1.7|0.3|Iris-versicolor
4.4|3.2|1.3|0.2|Iris-versicolor
5.4|3.4|1.5|0.4|Iris-versicolor
6.9|3.1|5.1|2.3|Iris-virginica
6.7|3.1|4.4|1.4|Iris-setosa
5.1|3.7|1.5|0.4|Iris-versicolor
5.2|2.7|3.9|1.4|Iris-setosa
6.9|3.1|4.9|1.5|Iris-setosa
5.8|4.0|1.2|0.2|Iris-versicolor
5.4|3.9|1.7|0.4|Iris-versicolor
7.7|3.8|6.7|2.2|Iris-virginica
6.3|3.3|4.7|1.6|Iris-setosa
6.8|3.2|5.9|2.3|Iris-virginica
7.6|3.0|6.6|2.1|Iris-virginica
6.4|3.2|5.3|2.3|Iris-virginica
5.7|4.4|1.5|0.4|Iris-versicolor
6.7|3.3|5.7|2.1|Iris-virginica
6.4|2.8|5.6|2.1|Iris-virginica
5.4|3.9|1.3|0.4|Iris-versicolor
6.1|2.6|5.6|1.4|Iris-virginica
7.2|3.0|5.8|1.6|Iris-virginica
5.2|3.5|1.5|0.2|Iris-versicolor
5.8|2.6|4.0|1.2|Iris-setosa
5.9|3.0|5.1|1.8|Iris-virginica
5.4|3.0|4.5|1.5|Iris-setosa
6.7|3.0|5.0|1.7|Iris-setosa
6.3|2.3|4.4|1.3|Iris-setosa
5.1|2.5|3.0|1.1|Iris-setosa
6.4|3.2|4.5|1.5|Iris-setosa
6.8|3.0|5.5|2.1|Iris-virginica
6.2|2.8|4.8|1.8|Iris-virginica
6.9|3.2|5.7|2.3|Iris-virginica
6.5|3.2|5.1|2.0|Iris-virginica
5.8|2.8|5.1|2.4|Iris-virginica
5.1|3.8|1.5|0.3|Iris-versicolor
4.8|3.0|1.4|0.3|Iris-versicolor
7.9|3.8|6.4|2.0|Iris-virginica
5.8|2.7|5.1|1.9|Iris-virginica
6.7|3.0|5.2|2.3|Iris-virginica
5.1|3.8|1.9|0.4|Iris-versicolor
4.7|3.2|1.6|0.2|Iris-versicolor
6.0|2.2|5.0|1.5|Iris-virginica
4.8|3.4|1.6|0.2|Iris-versicolor
7.7|2.6|6.9|2.3|Iris-virginica
4.6|3.6|1.0|0.2|Iris-versicolor
7.2|3.2|6.0|1.8|Iris-virginica
5.0|3.3|1.4|0.2|Iris-versicolor
6.6|3.0|4.4|1.4|Iris-setosa
6.1|2.8|4.0|1.3|Iris-setosa
5.0|3.2|1.2|0.2|Iris-versicolor
7.0|3.2|4.7|1.4|Iris-setosa
6.0|3.0|4.8|1.8|Iris-virginica
7.4|2.8|6.1|1.9|Iris-virginica
5.8|2.7|5.1|1.9|Iris-virginica
6.2|3.4|5.4|2.3|Iris-virginica
5.0|2.0|3.5|1.0|Iris-setosa
5.6|2.5|3.9|1.1|Iris-setosa
6.7|3.1|5.6|2.4|Iris-virginica
6.3|2.5|5.0|1.9|Iris-virginica
6.4|3.1|5.5|1.8|Iris-virginica
6.2|2.2|4.5|1.5|Iris-setosa
7.3|2.9|6.3|1.8|Iris-virginica
4.4|3.0|1.3|0.2|Iris-versicolor
7.2|3.6|6.1|2.5|Iris-virginica
6.5|3.0|5.5|1.8|Iris-virginica
5.0|3.4|1.5|0.2|Iris-versicolor
4.7|3.2|1.3|0.2|Iris-versicolor
6.6|2.9|4.6|1.3|Iris-setosa
5.5|3.5|1.3|0.2|Iris-versicolor
7.7|3.0|6.1|2.3|Iris-virginica
6.1|3.0|4.9|1.8|Iris-virginica
4.9|3.1|1.5|0.1|Iris-versicolor
5.5|2.4|3.8|1.1|Iris-setosa
5.7|2.9|4.2|1.3|Iris-setosa
6.0|2.9|4.5|1.5|Iris-setosa
6.4|2.7|5.3|1.9|Iris-virginica
5.4|3.7|1.5|0.2|Iris-versicolor
6.1|2.9|4.7|1.4|Iris-setosa
6.5|2.8|4.6|1.5|Iris-setosa
5.6|2.7|4.2|1.3|Iris-setosa
6.3|3.4|5.6|2.4|Iris-virginica
4.9|3.1|1.5|0.1|Iris-versicolor
6.8|2.8|4.8|1.4|Iris-setosa
5.7|2.8|4.5|1.3|Iris-setosa
6.0|2.7|5.1|1.6|Iris-setosa
5.0|3.5|1.3|0.3|Iris-versicolor
6.5|3.0|5.2|2.0|Iris-virginica
6.1|2.8|4.7|1.2|Iris-setosa
5.1|3.5|1.4|0.3|Iris-versicolor
4.6|3.1|1.5|0.2|Iris-versicolor
6.5|3.0|5.8|2.2|Iris-virginica
4.6|3.4|1.4|0.3|Iris-versicolor
4.6|3.2|1.4|0.2|Iris-versicolor
7.7|2.8|6.7|2.0|Iris-virginica
5.9|3.2|4.8|1.8|Iris-setosa
5.1|3.8|1.6|0.2|Iris-versicolor
4.9|3.0|1.4|0.2|Iris-versicolor
4.9|2.4|3.3|1.0|Iris-setosa
4.5|2.3|1.3|0.3|Iris-versicolor
5.8|2.7|4.1|1.0|Iris-setosa
5.0|3.4|1.6|0.4|Iris-versicolor
5.2|3.4|1.4|0.2|Iris-versicolor
5.3|3.7|1.5|0.2|Iris-versicolor
5.0|3.6|1.4|0.2|Iris-versicolor
5.6|2.9|3.6|1.3|Iris-setosa
4.8|3.1|1.6|0.2|Iris-versicolor
6.3|2.7|4.9|1.8|Iris-virginica
5.7|2.8|4.1|1.3|Iris-setosa
5.0|3.0|1.6|0.2|Iris-versicolor
6.3|3.3|6.0|2.5|Iris-virginica
5.0|3.5|1.6|0.6|Iris-versicolor
5.5|2.6|4.4|1.2|Iris-setosa
5.7|3.0|4.2|1.2|Iris-setosa
4.4|2.9|1.4|0.2|Iris-versicolor
4.8|3.0|1.4|0.1|Iris-versicolor
5.5|2.4|3.7|1.0|Iris-setosa
"""

IRIS_TEST_ROWS_RAW = """
5.9|3.0|4.2|1.5|Iris-setosa
6.9|3.1|5.4|2.1|Iris-virginica
5.1|3.3|1.7|0.5|Iris-versicolor
6.0|3.4|4.5|1.6|Iris-setosa
5.5|2.5|4.0|1.3|Iris-setosa
6.2|2.9|4.3|1.3|Iris-setosa
5.5|4.2|1.4|0.2|Iris-versicolor
6.3|2.8|5.1|1.5|Iris-virginica
5.6|3.0|4.1|1.3|Iris-setosa
6.7|2.5|5.8|1.8|Iris-virginica
7.1|3.0|5.9|2.1|Iris-virginica
4.3|3.0|1.1|0.1|Iris-versicolor
5.6|2.8|4.9|2.0|Iris-virginica
5.5|2.3|4.0|1.3|Iris-setosa
6.0|2.2|4.0|1.0|Iris-setosa
5.1|3.5|1.4|0.2|Iris-versicolor
5.7|2.6|3.5|1.0|Iris-setosa
4.8|3.4|1.9|0.2|Iris-versicolor
5.1|3.4|1.5|0.2|Iris-versicolor
5.7|2.5|5.0|2.0|Iris-virginica
5.4|3.4|1.7|0.2|Iris-versicolor
5.6|3.0|4.5|1.5|Iris-setosa
6.3|2.9|5.6|1.8|Iris-virginica
6.3|2.5|4.9|1.5|Iris-setosa
5.8|2.7|3.9|1.2|Iris-setosa
6.1|3.0|4.6|1.4|Iris-setosa
5.2|4.1|1.5|0.1|Iris-versicolor
6.7|3.1|4.7|1.5|Iris-setosa
6.7|3.3|5.7|2.5|Iris-virginica
6.4|2.9|4.3|1.3|Iris-setosa
"""


def _parse_iris_rows(raw_text):
    rows = []
    for line in str(raw_text or "").strip().splitlines():
        normalized = line.strip()
        if not normalized:
            continue
        sepal_length, sepal_width, petal_length, petal_width, iris_class = normalized.split("|", 4)
        rows.append(
            (
                float(sepal_length),
                float(sepal_width),
                float(petal_length),
                float(petal_width),
                iris_class,
            )
        )
    return rows


IRIS_TRAIN_ROWS = _parse_iris_rows(IRIS_TRAIN_ROWS_RAW)
IRIS_TEST_ROWS = _parse_iris_rows(IRIS_TEST_ROWS_RAW)

IRIS_DOCS_URL = "https://dev.mysql.com/doc/heatwave/en/mys-hwaml-iris-quickstart.html"
IRIS_ML_TRAIN_CALL_TEXT = (
    "CALL sys.ML_TRAIN('ml_data.iris_train', 'class', "
    "JSON_OBJECT('task', 'classification', 'exclude_column_list', JSON_ARRAY('my_row_id')), @model);"
)
IRIS_ML_MODEL_LOAD_CALL_TEXT = 'CALL sys.ML_MODEL_LOAD("iris_model", NULL);'
IRIS_ML_PREDICT_ROW_SET_TEXT = """mysql> SET @row_input = JSON_OBJECT(
           "sepal length", 7.3,
           "sepal width", 2.9,
           "petal length", 6.3,
           "petal width", 1.8);"""
IRIS_ML_PREDICT_ROW_SELECT_TEXT = "mysql> SELECT sys.ML_PREDICT_ROW(@row_input, @model, NULL);"
IRIS_ML_PREDICT_ROW_CALL_TEXT = "{}\n\n{}".format(
    IRIS_ML_PREDICT_ROW_SET_TEXT,
    IRIS_ML_PREDICT_ROW_SELECT_TEXT,
)
IRIS_ML_PREDICT_TABLE_CALL_TEXT = "CALL sys.ML_PREDICT_TABLE('ml_data.iris_test', @model, 'ml_data.iris_predictions', NULL);"
IRIS_ML_SCORE_CALL_TEXT = (
    "CALL sys.ML_SCORE('ml_data.iris_validate', 'class', @iris_model, "
    "'balanced_accuracy', @score, NULL);"
)
IRIS_ML_EXPLAIN_TABLE_CALL_TEXT = (
    "CALL sys.ML_EXPLAIN_TABLE('ml_data.iris_test', @iris_model, "
    "'ml_data.iris_explanations', JSON_OBJECT('prediction_explainer', 'permutation_importance'));"
)
NL2ML_DOCS_URL = "https://dev.mysql.com/doc/heatwave/en/mys-hwaml-nl2ml.html"
NL2ML_DEFAULT_PROMPT = "How to do HeatWave Training with dataset ml_data.iris_train."
NL2ML_OUTPUT_VAR = "@output"
CLASSIFICATION_OPTIMIZATION_METRICS = [
    {"value": "accuracy", "label": "accuracy"},
    {"value": "f1", "label": "f1"},
    {"value": "f1_macro", "label": "f1_macro"},
    {"value": "f1_micro", "label": "f1_micro"},
    {"value": "f1_weighted", "label": "f1_weighted"},
    {"value": "neg_log_loss", "label": "neg_log_loss"},
    {"value": "precision", "label": "precision"},
    {"value": "precision_macro", "label": "precision_macro"},
    {"value": "precision_micro", "label": "precision_micro"},
    {"value": "precision_weighted", "label": "precision_weighted"},
    {"value": "recall", "label": "recall"},
    {"value": "recall_macro", "label": "recall_macro"},
    {"value": "recall_micro", "label": "recall_micro"},
    {"value": "recall_weighted", "label": "recall_weighted"},
    {"value": "roc_auc", "label": "roc_auc"},
]
DEFAULT_CLASSIFICATION_OPTIMIZATION_METRIC = CLASSIFICATION_OPTIMIZATION_METRICS[0]["value"]
IRIS_ML_TRAIN_CONNECTION_TIMEOUT_SECONDS = 30
IRIS_ML_TRAIN_READ_TIMEOUT_SECONDS = 1800
IRIS_ML_TRAIN_WRITE_TIMEOUT_SECONDS = 1800


def _iris_insert_sql(table_name):
    column_sql = ", ".join(_quote_identifier(column) for column in IRIS_NON_PK_COLUMNS)
    placeholders = ", ".join(["%s"] * len(IRIS_NON_PK_COLUMNS))
    return "insert into ml_data.{} ({}) values ({})".format(
        _quote_identifier(table_name),
        column_sql,
        placeholders,
    )


def _normalize_classification_optimization_metric(value):
    normalized = str(value or "").strip()
    allowed = {item["value"] for item in CLASSIFICATION_OPTIMIZATION_METRICS}
    return normalized if normalized in allowed else DEFAULT_CLASSIFICATION_OPTIMIZATION_METRIC


def _build_iris_ml_train_call_text(optimization_metric=""):
    normalized_metric = _normalize_classification_optimization_metric(optimization_metric)
    options_parts = [
        "'task', 'classification'",
        "'exclude_column_list', JSON_ARRAY('my_row_id')",
        "'optimization_metric', '{}'".format(normalized_metric),
    ]
    return (
        "CALL sys.ML_TRAIN('ml_data.iris_train', 'class', "
        "JSON_OBJECT({}), @model);"
    ).format(", ".join(options_parts))


def _iris_ml_train_connection_config():
    config = dict(get_connection_config(include_database=False))
    config["connection_timeout"] = IRIS_ML_TRAIN_CONNECTION_TIMEOUT_SECONDS
    config["read_timeout"] = IRIS_ML_TRAIN_READ_TIMEOUT_SECONDS
    config["write_timeout"] = IRIS_ML_TRAIN_WRITE_TIMEOUT_SECONDS
    return config


def _fetch_iris_train_table():
    return _fetch_named_table("ml_data", "iris_train")


def _table_has_column(schema_name, table_name, column_name):
    rows = run_sql(
        """
        select count(*)
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
          and column_name = %s
        """,
        (schema_name, table_name, column_name),
        include_database=False,
    )
    return bool(rows and rows[0][0])


def _fetch_named_table(schema_name, table_name):
    order_clause = " order by `my_row_id`" if _table_has_column(schema_name, table_name, "my_row_id") else ""
    return run_sql_with_columns(
        """
        select *
        from {}.{}{}
        """.format(_quote_identifier(schema_name), _quote_identifier(table_name), order_clause),
        include_database=False,
    )


def _ml_schema_name():
    user_name = str(session.get("db_user", "")).strip()
    return f"ML_SCHEMA_{user_name}" if user_name else ""


def _model_catalog_exists(schema_name):
    if not schema_name:
        return False
    rows = run_sql(
        """
        select count(*)
        from information_schema.tables
        where table_schema = %s
          and table_name = 'MODEL_CATALOG'
        """,
        (schema_name,),
        include_database=False,
    )
    return bool(rows and rows[0][0])


def _fetch_model_catalog(schema_name):
    if not _model_catalog_exists(schema_name):
        return {"columns": [], "rows": []}
    return run_sql_with_columns(
        """
        select *
        from {}.MODEL_CATALOG
        where model_handle = 'iris_model'
        order by model_id desc
        """.format(_quote_identifier(schema_name)),
        include_database=False,
    )


def _build_model_catalog_records(model_catalog):
    columns = list(model_catalog.get("columns") or [])
    rows = list(model_catalog.get("rows") or [])
    records = []
    for row in rows:
        fields = []
        for index, column in enumerate(columns):
            value = row[index] if index < len(row) else ""
            fields.append({"label": column, "value": _normalize_modal_cell(value)})
        records.append(fields)
    return records


def _build_prediction_records(prediction_value):
    normalized = _normalize_modal_cell(prediction_value)
    try:
        parsed = json.loads(normalized) if isinstance(normalized, str) and normalized else normalized
    except json.JSONDecodeError:
        parsed = normalized

    if isinstance(parsed, dict):
        return [[{"label": str(key), "value": _normalize_modal_cell(value)} for key, value in parsed.items()]]
    if isinstance(parsed, list):
        return [[{"label": "Value {}".format(index + 1), "value": _normalize_modal_cell(value)} for index, value in enumerate(parsed)]]
    return [[{"label": "Prediction", "value": _normalize_modal_cell(parsed)}]]


def _build_score_records(score_value):
    return [[{"label": "@score", "value": _normalize_modal_cell(score_value)}]]


def _build_table_records(table_result):
    columns = list(table_result.get("columns") or [])
    rows = list(table_result.get("rows") or [])
    records = []
    for row in rows:
        fields = []
        for index, column in enumerate(columns):
            value = row[index] if index < len(row) else ""
            fields.append({"label": column, "value": _normalize_modal_cell(value)})
        records.append(fields)
    return records


def _consume_pending_results(cursor):
    if cursor.with_rows:
        cursor.fetchall()
    while cursor.nextset():
        if cursor.with_rows:
            cursor.fetchall()


def _collect_pending_resultsets(cursor, title_prefix="Result Set"):
    datasets = []
    result_index = 1
    if cursor.with_rows:
        datasets.append(
            {
                "title": "{} {}".format(title_prefix, result_index),
                "columns": list(cursor.column_names or ()),
                "rows": [
                    [_normalize_modal_cell(value) for value in row]
                    for row in cursor.fetchall()
                ],
            }
        )
    while cursor.nextset():
        result_index += 1
        if cursor.with_rows:
            datasets.append(
                {
                    "title": "{} {}".format(title_prefix, result_index),
                    "columns": list(cursor.column_names or ()),
                    "rows": [
                        [_normalize_modal_cell(value) for value in row]
                        for row in cursor.fetchall()
                    ],
                }
            )
    return datasets


def _fetch_supported_generation_llms():
    return get_generation_models()


def _normalize_text_value(value):
    normalized = _normalize_modal_cell(value)
    return "" if normalized == "" else str(normalized)


def _parse_json_value(value):
    if isinstance(value, (dict, list)):
        return value
    text = _normalize_text_value(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _format_json_value(value):
    parsed = _parse_json_value(value)
    if parsed is not None:
        return json.dumps(parsed, indent=2)
    return _normalize_text_value(value)


def _build_json_fields(value):
    parsed = _parse_json_value(value)
    if isinstance(parsed, dict):
        return [
            {"label": str(key), "value": _format_json_value(item_value)}
            for key, item_value in parsed.items()
        ]
    if isinstance(parsed, list):
        return [
            {"label": "Item {}".format(index + 1), "value": _format_json_value(item_value)}
            for index, item_value in enumerate(parsed)
        ]
    return []


def _normalize_multiline_text(value):
    return _normalize_text_value(value).replace("\r\n", "\n").replace("\r", "\n").strip()


def _build_json_array_table(title, items):
    normalized_items = items if isinstance(items, list) else []
    if not normalized_items:
        return {
            "title": title,
            "columns": [],
            "rows": [],
        }

    title_text = str(title or "").strip().lower()
    if (
        title_text == "chat_history"
        and all(isinstance(item, dict) for item in normalized_items)
        and all({"role", "content"}.issubset(set(item.keys())) for item in normalized_items)
    ):
        rows = []
        current_row = {"user message": "", "chat_bot_message": ""}
        has_values = False
        for item in normalized_items:
            role_value = str(item.get("role", "")).strip().lower()
            content_value = _format_json_value(item.get("content", ""))
            if role_value == "user":
                if has_values:
                    rows.append([current_row["user message"], current_row["chat_bot_message"]])
                    current_row = {"user message": "", "chat_bot_message": ""}
                    has_values = False
                current_row["user message"] = content_value
                has_values = True
            elif role_value == "assistant":
                current_row["chat_bot_message"] = content_value
                has_values = True
                rows.append([current_row["user message"], current_row["chat_bot_message"]])
                current_row = {"user message": "", "chat_bot_message": ""}
                has_values = False
            else:
                if has_values:
                    rows.append([current_row["user message"], current_row["chat_bot_message"]])
                rows.append([_format_json_value(item), ""])
                current_row = {"user message": "", "chat_bot_message": ""}
                has_values = False
        if has_values:
            rows.append([current_row["user message"], current_row["chat_bot_message"]])
        return {
            "title": title,
            "columns": ["user message", "chat_bot_message"],
            "rows": rows,
        }

    if all(isinstance(item, dict) for item in normalized_items):
        columns = []
        seen_columns = set()
        for item in normalized_items:
            for key in item.keys():
                key_text = str(key)
                if key_text not in seen_columns:
                    seen_columns.add(key_text)
                    columns.append(key_text)
        rows = []
        for item in normalized_items:
            rows.append([_format_json_value(item.get(column, "")) for column in columns])
        return {
            "title": title,
            "columns": columns,
            "rows": rows,
        }

    return {
        "title": title,
        "columns": ["Value"],
        "rows": [[_format_json_value(item)] for item in normalized_items],
    }


def _build_nl2ml_variable_result(title, value):
    raw_text = _normalize_text_value(value).strip()
    parsed = _parse_json_value(raw_text)
    response_text = ""
    fields = _build_json_fields(raw_text)
    array_tables = []
    if isinstance(parsed, dict) and isinstance(parsed.get("text"), str):
        response_text = _normalize_multiline_text(parsed.get("text"))
        fields = [
            {"label": str(key), "value": _format_json_value(item_value)}
            for key, item_value in parsed.items()
            if str(key) != "text" and not isinstance(item_value, list)
        ]
        for key, item_value in parsed.items():
            if isinstance(item_value, list):
                array_tables.append(_build_json_array_table(str(key), item_value))
    elif isinstance(parsed, dict):
        fields = [
            {"label": str(key), "value": _format_json_value(item_value)}
            for key, item_value in parsed.items()
            if not isinstance(item_value, list)
        ]
        for key, item_value in parsed.items():
            if isinstance(item_value, list):
                array_tables.append(_build_json_array_table(str(key), item_value))
    elif isinstance(parsed, list):
        fields = []
        array_tables.append(_build_json_array_table("Entries", parsed))
    return {
        "title": title,
        "raw_text": raw_text,
        "pretty_text": _format_json_value(raw_text) if raw_text else "",
        "response_text": response_text,
        "fields": fields,
        "array_tables": array_tables,
    }


def _build_nl2ml_generated_sql(question_text, model_id, keep_chat_history, prior_options_text=""):
    normalized_question = str(question_text or "").strip()
    if not normalized_question:
        raise ValueError("Enter an NL2ML prompt before generating SQL.")
    normalized_model_id = str(model_id or "").strip()
    if not normalized_model_id:
        raise ValueError("Choose an LLM before generating SQL.")

    statements = []
    prior_options = _normalize_text_value(prior_options_text).strip()
    if keep_chat_history and prior_options:
        statements.append(
            "SET @nl2ml_options = CAST({} AS JSON);".format(_mysql_quote(prior_options))
        )
        statements.append(
            "SET @nl2ml_options = JSON_SET(COALESCE(@nl2ml_options, JSON_OBJECT()), '$.model_id', {});".format(
                _mysql_quote(normalized_model_id)
            )
        )
    elif keep_chat_history:
        statements.append(
            "SET @nl2ml_options = JSON_SET(COALESCE(@nl2ml_options, JSON_OBJECT()), '$.model_id', {});".format(
                _mysql_quote(normalized_model_id)
            )
        )
    else:
        statements.append(
            'SET @nl2ml_options = JSON_OBJECT("model_id", {});'.format(
                _mysql_quote(normalized_model_id)
            )
        )

    statements.append(
        "CALL sys.NL2ML({}, {});".format(
            _mysql_quote(normalized_question),
            NL2ML_OUTPUT_VAR,
        )
    )
    return "\n".join(statements)


def _build_nl2ml_result_tabs(result_sets, output_variable=None, options_variable=None):
    tabs = []
    for index, dataset in enumerate(result_sets, start=1):
        tabs.append(
            {
                "id": "result-set-{}".format(index),
                "label": dataset.get("title") or "Result Set {}".format(index),
                "kind": "result_set",
                "dataset": dataset,
            }
        )
    if output_variable is not None:
        tabs.append(
            {
                "id": "output-variable",
                "label": output_variable["title"],
                "kind": "variable",
                "variable": output_variable,
            }
        )
    if options_variable is not None:
        tabs.append(
            {
                "id": "options-variable",
                "label": options_variable["title"],
                "kind": "variable",
                "variable": options_variable,
            }
        )
    return tabs


def _execute_nl2ml_sql(sql_text):
    normalized_sql = str(sql_text or "").strip()
    if not normalized_sql:
        raise ValueError("Generate or enter SQL before executing.")

    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        result_sets = []
        cursor.execute(normalized_sql, map_results=True)
        result_index = 1
        while True:
            if cursor.with_rows:
                result_sets.append(
                    {
                        "title": "Result Set {}".format(result_index),
                        "columns": list(cursor.column_names or ()),
                        "rows": [
                            [_normalize_modal_cell(value) for value in row]
                            for row in cursor.fetchall()
                        ],
                    }
                )
            result_index += 1
            if not cursor.nextset():
                break

        cursor.execute(
            "select cast(@output as char) as output_value, cast(@nl2ml_options as char) as options_value"
        )
        row = cursor.fetchone() or ("", "")
        output_value = row[0] if len(row) > 0 else ""
        options_value = row[1] if len(row) > 1 else ""
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "result_sets": result_sets,
            "output_variable": _build_nl2ml_variable_result(NL2ML_OUTPUT_VAR, output_value),
            "options_variable": _build_nl2ml_variable_result("@nl2ml_options", options_value),
            "options_raw_text": _normalize_text_value(options_value).strip(),
        }
    except (ValueError, mysql.connector.Error):
        raise
    finally:
        close_mysql_connection(cnx)


def _initialize_iris_database():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    ml_schema_name = _ml_schema_name()
    cleared_model_catalog = bool(ml_schema_name and _model_catalog_exists(ml_schema_name))
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute("drop database if exists ml_data")
        cursor.execute("create database ml_data")
        cursor.execute(
            """
            create table ml_data.iris_train (
                `my_row_id` bigint not null auto_increment invisible primary key,
                `sepal length` float default null,
                `sepal width` float default null,
                `petal length` float default null,
                `petal width` float default null,
                `class` varchar(16) default null
            )
            """
        )
        cursor.executemany(_iris_insert_sql("iris_train"), IRIS_TRAIN_ROWS)
        cursor.execute("create table ml_data.iris_test like ml_data.iris_train")
        cursor.executemany(_iris_insert_sql("iris_test"), IRIS_TEST_ROWS)
        cursor.execute("create table ml_data.iris_validate like ml_data.iris_test")
        cursor.execute(
            """
            insert into ml_data.iris_validate
                (`sepal length`, `sepal width`, `petal length`, `petal width`, `class`)
            select `sepal length`, `sepal width`, `petal length`, `petal width`, `class`
            from ml_data.iris_test
            """
        )
        for table_name in ("iris_train", "iris_test", "iris_validate"):
            cursor.execute(
                "alter table ml_data.{} modify column my_row_id bigint not null auto_increment".format(
                    _quote_identifier(table_name)
                )
            )
        cursor.execute("set @model = 'iris_model'")
        if cleared_model_catalog:
            cursor.execute(
                "delete from {}.MODEL_CATALOG where model_handle = 'iris_model'".format(
                    _quote_identifier(ml_schema_name)
                )
            )
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "train_rows": len(IRIS_TRAIN_ROWS),
            "test_rows": len(IRIS_TEST_ROWS),
            "validate_rows": len(IRIS_TEST_ROWS),
            "cleared_model_catalog": cleared_model_catalog,
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_train(optimization_metric=""):
    ml_schema_name = _ml_schema_name()
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    call_text = _build_iris_ml_train_call_text(optimization_metric)
    try:
        cnx = mysql_connection(_iris_ml_train_connection_config())
        cursor = cnx.cursor()
        cursor.execute("set @model = 'iris_model'")
        cursor.execute(call_text)
        procedure_datasets = _collect_pending_resultsets(cursor, title_prefix="ML_TRAIN Result")
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": call_text,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "procedure_datasets": procedure_datasets,
            "model_catalog": _fetch_model_catalog(ml_schema_name),
            "ml_schema_name": ml_schema_name,
        }
    except mysql.connector.Error as error:
        if getattr(error, "errno", None) in {2006, 2013}:
            raise RuntimeError(
                "The MySQL connection was lost while ML_TRAIN was running. "
                "The training job may still be processing on the server. Check the model catalog after a short wait."
            ) from error
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_model_load():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute(IRIS_ML_MODEL_LOAD_CALL_TEXT)
        _consume_pending_results(cursor)
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": IRIS_ML_MODEL_LOAD_CALL_TEXT,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_predict_row():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute(
            """
            set @row_input = JSON_OBJECT(
                "sepal length", 7.3,
                "sepal width", 2.9,
                "petal length", 6.3,
                "petal width", 1.8
            )
            """
        )
        cursor.execute("set @model = 'iris_model'")
        cursor.execute("select sys.ML_PREDICT_ROW(@row_input, @model, NULL) as prediction_value")
        rows = cursor.fetchall()
        prediction_value = rows[0][0] if rows else ""
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": IRIS_ML_PREDICT_ROW_CALL_TEXT,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "prediction_records": _build_prediction_records(prediction_value),
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_predict_table():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        if _table_exists("ml_data", "iris_predictions"):
            cursor.execute("drop table ml_data.iris_predictions")
        cursor.execute("set @model = 'iris_model'")
        cursor.execute("CALL sys.ML_PREDICT_TABLE('ml_data.iris_test', @model, 'ml_data.iris_predictions', NULL)")
        _consume_pending_results(cursor)
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": IRIS_ML_PREDICT_TABLE_CALL_TEXT,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "source_table": _fetch_named_table("ml_data", "iris_test"),
            "predictions_table": _fetch_named_table("ml_data", "iris_predictions"),
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_score():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute("set @iris_model = 'iris_model'")
        cursor.execute(IRIS_ML_SCORE_CALL_TEXT)
        _consume_pending_results(cursor)
        cursor.execute("select @score as score_value")
        rows = cursor.fetchall()
        score_value = rows[0][0] if rows else ""
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": IRIS_ML_SCORE_CALL_TEXT,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "score_records": _build_score_records(score_value),
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _execute_iris_ml_explain_table():
    started_at = datetime.now(timezone.utc)
    started_counter = time.perf_counter()
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        if _table_exists("ml_data", "iris_explanations"):
            cursor.execute("drop table ml_data.iris_explanations")
        cursor.execute("set @iris_model = 'iris_model'")
        cursor.execute(IRIS_ML_EXPLAIN_TABLE_CALL_TEXT)
        _consume_pending_results(cursor)
        cnx.commit()
        finished_at = datetime.now(timezone.utc)
        return {
            "call_text": IRIS_ML_EXPLAIN_TABLE_CALL_TEXT,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": time.perf_counter() - started_counter,
            "explanations_table": _fetch_named_table("ml_data", "iris_explanations"),
            "source_table": _fetch_named_table("ml_data", "iris_test"),
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


@app.route("/heatwave-ml", methods=["GET", "POST"])
@login_required
def heatwave_ml_page():
    active_tab = request.values.get("tab", "iris").strip().lower()
    if active_tab not in {"iris", "nl2ml"}:
        active_tab = "iris"

    current_action = ""
    init_result = None
    train_result = None
    action_result = None
    supported_generation_llms = []
    selected_nl2ml_model = str(request.values.get("nl2ml_model_id", "")).strip()
    keep_nl2ml_chat_history = str(request.values.get("nl2ml_keep_chat_history", "")).strip().lower() in {"1", "true", "yes", "on"}
    nl2ml_prompt = str(request.values.get("nl2ml_prompt", NL2ML_DEFAULT_PROMPT)).strip() or NL2ML_DEFAULT_PROMPT
    nl2ml_sql_text = str(request.values.get("nl2ml_sql_text", "")).strip()
    nl2ml_options_raw_text = str(request.values.get("nl2ml_options_raw_text", "")).strip()
    nl2ml_result_sets = []
    nl2ml_output_variable = None
    nl2ml_options_variable = None
    nl2ml_result_tabs = []
    train_result_tabs = []
    nl2ml_generation_result = None
    nl2ml_execution_result = None
    left_panel_table = {"columns": [], "rows": []}
    left_panel_title = "iris_train Content"
    left_panel_empty_text = "Initialize IrisDB to create and view `ml_data.iris_train`."
    model_catalog = {"columns": [], "rows": []}
    model_catalog_records = []
    prediction_records = []
    predictions_table = {"columns": [], "rows": []}
    explanations_table = {"columns": [], "rows": []}
    explanation_records = []
    score_records = []
    ml_call_text = ""
    ml_call_title = "ML Syntax"
    ml_schema_name = _ml_schema_name()
    iris_ready = False
    selected_classification_optimization_metric = _normalize_classification_optimization_metric(
        request.values.get("classification_optimization_metric", "")
    )

    try:
        supported_generation_llms = _fetch_supported_generation_llms()
        if selected_nl2ml_model not in supported_generation_llms:
            selected_nl2ml_model = supported_generation_llms[0] if supported_generation_llms else ""

        if request.method == "POST":
            current_action = request.form.get("heatwave_ml_action", "").strip()
            if active_tab == "iris":
                if current_action == "initialize_iris":
                    init_result = _initialize_iris_database()
                    flash("Schema `ml_data` and the Iris demo tables were initialized.", "success")
                elif current_action == "execute_ml_train":
                    selected_classification_optimization_metric = _normalize_classification_optimization_metric(
                        request.form.get("classification_optimization_metric", "")
                    )
                    ml_call_text = _build_iris_ml_train_call_text(selected_classification_optimization_metric)
                    ml_call_title = "ML_TRAIN Syntax"
                    if not _table_exists("ml_data", "iris_train"):
                        raise ValueError("Initialize IrisDB before running ML_TRAIN.")
                    train_result = _execute_iris_ml_train(selected_classification_optimization_metric)
                    action_result = train_result
                    train_result_tabs = [
                        {
                            "id": "ml-train-result-{}".format(index + 1),
                            "label": dataset.get("title") or "Result Set {}".format(index + 1),
                            "dataset": dataset,
                        }
                        for index, dataset in enumerate(train_result.get("procedure_datasets") or [])
                    ]
                    model_catalog = train_result["model_catalog"]
                    model_catalog_records = _build_model_catalog_records(model_catalog)
                    ml_schema_name = train_result["ml_schema_name"]
                    flash("ML_TRAIN finished for model `iris_model`.", "success")
                elif current_action == "execute_ml_model_load":
                    ml_call_text = IRIS_ML_MODEL_LOAD_CALL_TEXT
                    ml_call_title = "ML_MODEL_LOAD Syntax"
                    action_result = _execute_iris_ml_model_load()
                    flash("ML_MODEL_LOAD finished for model `iris_model`.", "success")
                elif current_action == "execute_ml_predict_row":
                    ml_call_text = IRIS_ML_PREDICT_ROW_CALL_TEXT
                    ml_call_title = "ML_PREDICT_ROW Syntax"
                    action_result = _execute_iris_ml_predict_row()
                    prediction_records = action_result["prediction_records"]
                    flash("ML_PREDICT_ROW finished for model `iris_model`.", "success")
                elif current_action == "execute_ml_predict_table":
                    ml_call_text = IRIS_ML_PREDICT_TABLE_CALL_TEXT
                    ml_call_title = "ML_PREDICT_TABLE Syntax"
                    left_panel_title = "iris_test Content"
                    left_panel_empty_text = "No rows returned from `ml_data.iris_test`."
                    if not _table_exists("ml_data", "iris_test"):
                        raise ValueError("Initialize IrisDB before running ML_PREDICT_TABLE.")
                    if _table_exists("ml_data", "iris_test"):
                        left_panel_table = _fetch_named_table("ml_data", "iris_test")
                    action_result = _execute_iris_ml_predict_table()
                    left_panel_table = action_result["source_table"]
                    left_panel_title = "iris_test Content"
                    left_panel_empty_text = "No rows returned from `ml_data.iris_test`."
                    predictions_table = action_result["predictions_table"]
                    flash("ML_PREDICT_TABLE finished for output table `ml_data.iris_predictions`.", "success")
                elif current_action == "execute_ml_score":
                    ml_call_text = IRIS_ML_SCORE_CALL_TEXT
                    ml_call_title = "ML_SCORE Syntax"
                    action_result = _execute_iris_ml_score()
                    score_records = action_result["score_records"]
                    flash("ML_SCORE finished for metric `balanced_accuracy`.", "success")
                elif current_action == "execute_ml_explain_table":
                    ml_call_text = IRIS_ML_EXPLAIN_TABLE_CALL_TEXT
                    ml_call_title = "ML_EXPLAIN_TABLE Syntax"
                    left_panel_title = "iris_test Content"
                    left_panel_empty_text = "No rows returned from `ml_data.iris_test`."
                    if not _table_exists("ml_data", "iris_test"):
                        raise ValueError("Initialize IrisDB before running ML_EXPLAIN_TABLE.")
                    left_panel_table = _fetch_named_table("ml_data", "iris_test")
                    action_result = _execute_iris_ml_explain_table()
                    left_panel_table = action_result["source_table"]
                    explanations_table = action_result["explanations_table"]
                    explanation_records = _build_table_records(explanations_table)
                    flash("ML_EXPLAIN_TABLE finished for output table `ml_data.iris_explanations`.", "success")
                elif current_action:
                    raise ValueError("Unsupported HeatWave ML action.")
            elif active_tab == "nl2ml":
                if current_action == "generate_nl2ml":
                    nl2ml_sql_text = _build_nl2ml_generated_sql(
                        nl2ml_prompt,
                        selected_nl2ml_model,
                        keep_nl2ml_chat_history,
                        nl2ml_options_raw_text,
                    )
                    nl2ml_generation_result = {"sql_text": nl2ml_sql_text}
                    flash("Generated NL2ML SQL. Review it before executing.", "success")
                elif current_action == "execute_nl2ml_sql":
                    nl2ml_execution_result = _execute_nl2ml_sql(nl2ml_sql_text)
                    nl2ml_result_sets = nl2ml_execution_result["result_sets"]
                    nl2ml_output_variable = nl2ml_execution_result["output_variable"]
                    nl2ml_options_variable = nl2ml_execution_result["options_variable"]
                    nl2ml_options_raw_text = nl2ml_execution_result["options_raw_text"]
                    nl2ml_result_tabs = _build_nl2ml_result_tabs(
                        nl2ml_result_sets,
                        nl2ml_output_variable,
                        nl2ml_options_variable,
                    )
                    flash("Executed NL2ML SQL.", "success")
                elif current_action:
                    raise ValueError("Unsupported NL2ML action.")

        if active_tab == "iris":
            iris_ready = _database_exists("ml_data") and _table_exists("ml_data", "iris_train")
            if iris_ready and not left_panel_table["columns"]:
                left_panel_table = _fetch_iris_train_table()
            if not model_catalog["columns"] and ml_schema_name:
                model_catalog = _fetch_model_catalog(ml_schema_name)
            if not model_catalog_records and model_catalog["columns"]:
                model_catalog_records = _build_model_catalog_records(model_catalog)
    except (ValueError, mysql.connector.Error) as error:
        flash(str(error), "error")
        try:
            if active_tab == "iris":
                iris_ready = _database_exists("ml_data") and _table_exists("ml_data", "iris_train")
                if current_action == "execute_ml_predict_table":
                    left_panel_title = "iris_test Content"
                    left_panel_empty_text = "No rows returned from `ml_data.iris_test`."
                    if _table_exists("ml_data", "iris_test"):
                        left_panel_table = _fetch_named_table("ml_data", "iris_test")
                    if _table_exists("ml_data", "iris_predictions"):
                        predictions_table = _fetch_named_table("ml_data", "iris_predictions")
                elif current_action == "execute_ml_explain_table":
                    left_panel_title = "iris_test Content"
                    left_panel_empty_text = "No rows returned from `ml_data.iris_test`."
                    if _table_exists("ml_data", "iris_test"):
                        left_panel_table = _fetch_named_table("ml_data", "iris_test")
                    if _table_exists("ml_data", "iris_explanations"):
                        explanations_table = _fetch_named_table("ml_data", "iris_explanations")
                        explanation_records = _build_table_records(explanations_table)
                elif iris_ready and not left_panel_table["columns"]:
                    left_panel_table = _fetch_iris_train_table()

                if not model_catalog["columns"] and ml_schema_name:
                    model_catalog = _fetch_model_catalog(ml_schema_name)
                if not model_catalog_records and model_catalog["columns"]:
                    model_catalog_records = _build_model_catalog_records(model_catalog)
        except mysql.connector.Error:
            pass

    return render_dashboard(
        "heatwave_ml.html",
        page_title="HeatWave ML",
        tabs=[{"id": "iris", "label": "Iris"}, {"id": "nl2ml", "label": "NL2ML"}],
        active_tab=active_tab,
        supported_generation_llms=supported_generation_llms,
        selected_nl2ml_model=selected_nl2ml_model,
        keep_nl2ml_chat_history=keep_nl2ml_chat_history,
        nl2ml_prompt=nl2ml_prompt,
        nl2ml_sql_text=nl2ml_sql_text,
        nl2ml_options_raw_text=nl2ml_options_raw_text,
        nl2ml_result_sets=nl2ml_result_sets,
        nl2ml_output_variable=nl2ml_output_variable,
        nl2ml_options_variable=nl2ml_options_variable,
        nl2ml_result_tabs=nl2ml_result_tabs,
        train_result_tabs=train_result_tabs,
        nl2ml_generation_result=nl2ml_generation_result,
        nl2ml_execution_result=nl2ml_execution_result,
        iris_ready=iris_ready,
        left_panel_table=left_panel_table,
        left_panel_title=left_panel_title,
        left_panel_empty_text=left_panel_empty_text,
        init_result=init_result,
        train_result=train_result,
        action_result=action_result,
        model_catalog=model_catalog,
        model_catalog_records=model_catalog_records,
        prediction_records=prediction_records,
        predictions_table=predictions_table,
        explanations_table=explanations_table,
        explanation_records=explanation_records,
        score_records=score_records,
        ml_schema_name=ml_schema_name,
        ml_call_text=ml_call_text,
        ml_call_title=ml_call_title,
        ml_train_call_text_default=_build_iris_ml_train_call_text(selected_classification_optimization_metric),
        classification_optimization_metrics=CLASSIFICATION_OPTIMIZATION_METRICS,
        selected_classification_optimization_metric=selected_classification_optimization_metric,
        ml_model_load_call_text_default=IRIS_ML_MODEL_LOAD_CALL_TEXT,
        ml_predict_row_call_text_default=IRIS_ML_PREDICT_ROW_CALL_TEXT,
        ml_predict_table_call_text_default=IRIS_ML_PREDICT_TABLE_CALL_TEXT,
        ml_score_call_text_default=IRIS_ML_SCORE_CALL_TEXT,
        ml_explain_table_call_text_default=IRIS_ML_EXPLAIN_TABLE_CALL_TEXT,
        docs_url=IRIS_DOCS_URL if active_tab == "iris" else NL2ML_DOCS_URL,
        current_user_name=str(session.get("db_user", "")).strip(),
    )
