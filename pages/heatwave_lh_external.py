import json
import importlib
import time
from datetime import datetime, timezone

import mysql.connector
from flask import flash, request

from app_context import (
    _mysql_quote,
    _quote_identifier,
    _table_exists,
    _validate_identifier,
    app,
    exec_sql,
    fetch_askme_config,
    get_oci_object_storage_client,
    login_required,
    render_dashboard,
    run_sql,
    run_sql_multi_resultsets,
    run_sql_with_columns,
    setup_askme_db,
)

HEATWAVE_LH_EXTERNAL_TABS = [
    {"id": "heatwave-load", "label": "HeatWave_load"},
    {"id": "incremental-refresh", "label": "Incremental Refresh"},
]
HEATWAVE_LH_EXTERNAL_FORMATS = ["avro", "json", "csv", "parquet", "delta"]
HEATWAVE_LOAD_MODES = ["normal", "dryrun", "validation"]
HEATWAVE_LOAD_OUTPUTS = ["normal", "compact", "silent", "help"]
HEATWAVE_LOAD_POLICIES = ["disable_unsupported_columns", "not_disable_unsupported_columns"]
HEATWAVE_LOAD_AUTO_ENC_MODES = ["off", "check"]
HEATWAVE_MATCH_COLUMNS_BY = ["order", "name_case_sensitive", "name_case_insensitive"]
HEATWAVE_COMPRESSION_OPTIONS = ["zip", "gzip", "bzip2", "auto"]
LAKEHOUSE_INCREMENTAL_LOAD_DOCS_URL = "https://dev.mysql.com/doc/heatwave/en/mys-hw-lakehouse-incremental-load.html"
LAKEHOUSE_TABLE_SYNTAX_DOCS_URL = "https://dev.mysql.com/doc/heatwave/en/mys-hw-lakehouse-table-syntax-sql.html"
AUTO_REFRESH_SOURCE_RE = r"AUTO_REFRESH_SOURCE\s*=\s*(?:'([^']*)'|(NONE))"


def _parse_optional_bool(raw_value):
    value = str(raw_value or "").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    return None


def _default_lh_external_form():
    return {
        "database_name": "",
        "table_name": "",
        "selected_object_folder": "",
        "new_object_folder": "",
        "upload_selected_object_folder": "",
        "upload_new_object_folder": "",
        "upload_use_new_folder": False,
        "oci_uri": "",
        "file_format": "csv",
        "has_header": True,
        "mode": "normal",
        "refresh_external_tables": False,
        "output": "",
        "sql_mode": "",
        "policy": "",
        "set_load_parallelism": "",
        "auto_enc_mode": "",
        "sampling": "",
        "match_columns_by": "",
        "allow_missing_columns": "",
        "is_strict_mode": "",
        "allow_missing_files": "",
        "skip_rows": "",
        "compression": "",
        "auto_refresh_source": "",
        "sql_text": "",
    }


def _load_lh_external_form(source):
    form = _default_lh_external_form()
    if not source:
        return form
    form["database_name"] = str(source.get("database_name", "")).strip()
    form["table_name"] = str(source.get("table_name", "")).strip()
    form["selected_object_folder"] = str(source.get("selected_object_folder", "")).strip().strip("/")
    form["new_object_folder"] = str(source.get("new_object_folder", "")).strip().strip("/")
    form["upload_selected_object_folder"] = str(source.get("upload_selected_object_folder", "")).strip().strip("/")
    form["upload_new_object_folder"] = str(source.get("upload_new_object_folder", "")).strip().strip("/")
    form["upload_use_new_folder"] = str(source.get("upload_use_new_folder", "")).strip().lower() in {"1", "true", "yes", "on"}
    form["oci_uri"] = str(source.get("oci_uri", "")).strip()
    file_format = str(source.get("file_format", form["file_format"])).strip().lower()
    form["file_format"] = file_format if file_format in HEATWAVE_LH_EXTERNAL_FORMATS else form["file_format"]
    mode = str(source.get("mode", form["mode"])).strip().lower()
    form["mode"] = mode if mode in HEATWAVE_LOAD_MODES else form["mode"]
    output = str(source.get("output", "")).strip().lower()
    form["output"] = output if output in HEATWAVE_LOAD_OUTPUTS else ""
    form["sql_mode"] = str(source.get("sql_mode", "")).strip()
    policy = str(source.get("policy", "")).strip()
    form["policy"] = policy if policy in HEATWAVE_LOAD_POLICIES else ""
    set_load_parallelism = _parse_optional_bool(source.get("set_load_parallelism", ""))
    form["set_load_parallelism"] = "" if set_load_parallelism is None else ("true" if set_load_parallelism else "false")
    auto_enc_mode = str(source.get("auto_enc_mode", "")).strip().lower()
    form["auto_enc_mode"] = auto_enc_mode if auto_enc_mode in HEATWAVE_LOAD_AUTO_ENC_MODES else ""
    refresh_external_tables = _parse_optional_bool(source.get("refresh_external_tables", ""))
    form["refresh_external_tables"] = bool(refresh_external_tables)
    sampling = _parse_optional_bool(source.get("sampling", ""))
    form["sampling"] = "" if sampling is None else ("true" if sampling else "false")
    match_columns_by = str(source.get("match_columns_by", "")).strip().lower()
    form["match_columns_by"] = match_columns_by if match_columns_by in HEATWAVE_MATCH_COLUMNS_BY else ""
    allow_missing_columns = _parse_optional_bool(source.get("allow_missing_columns", ""))
    form["allow_missing_columns"] = "" if allow_missing_columns is None else ("true" if allow_missing_columns else "false")
    is_strict_mode = _parse_optional_bool(source.get("is_strict_mode", ""))
    form["is_strict_mode"] = "" if is_strict_mode is None else ("true" if is_strict_mode else "false")
    allow_missing_files = _parse_optional_bool(source.get("allow_missing_files", ""))
    form["allow_missing_files"] = "" if allow_missing_files is None else ("true" if allow_missing_files else "false")
    skip_rows = str(source.get("skip_rows", "")).strip()
    form["skip_rows"] = skip_rows
    compression = str(source.get("compression", "")).strip().lower()
    form["compression"] = compression if compression in HEATWAVE_COMPRESSION_OPTIONS else ""
    form["auto_refresh_source"] = str(source.get("auto_refresh_source", "")).strip()
    form["sql_text"] = str(source.get("sql_text", "")).strip()
    has_header_value = source.get("has_header", "")
    form["has_header"] = str(has_header_value).strip().lower() in {"1", "true", "yes", "on"}
    return form


def _normalize_text(value):
    return str(value or "").strip()


def _fetch_object_storage_setup():
    try:
        setup_askme_db()
        config_values = fetch_askme_config()
    except mysql.connector.Error:
        return {}
    return {
        "region": _normalize_text(config_values.get("OCI_REGION")),
        "config_file": _normalize_text(config_values.get("OCI_CONFIG_FILE")),
        "config_profile": _normalize_text(config_values.get("OCI_CONFIG_PROFILE")),
        "bucket_name": _normalize_text(config_values.get("OCI_BUCKET_NAME")),
        "namespace_name": _normalize_text(config_values.get("OCI_NAMESPACE")),
        "base_folder": _normalize_text(config_values.get("OCI_BUCKET_FOLDER")).strip("/"),
    }


def _object_storage_setup_is_ready(config_values):
    return bool(
        _normalize_text(config_values.get("region"))
        and _normalize_text(config_values.get("bucket_name"))
        and _normalize_text(config_values.get("namespace_name"))
    )


def _import_oci():
    try:
        return importlib.import_module("oci")
    except Exception as error:
        raise RuntimeError("The OCI SDK is not available. Install the project requirements first.") from error


def _get_object_storage_client(config_values):
    return get_oci_object_storage_client(config_values, timeout=(30, 180))


def _build_object_storage_base_uri(config_values, folder_name=""):
    bucket_name = _normalize_text(config_values.get("bucket_name"))
    namespace_name = _normalize_text(config_values.get("namespace_name"))
    folder_value = _normalize_text(folder_name).strip("/")
    if not bucket_name or not namespace_name:
        return ""
    if folder_value:
        return "oci://{}@{}/{}/".format(bucket_name, namespace_name, folder_value)
    return "oci://{}@{}/".format(bucket_name, namespace_name)


def _list_object_storage_folders(config_values):
    if not _object_storage_setup_is_ready(config_values):
        return [], ""
    client = _get_object_storage_client(config_values)
    namespace_name = _normalize_text(config_values.get("namespace_name"))
    bucket_name = _normalize_text(config_values.get("bucket_name"))
    configured_prefix = _normalize_text(config_values.get("base_folder")).strip("/")
    object_names = []
    start_value = None
    try:
        while True:
            response = client.list_objects(
                namespace_name=namespace_name,
                bucket_name=bucket_name,
                start=start_value,
                fields="name",
            )
            object_names.extend(str(item.name or "") for item in getattr(response.data, "objects", []))
            start_value = getattr(response.data, "next_start_with", None)
            if not start_value:
                break
    except Exception as error:
        raise RuntimeError(
            "Listing object-storage folders failed. Verify the OCI Configuration values and OCI access."
        ) from error

    folder_names = set()
    if configured_prefix:
        folder_names.add(configured_prefix)
    for object_name in object_names:
        normalized_name = str(object_name or "").strip().strip("/")
        if "/" not in normalized_name:
            continue
        folder_names.add(normalized_name.rsplit("/", 1)[0])
    return sorted(folder_names), ""


def _resolve_selected_object_folder(form_state, config_values, folder_options):
    selected_folder = _normalize_text(form_state.get("selected_object_folder")).strip("/")
    if selected_folder:
        return selected_folder
    oci_uri = _normalize_text(form_state.get("oci_uri"))
    base_uri = _build_object_storage_base_uri(config_values, "")
    if base_uri and oci_uri.startswith(base_uri):
        uri_path = oci_uri[len(base_uri):].strip("/")
        ordered_options = sorted(folder_options, key=len, reverse=True)
        for folder_option in ordered_options:
            if uri_path == folder_option or uri_path.startswith(folder_option + "/"):
                return folder_option
    configured_prefix = _normalize_text(config_values.get("base_folder")).strip("/")
    if configured_prefix:
        return configured_prefix
    return folder_options[0] if folder_options else ""


def _apply_object_storage_defaults(form_state, config_values, folder_options):
    selected_folder = _resolve_selected_object_folder(form_state, config_values, folder_options)
    form_state["selected_object_folder"] = selected_folder
    if not _normalize_text(form_state.get("oci_uri")) and _object_storage_setup_is_ready(config_values):
        form_state["oci_uri"] = _build_object_storage_base_uri(config_values, selected_folder)


def _get_target_object_folder(form_state, config_values, *, for_upload=False):
    if for_upload:
        if form_state.get("upload_use_new_folder"):
            new_folder = _normalize_text(form_state.get("upload_new_object_folder")).strip("/")
            if new_folder:
                return new_folder
        selected_folder = _normalize_text(form_state.get("upload_selected_object_folder")).strip("/")
        if selected_folder:
            return selected_folder

    new_folder = _normalize_text(form_state.get("new_object_folder")).strip("/")
    if new_folder:
        return new_folder
    selected_folder = _normalize_text(form_state.get("selected_object_folder")).strip("/")
    if selected_folder:
        return selected_folder
    return _normalize_text(config_values.get("base_folder")).strip("/")


def _upload_object_storage_file(file_storage, config_values, folder_name):
    filename = _normalize_text(getattr(file_storage, "filename", ""))
    if not filename:
        raise ValueError("Choose a file to upload.")
    object_name = filename if not folder_name else "{}/{}".format(folder_name.rstrip("/"), filename)
    client = _get_object_storage_client(config_values)
    namespace_name = _normalize_text(config_values.get("namespace_name"))
    bucket_name = _normalize_text(config_values.get("bucket_name"))
    try:
        file_storage.stream.seek(0)
        client.put_object(
            namespace_name=namespace_name,
            bucket_name=bucket_name,
            object_name=object_name,
            put_object_body=file_storage.stream.read(),
        )
    except Exception as error:
        raise RuntimeError(
            "Uploading the file to OCI Object Storage failed. Verify the OCI Configuration values and OCI access."
        ) from error
    return object_name, _build_object_storage_base_uri(config_values, "") + object_name


def _fetch_target_databases():
    rows = run_sql(
        """
        select schema_name as schema_name_value
        from information_schema.schemata
        where schema_name not in ('information_schema', 'mysql', 'performance_schema', 'sys')
          and schema_name not like 'mysql\\_%'
        order by schema_name
        """,
        include_database=False,
    )
    return [row[0] for row in rows]


def _fetch_lakehouse_databases():
    rows = run_sql(
        """
        select distinct table_schema as table_schema_value
        from information_schema.tables
        where upper(engine) = 'LAKEHOUSE'
          and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
          and table_schema not like 'mysql\\_%'
        order by table_schema
        """,
        include_database=False,
    )
    return [row[0] for row in rows]


def _fetch_lakehouse_tables(database_name):
    if not database_name:
        return []
    rows = run_sql(
        """
        select
            table_name as table_name_value,
            engine as engine_value,
            coalesce(table_rows, 0) as table_rows_value,
            coalesce(create_options, '') as create_options_value,
            coalesce(table_comment, '') as table_comment_value
        from information_schema.tables
        where table_schema = %s
          and upper(engine) = 'LAKEHOUSE'
        order by table_name
        """,
        (database_name,),
        include_database=False,
    )
    return [
        {
            "table_name": row[0],
            "engine": row[1] or "-",
            "table_rows": row[2],
            "create_options": row[3] or "",
            "table_comment": row[4] or "",
        }
        for row in rows
    ]


def _extract_auto_refresh_source(create_sql):
    import re

    match = re.search(AUTO_REFRESH_SOURCE_RE, str(create_sql or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    if match.group(1):
        return match.group(1)
    return ""


def _show_create_table(database_name, table_name):
    result = run_sql_with_columns(
        "show create table {}.{}".format(
            _quote_identifier(database_name),
            _quote_identifier(table_name),
        ),
        include_database=False,
    )
    create_sql = ""
    if result["rows"] and len(result["rows"][0]) > 1:
        create_sql = str(result["rows"][0][1] or "")
    return {
        "columns": result["columns"],
        "rows": result["rows"],
        "create_sql": create_sql,
        "auto_refresh_source": _extract_auto_refresh_source(create_sql),
    }


def _build_incremental_refresh_sql(database_name, table_name):
    input_list = json.dumps(
        [
            {
                "db_name": database_name,
                "tables": [{"table_name": table_name}],
            }
        ],
        indent=2,
    )
    return (
        "SET @input_list = {input_json};\n"
        "SET @options = JSON_OBJECT('mode', 'normal', 'refresh_external_tables', TRUE);\n"
        "CALL sys.HEATWAVE_LOAD(CAST(@input_list AS JSON), @options);"
    ).format(input_json=_mysql_quote(input_list))


def _build_auto_refresh_source_sql(database_name, table_name, source_value):
    qualified_table = "{}.{}".format(_quote_identifier(database_name), _quote_identifier(table_name))
    if str(source_value or "").strip():
        return "ALTER TABLE {} AUTO_REFRESH_SOURCE = {};".format(
            qualified_table,
            _mysql_quote(source_value.strip()),
        )
    return "ALTER TABLE {} AUTO_REFRESH_SOURCE = NONE;".format(qualified_table)


def _build_heatwave_load_input_list(form_state):
    database_name = str(form_state.get("database_name", "")).strip()
    table_name = _validate_identifier(form_state.get("table_name", ""), "Table name")
    oci_uri = str(form_state.get("oci_uri", "")).strip()
    file_format = str(form_state.get("file_format", "")).strip().lower()
    auto_refresh_source = str(form_state.get("auto_refresh_source", "")).strip()
    sampling = _parse_optional_bool(form_state.get("sampling", ""))
    allow_missing_columns = _parse_optional_bool(form_state.get("allow_missing_columns", ""))
    is_strict_mode = _parse_optional_bool(form_state.get("is_strict_mode", ""))
    allow_missing_files = _parse_optional_bool(form_state.get("allow_missing_files", ""))
    match_columns_by = str(form_state.get("match_columns_by", "")).strip().lower()
    compression = str(form_state.get("compression", "")).strip().lower()
    skip_rows = str(form_state.get("skip_rows", "")).strip()

    if not database_name:
        raise ValueError("Database name is required.")
    if not oci_uri:
        raise ValueError("OCI URL is required.")
    if not oci_uri.lower().startswith("oci://"):
        raise ValueError("OCI URL must start with `oci://`.")
    if file_format not in HEATWAVE_LH_EXTERNAL_FORMATS:
        raise ValueError("Choose a supported format: avro, json, csv, parquet, or delta.")

    engine_attribute = {
        "dialect": {
            "format": file_format,
        },
        "file": [
            {
                "uri": oci_uri,
            }
        ],
    }
    if sampling is not None:
        engine_attribute["sampling"] = sampling
    if file_format == "csv":
        engine_attribute["dialect"]["has_header"] = bool(form_state.get("has_header"))
    if is_strict_mode is not None:
        engine_attribute["dialect"]["is_strict_mode"] = is_strict_mode
    if skip_rows:
        try:
            skip_rows_value = int(skip_rows)
        except ValueError as error:
            raise ValueError("Skip rows must be an integer.") from error
        if skip_rows_value < 0 or skip_rows_value > 20:
            raise ValueError("Skip rows must be between 0 and 20.")
        engine_attribute["dialect"]["skip_rows"] = skip_rows_value
    if compression:
        if file_format in {"avro", "parquet"} and compression != "auto":
            raise ValueError("Compression for avro and parquet must be `auto`.")
        engine_attribute["dialect"]["compression"] = compression
    if allow_missing_files is not None:
        engine_attribute["dialect"]["allow_missing_files"] = allow_missing_files
    if auto_refresh_source:
        engine_attribute["auto_refresh_event_source"] = auto_refresh_source
    if match_columns_by:
        engine_attribute["match_columns_by"] = match_columns_by
    if allow_missing_columns is not None:
        engine_attribute["allow_missing_columns"] = allow_missing_columns

    return [
        {
            "db_name": database_name,
            "tables": [
                {
                    "table_name": table_name,
                    "engine_attribute": engine_attribute,
                }
            ],
        }
    ]


def _build_heatwave_load_options(form_state):
    options = {
        "mode": str(form_state.get("mode", "normal")).strip().lower() or "normal",
        "refresh_external_tables": bool(_parse_optional_bool(form_state.get("refresh_external_tables", ""))),
    }
    output = str(form_state.get("output", "")).strip().lower()
    sql_mode = str(form_state.get("sql_mode", "")).strip()
    policy = str(form_state.get("policy", "")).strip()
    set_load_parallelism = _parse_optional_bool(form_state.get("set_load_parallelism", ""))
    auto_enc_mode = str(form_state.get("auto_enc_mode", "")).strip().lower()

    if output:
        options["output"] = output
    if sql_mode:
        options["sql_mode"] = sql_mode
    if policy:
        options["policy"] = policy
    if set_load_parallelism is not None:
        options["set_load_parallelism"] = set_load_parallelism
    if auto_enc_mode:
        options["auto_enc"] = {"mode": auto_enc_mode}
    return options


def _build_heatwave_load_sql(form_state):
    input_list = _build_heatwave_load_input_list(form_state)
    options = _build_heatwave_load_options(form_state)
    input_json_text = json.dumps(input_list, indent=2)
    options_json_text = json.dumps(options, indent=2)
    return (
        "SET @input_list = {input_json_text};\n"
        "SET @options = {options_json_text};\n"
        "CALL sys.HEATWAVE_LOAD(CAST(@input_list AS JSON), CAST(@options AS JSON));"
    ).format(
        input_json_text=_mysql_quote(input_json_text),
        options_json_text=_mysql_quote(options_json_text),
    )


def _validate_lh_external_form(form_state, database_names):
    database_name = str(form_state.get("database_name", "")).strip()
    if database_name not in database_names:
        raise ValueError("Choose a database from the dropdown list.")
    table_name = _validate_identifier(form_state.get("table_name", ""), "Table name")
    if _table_exists(database_name, table_name):
        raise ValueError("Table `{}` already exists in database `{}`.".format(table_name, database_name))
    return table_name


@app.route("/heatwave-lh-external", methods=["GET", "POST"])
@login_required
def heatwave_lh_external_page():
    active_tab = request.values.get("tab", "heatwave-load").strip().lower()
    if active_tab not in {tab["id"] for tab in HEATWAVE_LH_EXTERNAL_TABS}:
        active_tab = "heatwave-load"

    database_names = []
    lakehouse_databases = []
    lakehouse_tables = []
    selected_refresh_db = ""
    selected_refresh_table = ""
    selected_refresh_source = ""
    selected_table_definition = {"columns": [], "rows": [], "create_sql": "", "auto_refresh_source": ""}
    open_definition_modal = False
    incremental_result_sets = []
    incremental_action_sql = ""
    incremental_execution_timing = None
    result_sets = []
    execution_timing = None
    form_state = _load_lh_external_form(request.form if request.method == "POST" else request.args)
    object_storage_setup = {}
    object_folder_options = []
    object_storage_error = ""
    object_folders_loaded = False

    try:
        object_storage_setup = _fetch_object_storage_setup()
        heatwave_load_action = ""
        if request.method == "POST" and active_tab == "heatwave-load":
            heatwave_load_action = request.form.get("lh_external_action", "").strip().lower()
        if _object_storage_setup_is_ready(object_storage_setup) and heatwave_load_action == "load_folders":
            try:
                object_folder_options, _unused = _list_object_storage_folders(object_storage_setup)
                object_folders_loaded = True
            except RuntimeError as error:
                object_storage_error = str(error)
                flash(object_storage_error, "warning")
        _apply_object_storage_defaults(form_state, object_storage_setup, object_folder_options)
        if form_state["selected_object_folder"] and form_state["selected_object_folder"] not in object_folder_options:
            object_folder_options = [form_state["selected_object_folder"]] + object_folder_options

        database_names = _fetch_target_databases()
        lakehouse_databases = _fetch_lakehouse_databases()
        if not form_state["database_name"] and database_names:
            form_state["database_name"] = database_names[0]

        selected_refresh_db = str(request.values.get("refresh_db", "")).strip()
        if not selected_refresh_db and lakehouse_databases:
            selected_refresh_db = lakehouse_databases[0]
        if selected_refresh_db and selected_refresh_db in lakehouse_databases:
            lakehouse_tables = _fetch_lakehouse_tables(selected_refresh_db)

        selected_refresh_table = str(request.values.get("refresh_table", "")).strip()
        available_table_names = {row["table_name"] for row in lakehouse_tables}
        if not selected_refresh_table and lakehouse_tables:
            selected_refresh_table = lakehouse_tables[0]["table_name"]
        if selected_refresh_table and selected_refresh_table not in available_table_names:
            selected_refresh_table = ""

        if request.method == "POST":
            if active_tab == "heatwave-load":
                action = heatwave_load_action
                if action == "load_folders":
                    if object_storage_error:
                        pass
                    elif not _object_storage_setup_is_ready(object_storage_setup):
                        flash("Configure Admin > OCI Configuration before loading bucket folders.", "warning")
                    elif object_folder_options:
                        flash("Loaded object storage folders.", "info")
                    else:
                        flash("No object folders were found in the configured bucket path.", "info")
                elif action == "upload_file":
                    if not _object_storage_setup_is_ready(object_storage_setup):
                        flash("Configure Admin > OCI Configuration before uploading files.", "warning")
                    else:
                        target_folder = _get_target_object_folder(form_state, object_storage_setup, for_upload=True)
                        uploaded_file = request.files.get("object_file")
                        object_name, uploaded_uri = _upload_object_storage_file(
                            uploaded_file,
                            object_storage_setup,
                            target_folder,
                        )
                        if target_folder and target_folder not in object_folder_options:
                            object_folder_options = [target_folder] + object_folder_options
                        form_state["selected_object_folder"] = target_folder
                        form_state["upload_selected_object_folder"] = target_folder
                        form_state["upload_use_new_folder"] = False
                        form_state["upload_new_object_folder"] = ""
                        form_state["new_object_folder"] = ""
                        form_state["oci_uri"] = uploaded_uri
                        object_folders_loaded = True
                        flash("Uploaded `{}` to OCI Object Storage.".format(object_name), "success")
                else:
                    _validate_lh_external_form(form_state, database_names)
                    generated_sql = _build_heatwave_load_sql(form_state)
                    form_state["sql_text"] = generated_sql

                if action == "execute_sql":
                    sql_text = request.form.get("sql_text", "").strip() or generated_sql
                    started_at = datetime.now(timezone.utc)
                    started_counter = time.perf_counter()
                    result_sets = run_sql_multi_resultsets(
                        sql_text,
                        include_database=False,
                        autocommit=True,
                    )
                    finished_at = datetime.now(timezone.utc)
                    finished_counter = time.perf_counter()
                    execution_timing = {
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "elapsed_seconds": (finished_at - started_at).total_seconds(),
                        "elapsed_perf_seconds": finished_counter - started_counter,
                    }
                    flash("HeatWave load executed.", "success")
                elif action == "generate_sql":
                    flash("HeatWave load SQL generated.", "info")
            elif active_tab == "incremental-refresh":
                selected_refresh_db = str(request.form.get("refresh_db", "")).strip()
                if selected_refresh_db not in lakehouse_databases:
                    raise ValueError("Choose a Lakehouse database from the left panel.")
                lakehouse_tables = _fetch_lakehouse_tables(selected_refresh_db)
                available_table_names = {row["table_name"] for row in lakehouse_tables}
                selected_refresh_table = _validate_identifier(request.form.get("refresh_table", ""), "Table name")
                if selected_refresh_table not in available_table_names:
                    raise ValueError("Choose a Lakehouse table from the selected database.")

                incremental_action = request.form.get("incremental_action", "").strip().lower()
                if incremental_action == "refresh_table":
                    incremental_action_sql = _build_incremental_refresh_sql(selected_refresh_db, selected_refresh_table)
                    flash("Incremental refresh SQL generated.", "info")
                elif incremental_action == "execute_refresh":
                    incremental_action_sql = (
                        str(request.form.get("incremental_sql", "")).strip()
                        or _build_incremental_refresh_sql(selected_refresh_db, selected_refresh_table)
                    )
                    started_at = datetime.now(timezone.utc)
                    started_counter = time.perf_counter()
                    incremental_result_sets = run_sql_multi_resultsets(
                        incremental_action_sql,
                        include_database=False,
                        autocommit=True,
                    )
                    finished_at = datetime.now(timezone.utc)
                    finished_counter = time.perf_counter()
                    incremental_execution_timing = {
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "elapsed_seconds": (finished_at - started_at).total_seconds(),
                        "elapsed_perf_seconds": finished_counter - started_counter,
                    }
                    flash("Incremental refresh executed.", "success")
                elif incremental_action == "update_refresh_source":
                    selected_refresh_source = str(request.form.get("new_auto_refresh_source", "")).strip()
                    incremental_action_sql = _build_auto_refresh_source_sql(
                        selected_refresh_db,
                        selected_refresh_table,
                        selected_refresh_source,
                    )
                    exec_sql(incremental_action_sql, include_database=False, autocommit=True)
                    flash("AUTO_REFRESH_SOURCE updated.", "success")
                    open_definition_modal = True
                elif incremental_action == "show_definition":
                    flash("Table definition loaded.", "info")
                    open_definition_modal = True

                if incremental_action in {"show_definition", "update_refresh_source"}:
                    selected_table_definition = _show_create_table(selected_refresh_db, selected_refresh_table)
                    selected_refresh_source = selected_table_definition["auto_refresh_source"]
    except (ValueError, mysql.connector.Error) as error:
        flash(str(error), "error")

    return render_dashboard(
        "heatwave_lh_external.html",
        page_title="HeatWave LH/External Table",
        active_tab=active_tab,
        tabs=HEATWAVE_LH_EXTERNAL_TABS,
        docs_url_incremental=LAKEHOUSE_INCREMENTAL_LOAD_DOCS_URL,
        docs_url_syntax=LAKEHOUSE_TABLE_SYNTAX_DOCS_URL,
        supported_formats=HEATWAVE_LH_EXTERNAL_FORMATS,
        load_modes=HEATWAVE_LOAD_MODES,
        load_outputs=HEATWAVE_LOAD_OUTPUTS,
        load_policies=HEATWAVE_LOAD_POLICIES,
        auto_enc_modes=HEATWAVE_LOAD_AUTO_ENC_MODES,
        match_columns_by_values=HEATWAVE_MATCH_COLUMNS_BY,
        compression_values=HEATWAVE_COMPRESSION_OPTIONS,
        database_names=database_names,
        form_state=form_state,
        object_storage_setup=object_storage_setup,
        object_folder_options=object_folder_options,
        object_folders_loaded=object_folders_loaded,
        object_storage_setup_ready=_object_storage_setup_is_ready(object_storage_setup),
        object_storage_error=object_storage_error,
        result_sets=result_sets,
        execution_timing=execution_timing,
        lakehouse_databases=lakehouse_databases,
        lakehouse_tables=lakehouse_tables,
        selected_refresh_db=selected_refresh_db,
        selected_refresh_table=selected_refresh_table,
        selected_refresh_source=selected_refresh_source,
        selected_table_definition=selected_table_definition,
        open_definition_modal=open_definition_modal,
        incremental_result_sets=incremental_result_sets,
        incremental_action_sql=incremental_action_sql,
        incremental_execution_timing=incremental_execution_timing,
    )
