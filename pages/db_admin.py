import mysql.connector
from flask import flash, redirect, request, url_for

from app_context import (
    DB_ADMIN_TABS,
    MYSQL_TYPE_OPTIONS,
    SYSTEM_DATABASES,
    _build_csv_response,
    _build_db_admin_download_payload,
    _database_exists,
    _default_column_form,
    _default_table_form,
    _is_system_database,
    _normalize_db_admin_tab,
    _pop_db_admin_modal_result,
    _queue_db_admin_modal_result,
    add_table_column,
    app,
    collect_table_column_definitions,
    create_database,
    create_table,
    drop_database,
    drop_table,
    fetch_database_inventory,
    fetch_heatwave_ml_queries,
    fetch_heatwave_ml_current_running_detail,
    fetch_heatwave_performance_queries,
    fetch_heatwave_table_load_recovery,
    fetch_heatwave_tables_report,
    fetch_table_browse_page,
    fetch_table_definition,
    fetch_tables_for_database,
    load_database_to_heatwave,
    load_table_to_heatwave,
    login_required,
    modify_table_column,
    render_dashboard,
    unload_database_from_heatwave,
    unload_table_from_heatwave,
)


def _parse_checkbox_value(raw_value):
    return str(raw_value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_monitor_view(raw_value):
    value = str(raw_value or "heatwave-performance-query").strip().lower()
    if value not in {"heatwave-performance-query", "heatwave-ml-query", "hw-table-load-recovery"}:
        return "heatwave-performance-query"
    return value


def _normalize_monitor_refresh(raw_value):
    value = str(raw_value or "none").strip().lower()
    return value if value in {"2", "5", "30", "60", "none"} else "none"


def _selected_database_names(form):
    return [str(value or "").strip() for value in form.getlist("database_name") if str(value or "").strip()]


def _ensure_bulk_database_selection(database_names, action_label):
    if not database_names:
        raise ValueError(f"Select at least one database before choosing {action_label}.")
    protected_names = [name for name in database_names if _is_system_database(name)]
    if protected_names:
        raise ValueError("System schemas cannot be modified: {}.".format(", ".join(protected_names)))
    return database_names


def _selected_table_names(form):
    table_names = []
    for value in form.getlist("table_name"):
        table_name = str(value or "").strip()
        if table_name and table_name not in table_names:
            table_names.append(table_name)
    return table_names


def _ensure_bulk_table_selection(database_name, table_names, action_label):
    if not database_name:
        raise ValueError(f"Choose a database before choosing {action_label}.")
    if _is_system_database(database_name):
        raise ValueError("Tables in system schemas cannot be modified from this screen.")
    if not table_names:
        raise ValueError(f"Select at least one table before choosing {action_label}.")
    return table_names


def _selected_heatwave_table_refs(form):
    refs = []
    for value in form.getlist("hw_table_ref"):
        raw_value = str(value or "").strip()
        if not raw_value:
            continue
        if "||" in raw_value:
            schema_name, table_name = [part.strip() for part in raw_value.split("||", 1)]
        elif "\t" in raw_value:
            schema_name, table_name = [part.strip() for part in raw_value.split("\t", 1)]
        else:
            continue
        if schema_name and table_name and (schema_name, table_name) not in refs:
            refs.append((schema_name, table_name))
    if refs:
        return refs

    schema_name = str(form.get("database", "") or "").strip()
    table_name = str(form.get("table_name", "") or "").strip()
    return [(schema_name, table_name)] if schema_name and table_name else []


def _ensure_heatwave_table_selection(table_refs):
    if not table_refs:
        raise ValueError("Select at least one HeatWave table before choosing Unload.")
    protected_refs = [f"{schema_name}.{table_name}" for schema_name, table_name in table_refs if _is_system_database(schema_name)]
    if protected_refs:
        raise ValueError("Tables in system schemas cannot be modified from this screen: {}.".format(", ".join(protected_refs)))
    return table_refs


@app.route("/db-admin/download", methods=["GET"])
@login_required
def db_admin_download():
    raw_tab = str(request.args.get("tab", "db")).strip().lower()
    active_tab = _normalize_db_admin_tab(raw_tab)
    monitor_view = _normalize_monitor_view(request.args.get("monitor_view", raw_tab))
    selected_database = str(request.args.get("database", "")).strip()
    current_ml_connection_only = _parse_checkbox_value(request.args.get("current_ml_connection_only", ""))
    try:
        filename, columns, rows = _build_db_admin_download_payload(
            active_tab,
            selected_database,
            monitor_view=monitor_view,
            current_ml_connection_only=current_ml_connection_only,
        )
        return _build_csv_response(filename, columns, rows)
    except (ValueError, mysql.connector.Error) as error:
        flash(str(error), "error")
        return redirect(
            url_for(
                "db_admin_page",
                tab=active_tab,
                database=selected_database,
                monitor_view=monitor_view,
                current_ml_connection_only="1" if current_ml_connection_only else "0",
            )
        )


@app.route("/db-admin", methods=["GET", "POST"])
@login_required
def db_admin_page():
    raw_requested_tab = str(request.values.get("tab", "db")).strip().lower()
    active_tab = _normalize_db_admin_tab(raw_requested_tab)
    requested_database = str(request.values.get("database", "")).strip()
    requested_table = str(request.args.get("table", "")).strip()
    requested_browse_table = str(request.args.get("browse_table", "")).strip()
    requested_browse_page = str(request.args.get("browse_page", "1")).strip()
    requested_edit_column = str(request.values.get("edit_column", "")).strip()
    selected_monitor_view = _normalize_monitor_view(request.values.get("monitor_view", raw_requested_tab))
    selected_monitor_refresh = _normalize_monitor_refresh(request.values.get("monitor_refresh", "none"))
    current_ml_connection_only = _parse_checkbox_value(request.values.get("current_ml_connection_only", ""))
    table_form = _default_table_form()
    add_column_form = _default_column_form()
    modify_column_form = None
    db_admin_modal_result = None

    if request.method == "POST":
        action = str(request.form.get("db_admin_action", "")).strip()
        raw_post_tab = str(request.form.get("tab", active_tab)).strip().lower()
        active_tab = _normalize_db_admin_tab(raw_post_tab)
        requested_database = str(request.form.get("database", requested_database)).strip()
        requested_table = str(request.form.get("table_name", requested_table)).strip()
        requested_edit_column = str(request.form.get("original_column_name", requested_edit_column)).strip()
        selected_monitor_view = _normalize_monitor_view(request.form.get("monitor_view", raw_post_tab or selected_monitor_view))
        selected_monitor_refresh = _normalize_monitor_refresh(request.form.get("monitor_refresh", selected_monitor_refresh))

        if action == "create_database":
            database_name = str(request.form.get("database_name", "")).strip()
            try:
                created_database = create_database(database_name)
                flash(f"Database `{created_database}` created.", "success")
                return redirect(url_for("db_admin_page", tab="db", database=created_database))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")

        elif action == "delete_database":
            try:
                database_names = _ensure_bulk_database_selection(_selected_database_names(request.form), "Delete")
                deleted_databases = [drop_database(database_name) for database_name in database_names]
                flash("Deleted database{}: {}.".format(
                    "" if len(deleted_databases) == 1 else "s",
                    ", ".join(f"`{database_name}`" for database_name in deleted_databases),
                ), "success")
                return redirect(url_for("db_admin_page", tab="db"))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")

        elif action == "load_database_heatwave":
            try:
                database_names = _ensure_bulk_database_selection(_selected_database_names(request.form), "Load to HeatWave")
                loaded_databases = [load_database_to_heatwave(database_name) for database_name in database_names]
                datasets = []
                for loaded_database in loaded_databases:
                    datasets.extend(loaded_database["datasets"])
                _queue_db_admin_modal_result(
                    "HeatWave Load Result",
                    datasets,
                )
                flash("HeatWave load requested for database{}: {}.".format(
                    "" if len(loaded_databases) == 1 else "s",
                    ", ".join(f"`{row['database_name']}`" for row in loaded_databases),
                ), "success")
                return redirect(url_for("db_admin_page", tab="db", database=loaded_databases[0]["database_name"]))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")

        elif action == "unload_database_heatwave":
            try:
                database_names = _ensure_bulk_database_selection(_selected_database_names(request.form), "Unload from HeatWave")
                unloaded_databases = [unload_database_from_heatwave(database_name) for database_name in database_names]
                datasets = []
                for unloaded_database in unloaded_databases:
                    datasets.extend(unloaded_database["datasets"])
                _queue_db_admin_modal_result(
                    "HeatWave Unload Result",
                    datasets,
                )
                flash("HeatWave unload requested for database{}: {}.".format(
                    "" if len(unloaded_databases) == 1 else "s",
                    ", ".join(f"`{row['database_name']}`" for row in unloaded_databases),
                ), "success")
                return redirect(url_for("db_admin_page", tab="db", database=unloaded_databases[0]["database_name"]))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")

        elif action == "unload_heatwave_hw_tables":
            try:
                table_refs = _ensure_heatwave_table_selection(_selected_heatwave_table_refs(request.form))
                unloaded_refs = []
                for schema_name, table_name in table_refs:
                    unloaded_table = unload_table_from_heatwave(schema_name, table_name)
                    unloaded_refs.append(f"{schema_name}.{unloaded_table}")
                flash("HeatWave unload requested for table{}: {}.".format(
                    "" if len(unloaded_refs) == 1 else "s",
                    ", ".join(f"`{table_ref}`" for table_ref in unloaded_refs),
                ), "success")
                return redirect(url_for("db_admin_page", tab="hw-tables", database=table_refs[0][0]))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "hw-tables"

        elif action == "create_table":
            table_form = {
                "table_name": str(request.form.get("table_name", "")).strip(),
                "columns": [],
            }
            raw_names = request.form.getlist("column_name")
            raw_type_names = request.form.getlist("column_type_name")
            raw_type_params = request.form.getlist("column_type_params")
            raw_nullable = request.form.getlist("column_nullable")
            primary_indexes = set(request.form.getlist("column_primary"))
            row_count = max(len(raw_names), len(raw_type_names), len(raw_type_params), len(raw_nullable), 1)
            for index in range(row_count):
                table_form["columns"].append(
                    {
                        "name": str(raw_names[index] if index < len(raw_names) else "").strip(),
                        "type_name": str(raw_type_names[index] if index < len(raw_type_names) else "VARCHAR").strip().upper() or "VARCHAR",
                        "type_params": str(raw_type_params[index] if index < len(raw_type_params) else "").strip(),
                        "nullable": str(raw_nullable[index] if index < len(raw_nullable) else "yes").strip().lower() != "no",
                        "primary": str(index) in primary_indexes,
                    }
                )
            try:
                if not requested_database:
                    raise ValueError("Choose a database before creating a table.")
                new_table = create_table(
                    requested_database,
                    table_form["table_name"],
                    collect_table_column_definitions(request.form),
                )
                flash(f"Table `{new_table}` created in `{requested_database}`.", "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database, table=new_table))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

        elif action == "add_column":
            add_column_form = {
                "original_name": "",
                "name": str(request.form.get("column_name", "")).strip(),
                "type_name": str(request.form.get("column_type_name", "VARCHAR")).strip().upper() or "VARCHAR",
                "type_params": str(request.form.get("column_type_params", "")).strip(),
                "nullable": str(request.form.get("column_nullable", "yes")).strip().lower() != "no",
            }
            try:
                if not requested_database or not requested_table:
                    raise ValueError("Choose a table before adding a column.")
                new_column = add_table_column(requested_database, requested_table, add_column_form)
                flash(f"Column `{new_column}` added to `{requested_database}.{requested_table}`.", "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database, table=requested_table))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

        elif action == "modify_column":
            modify_column_form = {
                "original_name": str(request.form.get("original_column_name", "")).strip(),
                "name": str(request.form.get("column_name", "")).strip(),
                "type_name": str(request.form.get("column_type_name", "VARCHAR")).strip().upper() or "VARCHAR",
                "type_params": str(request.form.get("column_type_params", "")).strip(),
                "nullable": str(request.form.get("column_nullable", "yes")).strip().lower() != "no",
            }
            requested_edit_column = modify_column_form["original_name"] or requested_edit_column
            try:
                if not requested_database or not requested_table:
                    raise ValueError("Choose a table before modifying a column.")
                updated_column = modify_table_column(
                    requested_database,
                    requested_table,
                    modify_column_form["original_name"],
                    modify_column_form,
                )
                flash(f"Column `{updated_column}` updated on `{requested_database}.{requested_table}`.", "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database, table=requested_table))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

        elif action == "delete_table":
            try:
                table_names = _ensure_bulk_table_selection(
                    requested_database,
                    _selected_table_names(request.form),
                    "Delete",
                )
                deleted_tables = [drop_table(requested_database, table_name) for table_name in table_names]
                flash("Deleted table{} from `{}`: {}.".format(
                    "" if len(deleted_tables) == 1 else "s",
                    requested_database,
                    ", ".join(f"`{table_name}`" for table_name in deleted_tables),
                ), "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

        elif action == "load_heatwave":
            try:
                table_names = _ensure_bulk_table_selection(
                    requested_database,
                    _selected_table_names(request.form),
                    "Load HeatWave",
                )
                table_inventory = fetch_tables_for_database(requested_database)
                secondary_engine_by_table = {
                    row["table_name"]: str(row.get("secondary_engine", "") or "").strip()
                    for row in table_inventory
                }
                loaded_tables = [
                    load_table_to_heatwave(
                        requested_database,
                        table_name,
                        secondary_engine_by_table.get(table_name, ""),
                    )
                    for table_name in table_names
                ]
                flash("HeatWave load requested for table{} in `{}`: {}.".format(
                    "" if len(loaded_tables) == 1 else "s",
                    requested_database,
                    ", ".join(f"`{table_name}`" for table_name in loaded_tables),
                ), "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

        elif action == "unload_heatwave":
            try:
                table_names = _ensure_bulk_table_selection(
                    requested_database,
                    _selected_table_names(request.form),
                    "Unload HeatWave",
                )
                unloaded_tables = [unload_table_from_heatwave(requested_database, table_name) for table_name in table_names]
                flash("HeatWave unload requested for table{} in `{}`: {}.".format(
                    "" if len(unloaded_tables) == 1 else "s",
                    requested_database,
                    ", ".join(f"`{table_name}`" for table_name in unloaded_tables),
                ), "success")
                return redirect(url_for("db_admin_page", tab="table", database=requested_database))
            except (ValueError, mysql.connector.Error) as error:
                flash(str(error), "error")
                active_tab = "table"

    database_inventory = []
    selected_database = requested_database
    tables = []
    selected_table_definition = []
    selected_column_definition = None
    browse_table_data = None
    heatwave_tables_stats = {"columns": [], "rows": [], "row_classes": [], "row_actions": []}
    heatwave_query_stats = {"columns": [], "rows": []}
    heatwave_ml_query_stats = {"columns": [], "rows": []}
    heatwave_ml_current_detail_stats = {"columns": [], "rows": []}
    heatwave_table_load_recovery_stats = {"columns": [], "rows": []}
    if request.method == "GET":
        db_admin_modal_result = _pop_db_admin_modal_result()

    try:
        database_inventory = fetch_database_inventory()
        available_database_names = [row["database_name"] for row in database_inventory]
        non_system_database_names = [row["database_name"] for row in database_inventory if not row["is_system"]]
        if selected_database not in available_database_names:
            selected_database = non_system_database_names[0] if non_system_database_names else (available_database_names[0] if available_database_names else "")

        if active_tab == "hw-tables":
            heatwave_tables_stats = fetch_heatwave_tables_report()
        elif active_tab == "monitoring":
            if selected_monitor_view == "heatwave-performance-query":
                heatwave_query_stats = fetch_heatwave_performance_queries()
            elif selected_monitor_view == "heatwave-ml-query":
                heatwave_ml_query_stats = fetch_heatwave_ml_queries(current_ml_connection_only=current_ml_connection_only)
                if current_ml_connection_only:
                    heatwave_ml_current_detail_stats = fetch_heatwave_ml_current_running_detail()
            elif selected_monitor_view == "hw-table-load-recovery":
                heatwave_table_load_recovery_stats = fetch_heatwave_table_load_recovery()
        elif selected_database and _database_exists(selected_database):
            tables = fetch_tables_for_database(selected_database)
            available_table_names = [row["table_name"] for row in tables]
            if requested_browse_table:
                if requested_browse_table in available_table_names:
                    browse_table_data = fetch_table_browse_page(selected_database, requested_browse_table, requested_browse_page)
                else:
                    flash(f"Table `{requested_browse_table}` was not found in `{selected_database}`.", "warning")
                    requested_browse_table = ""
            if requested_table and requested_table in available_table_names:
                selected_table_definition = fetch_table_definition(selected_database, requested_table)
                if requested_edit_column:
                    selected_column_definition = next(
                        (row for row in selected_table_definition if row["column_name"] == requested_edit_column),
                        None,
                    )
                    if selected_column_definition and not modify_column_form:
                        modify_column_form = {
                            "original_name": selected_column_definition["column_name"],
                            "name": selected_column_definition["column_name"],
                            "type_name": selected_column_definition["type_name"],
                            "type_params": selected_column_definition["type_params"],
                            "nullable": selected_column_definition["is_nullable"] == "YES",
                        }
                    elif requested_edit_column and not selected_column_definition:
                        flash(f"Column `{requested_edit_column}` was not found in `{selected_database}.{requested_table}`.", "warning")
                        requested_edit_column = ""
            elif requested_table:
                flash(f"Table `{requested_table}` was not found in `{selected_database}`.", "warning")
                requested_table = ""
    except mysql.connector.Error as error:
        flash(str(error), "error")
        database_inventory = []
        tables = []
        selected_database = ""
        requested_table = ""
        requested_browse_table = ""
        selected_table_definition = []
        browse_table_data = None
        heatwave_tables_stats = {"columns": [], "rows": [], "row_classes": [], "row_actions": []}
        heatwave_query_stats = {"columns": [], "rows": []}
        heatwave_ml_query_stats = {"columns": [], "rows": []}
        heatwave_ml_current_detail_stats = {"columns": [], "rows": []}
        heatwave_table_load_recovery_stats = {"columns": [], "rows": []}

    return render_dashboard(
        "db_admin.html",
        page_title="DB Admin",
        active_tab=active_tab,
        tabs=DB_ADMIN_TABS,
        database_inventory=database_inventory,
        selected_database=selected_database,
        selected_table=requested_table,
        browse_table=requested_browse_table,
        browse_table_data=browse_table_data,
        tables=tables,
        table_definition=selected_table_definition,
        mysql_type_options=MYSQL_TYPE_OPTIONS,
        table_form=table_form,
        add_column_form=add_column_form,
        edit_column_name=requested_edit_column,
        modify_column_form=modify_column_form,
        system_databases=SYSTEM_DATABASES,
        selected_database_is_system=_is_system_database(selected_database) if selected_database else False,
        db_admin_modal_result=db_admin_modal_result,
        heatwave_tables_stats=heatwave_tables_stats,
        selected_monitor_view=selected_monitor_view,
        selected_monitor_refresh=selected_monitor_refresh,
        heatwave_query_stats=heatwave_query_stats,
        heatwave_ml_query_stats=heatwave_ml_query_stats,
        heatwave_ml_current_detail_stats=heatwave_ml_current_detail_stats,
        current_ml_connection_only=current_ml_connection_only,
        heatwave_table_load_recovery_stats=heatwave_table_load_recovery_stats,
    )
