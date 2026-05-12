import mysql.connector
from flask import flash, redirect, request, url_for

from app_context import (
    _build_import_preview_table,
    _database_exists,
    _default_import_form,
    _default_import_table_name_from_filename,
    _delete_import_preview_file,
    _is_system_database,
    _load_import_rows_from_path,
    _save_import_preview_file,
    _table_exists,
    app,
    fetch_import_tree,
    import_file_to_table,
    login_required,
    render_dashboard,
)


@app.route("/import", methods=["GET", "POST"])
@login_required
def import_page():
    selected_database = str(request.values.get("database", request.values.get("database_name", ""))).strip()
    target_table_name = str(request.values.get("table_name", "")).strip()
    import_form = _default_import_form()
    import_summary = None
    import_preview = None

    if selected_database:
        import_form["database_name"] = selected_database
    if target_table_name:
        import_form["table_name"] = target_table_name

    import_tree = []
    existing_target = False
    selected_database_is_system = False

    try:
        import_tree = fetch_import_tree()
    except mysql.connector.Error as error:
        flash(str(error), "error")
        import_tree = []

    database_names = [row["database_name"] for row in import_tree]
    if not selected_database and import_tree:
        first_non_system = next((row["database_name"] for row in import_tree if not row["is_system"]), import_tree[0]["database_name"])
        selected_database = first_non_system
        if not import_form["database_name"]:
            import_form["database_name"] = selected_database

    if selected_database not in database_names:
        selected_database = ""

    if request.method == "POST":
        action = str(request.form.get("import_action", "load_preview")).strip()
        new_schema = request.form.get("new_schema") == "on"
        selected_database_name = str(request.form.get("database_name", "")).strip()
        new_database_name = str(request.form.get("new_database_name", "")).strip()
        database_name = new_database_name if new_schema else selected_database_name
        import_form = {
            "database_name": database_name,
            "new_schema": new_schema,
            "new_database_name": new_database_name,
            "table_name": str(request.form.get("table_name", "")).strip(),
            "overwrite_existing": request.form.get("overwrite_existing") == "on",
            "create_new_table": request.form.get("create_new_table") == "on",
            "add_invisible_primary_key": request.form.get("add_invisible_primary_key") == "on",
            "primary_key_mode": str(request.form.get("primary_key_mode", "my_row_id")).strip() or "my_row_id",
            "primary_key_columns": request.form.getlist("primary_key_columns"),
            "import_row_limit": str(request.form.get("import_row_limit", "full")).strip() or "full",
            "preview_token": str(request.form.get("preview_token", "")).strip(),
        }
        import_form["add_invisible_primary_key"] = import_form["primary_key_mode"] == "my_row_id"
        selected_database = import_form["database_name"]
        target_table_name = import_form["table_name"]
        try:
            if action == "load_preview":
                file_storage = request.files.get("import_file")
                if not file_storage or not str(file_storage.filename or "").strip():
                    raise ValueError("Choose a CSV file to load.")
                if not target_table_name:
                    import_form["table_name"] = _default_import_table_name_from_filename(file_storage.filename)
                    target_table_name = import_form["table_name"]
                if not selected_database:
                    raise ValueError("Choose a database before importing.")
                if not target_table_name:
                    raise ValueError("Enter the target table name before importing.")
                if not import_form["new_schema"] and not _database_exists(selected_database):
                    raise ValueError("Choose an existing database, or check New Schema and enter a schema name.")
                if _is_system_database(selected_database):
                    raise ValueError("System databases cannot be modified.")
                if import_form["preview_token"]:
                    _delete_import_preview_file(import_form["preview_token"])
                preview_token, _ = _save_import_preview_file(file_storage)
                import_form["preview_token"] = preview_token
                import_payload = _load_import_rows_from_path(preview_token)
                import_preview = _build_import_preview_table(import_payload)
                flash(
                    "Loaded {row_count} rows from `{filename}` for preview.".format(**import_payload),
                    "success",
                )
            elif action == "import_file":
                if not selected_database:
                    raise ValueError("Choose a database before importing.")
                if not target_table_name:
                    raise ValueError("Enter the target table name before importing.")
                if not import_form["preview_token"]:
                    raise ValueError("Load a file preview before importing.")
                import_payload = _load_import_rows_from_path(import_form["preview_token"])
                import_preview = _build_import_preview_table(import_payload)
                target_exists = (
                    bool(selected_database)
                    and _database_exists(selected_database)
                    and _table_exists(selected_database, target_table_name)
                )
                if target_exists and not import_form["overwrite_existing"]:
                    raise ValueError("Target table already exists. Confirm replacement before importing.")
                import_summary = import_file_to_table(
                    selected_database,
                    target_table_name,
                    import_payload,
                    overwrite_existing=import_form["overwrite_existing"],
                    create_new_table=import_form["create_new_table"],
                    create_schema=import_form["new_schema"],
                    primary_key_mode=import_form["primary_key_mode"],
                    primary_key_columns=import_form["primary_key_columns"],
                    import_row_limit=import_form["import_row_limit"],
                )
                _delete_import_preview_file(import_form["preview_token"])
                import_form["preview_token"] = ""
                flash(
                    "Imported {row_count} rows from `{filename}` into `{database_name}.{table_name}`.".format(**import_summary),
                    "success",
                )
                return redirect(
                    url_for(
                        "import_page",
                        database=import_summary["database_name"],
                        table_name=import_summary["table_name"],
                    )
                )
            else:
                raise ValueError("Unsupported import action.")
        except (ValueError, mysql.connector.Error) as error:
            flash(str(error), "error")

    selected_database_is_system = (
        _is_system_database(selected_database) if selected_database and not import_form["new_schema"] else False
    )
    try:
        selected_database_exists = _database_exists(selected_database) if selected_database else False
        existing_target = bool(
            selected_database_exists
            and import_form["table_name"]
            and _table_exists(selected_database, import_form["table_name"])
        )
    except mysql.connector.Error as error:
        flash(str(error), "error")
        existing_target = False
    if import_form["preview_token"] and import_preview is None:
        try:
            import_preview = _build_import_preview_table(_load_import_rows_from_path(import_form["preview_token"]))
        except ValueError:
            import_form["preview_token"] = ""
            import_preview = None

    return render_dashboard(
        "import.html",
        page_title="Import",
        import_tree=import_tree,
        selected_database=selected_database,
        selected_database_is_system=selected_database_is_system,
        existing_target=existing_target,
        import_form=import_form,
        import_preview=import_preview,
        import_summary=import_summary,
    )
