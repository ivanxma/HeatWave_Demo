import json

import mysql.connector
from flask import flash, request, session

from app_context import (
    app,
    build_nlsql_call_text,
    build_nlsql_tables,
    call_nlsql,
    choose_default_model,
    fetch_enabled_databases,
    get_nlsql_models,
    login_required,
    render_dashboard,
    setup_db,
)


@app.route("/nlsql", methods=["GET", "POST"])
@login_required
def nlsql_page():
    question = ""
    selected_databases = []
    sql_text = ""
    table_names = ""
    result_tables = []
    models = []
    selected_model = ""
    proc_call_text = ""

    try:
        setup_db()
        available_databases = fetch_enabled_databases()
        models = get_nlsql_models()
        selected_model = choose_default_model(models)
        default_databases = [
            name
            for name in ("information_schema", "performance_schema")
            if name in available_databases
        ]

        if request.method == "POST":
            question = request.form.get("question", "").strip()
            selected_databases = request.form.getlist("databases")
            selected_model = request.form.get("llm", "").strip() or selected_model

            if not question:
                flash("Enter a question.", "warning")
            elif not selected_databases:
                flash("Choose at least one schema.", "warning")
            elif not selected_model:
                flash("No supported generation models were found for this connection.", "error")
            else:
                proc_call_text = build_nlsql_call_text(question, selected_model, selected_databases)
                response = call_nlsql(question, selected_model, selected_databases)
                output = json.loads(response.get("output") or "{}")
                sql_text = output.get("sql_query", "")
                table_names = output.get("tables", "")
                if output.get("is_sql_valid") == 1:
                    result_tables = build_nlsql_tables(response)
        else:
            selected_databases = default_databases
    except mysql.connector.Error as error:
        flash(str(error), "error")
        available_databases = []
    except json.JSONDecodeError:
        flash("The NL_SQL response could not be parsed.", "error")
        available_databases = fetch_enabled_databases() if session.get("logged_in") else []

    return render_dashboard(
        "nlsql.html",
        page_title="HWnlsql",
        available_databases=available_databases,
        selected_databases=selected_databases,
        llm_models=models,
        selected_model=selected_model,
        question=question,
        proc_call_text=proc_call_text,
        sql_text=sql_text,
        table_names=table_names,
        result_tables=result_tables,
        docs_url="https://dev.mysql.com/doc/heatwave/en/mys-hw-genai-nl-sql.html",
    )
