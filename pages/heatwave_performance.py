import mysql.connector
from flask import flash, redirect, request, url_for

from app_context import (
    HEATWAVE_PERFORMANCE_SQL,
    airportdb_exists,
    app,
    execute_heatwave_performance_query,
    explain_heatwave_performance_query,
    get_heatwave_performance_table_counts,
    get_session_autocommit_value,
    login_required,
    render_dashboard,
)


@app.route("/heatwave-performance", methods=["GET", "POST"])
@login_required
def heatwave_performance_page():
    active_tab = request.values.get("tab", "innodb").strip().lower()
    if active_tab not in HEATWAVE_PERFORMANCE_SQL:
        active_tab = "innodb"
    sql_text = HEATWAVE_PERFORMANCE_SQL[active_tab]

    try:
        if not airportdb_exists(autocommit=True):
            flash("The HeatWave Performance page is available only when schema `airportdb` exists.", "warning")
            return redirect(url_for("home"))

        table_counts = get_heatwave_performance_table_counts()
        result_table = {"columns": [], "rows": []}
        execution_timing = None
        query_executed = False
        autocommit_value = get_session_autocommit_value()
        explain_plan = {"columns": [], "rows": []}

        if request.method == "POST":
            sql_text = request.form.get("sql_text", "").strip() or sql_text
            result_table, execution_timing, autocommit_value = execute_heatwave_performance_query(sql_text)
            query_executed = True
            explain_plan = explain_heatwave_performance_query(sql_text)
    except mysql.connector.Error as error:
        flash(str(error), "error")
        table_counts = []
        explain_plan = {"columns": [], "rows": []}
        result_table = {"columns": [], "rows": []}
        execution_timing = None
        query_executed = request.method == "POST"
        autocommit_value = None

    return render_dashboard(
        "heatwave_performance.html",
        page_title="HeatWave Performance",
        active_tab=active_tab,
        active_tab_label="InnoDB" if active_tab == "innodb" else "RAPID engine",
        tabs=[
            {"id": "innodb", "label": "InnoDB"},
            {"id": "rapid", "label": "RAPID engine"},
        ],
        sql_text=sql_text,
        explain_plan=explain_plan,
        table_counts=table_counts,
        result_table=result_table,
        execution_timing=execution_timing,
        query_executed=query_executed,
        autocommit_value=autocommit_value,
    )
