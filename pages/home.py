import mysql.connector
from flask import flash, redirect, request, session, url_for

from app_context import (
    DASHBOARD_TABS,
    app,
    clear_login_state,
    get_connection_summary,
    get_current_db_user,
    get_dashboard_server_info,
    get_session_profile,
    render_dashboard,
)


@app.route("/", methods=["GET"])
def home():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    if not get_current_db_user():
        clear_login_state()
        flash("Your database login session expired. Please log in again.", "warning")
        return redirect(url_for("login"))
    active_tab = request.args.get("tab", "demo").strip().lower()
    if active_tab not in {tab["id"] for tab in DASHBOARD_TABS}:
        active_tab = "demo"
    server_info = None
    if active_tab == "server-info":
        try:
            server_info = get_dashboard_server_info()
        except mysql.connector.Error as error:
            flash(str(error), "error")
            server_info = {
                "connection_endpoint": get_connection_summary(),
                "default_database": get_session_profile()["database"] or "-",
                "user": get_current_db_user() or "-",
                "server_version": "-",
                "uptime": "-",
                "database_rows": [],
                "summary": {
                    "database_count_display": "0",
                    "table_count_display": "0",
                    "data_length_display": "0 B",
                    "index_length_display": "0 B",
                    "total_length_display": "0 B",
                },
                "heatwave": {
                    "available": False,
                    "status": "Unable to query server metadata",
                    "node_count": 0,
                    "global_statuses": [
                        {"name": "rapid_cluster_status", "value": "-"},
                        {"name": "rapid_ml_status", "value": "-"},
                        {"name": "rapid_change_propagation_status", "value": "-"},
                        {"name": "rapid_net_encryption_status", "value": "-"},
                        {"name": "rapid_preload_stats_status", "value": "-"},
                        {"name": "rapid_resize_status", "value": "-"},
                        {"name": "rapid_service_status", "value": "-"},
                    ],
                    "traffic_light": {"state": "partial", "label": "YELLOW"},
                    "fully_loaded_count": None,
                    "partially_loaded_count": None,
                    "nodes_table": {"columns": [], "rows": []},
                    "loaded_tables": {"columns": [], "rows": []},
                    "partial_tables": {"columns": [], "rows": []},
                    "notes": [],
                },
                "errors": [str(error)],
            }
    return render_dashboard(
        "dashboard.html",
        page_title="Dashboard",
        active_tab=active_tab,
        tabs=DASHBOARD_TABS,
        server_info=server_info,
    )
