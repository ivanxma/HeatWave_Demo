import mysql.connector
from flask import flash, redirect, request, url_for

from app_context import (
    app,
    fetch_available_databases,
    fetch_enabled_databases,
    login_required,
    render_dashboard,
    save_configdb_databases,
    setup_db,
)


@app.route("/setup-configdb", methods=["GET", "POST"])
@login_required
def setup_configdb_page():
    try:
        setup_db()
        if request.method == "POST":
            selected = request.form.getlist("enabled_databases")
            save_configdb_databases(selected)
            flash("Updated nlsql.configdb.", "success")
            return redirect(url_for("setup_configdb_page"))
        configured = fetch_enabled_databases()
        available = fetch_available_databases()
    except mysql.connector.Error as error:
        flash(str(error), "error")
        configured = []
        available = []

    return render_dashboard(
        "setup_configdb.html",
        page_title="Setup configdb",
        configured_databases=configured,
        available_databases=available,
        unconfigured_databases=[name for name in available if name not in configured],
    )
