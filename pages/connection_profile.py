from flask import session

from app_context import (
    app,
    get_profile_by_name,
    get_selected_profile_name,
    get_session_profile,
    load_profiles,
    login_required,
    render_dashboard,
)


@app.route("/connection-profile", methods=["GET"])
@login_required
def connection_profile():
    profiles = load_profiles()
    current_name = get_selected_profile_name() or session.get("profile_name", "")
    current_profile = get_profile_by_name(current_name) or get_session_profile()
    return render_dashboard(
        "connection_profile.html",
        page_title="Connection Profile",
        profiles=profiles,
        selected_profile_name=current_name,
        form_profile=current_profile,
    )
