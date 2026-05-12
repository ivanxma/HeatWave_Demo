from flask import flash, redirect, render_template, request, session, url_for

from app_context import (
    APP_TITLE,
    DEFAULT_PROFILE,
    app,
    check_repo_version,
    clear_login_state,
    get_profile_by_name,
    get_selected_profile_name,
    load_profiles,
    normalize_profile,
    redirect_for_profile_update,
    save_profiles,
    set_session_credentials,
    set_session_profile,
    start_connection_cache_session,
    validate_profile_settings,
    validate_user,
)


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("home"))

    profiles = load_profiles()
    selected_profile_name = get_selected_profile_name()
    selected_profile = get_profile_by_name(selected_profile_name) or normalize_profile(DEFAULT_PROFILE)
    if request.method == "POST":
        selected_profile_name = request.form.get("profile_name", "").strip()
        selected_profile = get_profile_by_name(selected_profile_name)
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not selected_profile:
            flash("Choose a saved connection profile before logging in.", "warning")
        else:
            set_session_profile(selected_profile)
            ok, message = validate_user(username, password)
            if ok:
                start_connection_cache_session()
                set_session_credentials(username, password)
                session["logged_in"] = True
                version_status = check_repo_version()
                session["version_check"] = version_status
                if version_status.get("update_available"):
                    flash(
                        "Version {repo_version} is available. Current version is {current_version}.".format(
                            repo_version=version_status.get("repo_version") or "-",
                            current_version=version_status.get("current_version") or "-",
                        ),
                        "info",
                    )
                    return redirect(url_for("update_heatwave_demo"))
                flash("Login successful.", "success")
                return redirect(url_for("home"))
            clear_login_state()
            flash(message or "Invalid connection profile or database credentials.", "error")
        selected_profile = selected_profile or normalize_profile(DEFAULT_PROFILE)

    return render_template(
        "login.html",
        app_title=APP_TITLE,
        profiles=profiles,
        selected_profile_name=selected_profile_name,
        selected_profile=selected_profile,
        form_profile=selected_profile,
        logged_in=False,
    )


@app.route("/profiles", methods=["POST"])
def save_profile_route():
    existing_profiles = load_profiles()
    action = request.form.get("profile_action", "save")
    profile = normalize_profile(request.form)

    if not profile["name"]:
        flash("Profile name is required.", "warning")
        return redirect_for_profile_update()

    validation_errors = validate_profile_settings(profile, require_key_file=bool(profile.get("ssh_enabled")))
    if validation_errors and action != "delete":
        for error in validation_errors:
            flash(error, "warning")
        return redirect_for_profile_update(profile["name"])

    if action == "delete":
        updated = [row for row in existing_profiles if row["name"].lower() != profile["name"].lower()]
        save_profiles(updated)
        if session.get("profile_name", "").lower() == profile["name"].lower():
            clear_login_state(keep_profile=False)
        flash("Profile deleted.", "success")
        return redirect_for_profile_update()

    updated = [row for row in existing_profiles if row["name"].lower() != profile["name"].lower()]
    updated.append(profile)
    save_profiles(updated)

    if session.get("profile_name", "").lower() == profile["name"].lower():
        set_session_profile(profile)
    flash("Profile saved.", "success")
    return redirect_for_profile_update(profile["name"])


@app.route("/logout", methods=["POST"])
def logout():
    clear_login_state()
    flash("Logged out.", "info")
    return redirect(url_for("login"))
