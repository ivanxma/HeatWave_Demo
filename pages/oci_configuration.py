import os
import re
from datetime import datetime, timedelta, timezone
import mysql.connector
from flask import flash, redirect, request, session, url_for

from app_context import (
    ASKME_CONFIG_ROWS,
    app,
    fetch_askme_config,
    get_oci_config_settings,
    get_oci_object_storage_client,
    import_oci_sdk,
    login_required,
    render_dashboard,
    save_askme_config,
    setup_askme_db,
    ROOT_DIR,
)


PAR_PREFIX_ACCESS_TYPES = [
    "AnyObjectRead",
    "AnyObjectWrite",
    "AnyObjectReadWrite",
]
SETUP_TABS = [
    {"id": "oci-config", "label": "OCI Config"},
    {"id": "bucket", "label": "Bucket Settings"},
    {"id": "upload", "label": "Bucket Upload"},
    {"id": "preauth", "label": "Pre-Authenticated URLs"},
]
LOCAL_OCI_CONFIG_DIR = ROOT_DIR / "oci_config"
LOCAL_OCI_CONFIG_FILE = LOCAL_OCI_CONFIG_DIR / "config"
OCI_PROFILE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _normalize_text(value):
    return str(value or "").strip()


def _normalize_setup_tab(value):
    tab_id = _normalize_text(value) or SETUP_TABS[0]["id"]
    allowed = {tab["id"] for tab in SETUP_TABS}
    return tab_id if tab_id in allowed else SETUP_TABS[0]["id"]


def _bucket_settings(config_values):
    return {
        "region": _normalize_text(config_values.get("OCI_REGION")),
        "bucket_name": _normalize_text(config_values.get("OCI_BUCKET_NAME")),
        "namespace_name": _normalize_text(config_values.get("OCI_NAMESPACE")),
        "base_folder": _normalize_text(config_values.get("OCI_BUCKET_FOLDER")).strip("/"),
    }


def _folder_cache_key(config_values):
    settings = _bucket_settings(config_values)
    return "|".join(
        (
            settings["region"],
            settings["namespace_name"],
            settings["bucket_name"],
        )
    )


def _get_cached_folder_options(config_values):
    cache = session.get("object_storage_folder_options", {})
    if not isinstance(cache, dict):
        return []
    payload = cache.get(_folder_cache_key(config_values), [])
    if not isinstance(payload, list):
        return []
    return [
        {"name": _normalize_text(row.get("name")), "label": _normalize_text(row.get("label"))}
        for row in payload
        if isinstance(row, dict) and _normalize_text(row.get("name"))
    ]


def _set_cached_folder_options(config_values, folder_options):
    cache = session.get("object_storage_folder_options", {})
    if not isinstance(cache, dict):
        cache = {}
    cache[_folder_cache_key(config_values)] = [
        {"name": row["name"], "label": row["label"]}
        for row in folder_options
    ]
    session["object_storage_folder_options"] = cache


def _merge_folder_option(folder_options, folder_name):
    normalized_name = _normalize_text(folder_name).strip("/")
    if not normalized_name:
        return folder_options
    merged = {row["name"]: row for row in folder_options if row.get("name")}
    merged[normalized_name] = {"name": normalized_name, "label": normalized_name}
    return [merged[name] for name in sorted(merged)]


def _require_bucket_settings(config_values):
    settings = _bucket_settings(config_values)
    missing = [key for key in ("region", "bucket_name", "namespace_name") if not settings[key]]
    if missing:
        raise ValueError("Configure OCI region, bucket name, and namespace before using bucket actions.")
    return settings


def _object_name(prefix, filename):
    clean_filename = os.path.basename(_normalize_text(filename))
    clean_prefix = _normalize_text(prefix).strip("/")
    if not clean_filename:
        raise ValueError("Choose a file to upload.")
    return "{}/{}".format(clean_prefix, clean_filename) if clean_prefix else clean_filename


def _join_folder_object(folder_name, object_name):
    clean_folder = _normalize_text(folder_name).strip("/")
    clean_object = _normalize_text(object_name).strip("/")
    if clean_folder and clean_object:
        return "{}/{}".format(clean_folder, clean_object)
    return clean_object or clean_folder


def _list_bucket_folders(config_values):
    settings = _require_bucket_settings(config_values)
    client = get_oci_object_storage_client(config_values, timeout=(10, 120))
    object_names = []
    start_value = None
    while True:
        response = client.list_objects(
            namespace_name=settings["namespace_name"],
            bucket_name=settings["bucket_name"],
            start=start_value,
            fields="name",
        )
        object_names.extend(_normalize_text(item.name) for item in getattr(response.data, "objects", []))
        start_value = getattr(response.data, "next_start_with", None)
        if not start_value:
            break

    folder_names = set()
    if settings["base_folder"]:
        folder_names.add(settings["base_folder"])
    for object_name in object_names:
        normalized_object = object_name.strip("/")
        if object_name.endswith("/") and normalized_object:
            folder_names.add(normalized_object)
        parts = [part for part in normalized_object.split("/") if part]
        for index in range(1, len(parts)):
            folder_names.add("/".join(parts[:index]))
    return [{"name": folder_name, "label": folder_name} for folder_name in sorted(folder_names)]


def _test_oci_config(config_values):
    client = get_oci_object_storage_client(config_values, timeout=(10, 30))
    namespace = _normalize_text(config_values.get("OCI_NAMESPACE"))
    if not namespace:
        namespace = _normalize_text(client.get_namespace().data)
    return namespace


def _save_existing_oci_config_reference(config_values, form):
    config_file = _normalize_text(form.get("existing_config_file")) or "~/.oci/config"
    config_profile = _normalize_text(form.get("existing_config_profile")) or "DEFAULT"
    region = _normalize_text(form.get("existing_region")) or _normalize_text(config_values.get("OCI_REGION"))
    submitted = dict(config_values)
    submitted["OCI_CONFIG_FILE"] = config_file
    submitted["OCI_CONFIG_PROFILE"] = config_profile
    if region:
        submitted["OCI_REGION"] = region
    save_askme_config(submitted)


def _safe_profile_name(value):
    profile = _normalize_text(value) or "DEFAULT"
    if not OCI_PROFILE_RE.match(profile):
        raise ValueError("OCI config profile may contain only letters, numbers, underscore, dot, and hyphen.")
    return profile


def _store_local_oci_config(config_values, form, file_storage):
    profile = _safe_profile_name(form.get("local_config_profile"))
    tenancy_id = _normalize_text(form.get("tenancy_id"))
    user_id = _normalize_text(form.get("user_id"))
    fingerprint = _normalize_text(form.get("fingerprint"))
    region = _normalize_text(form.get("local_region"))
    if not tenancy_id or not user_id or not fingerprint or not region:
        raise ValueError("Tenancy OCID, user OCID, fingerprint, and region are required.")
    if not file_storage or not _normalize_text(getattr(file_storage, "filename", "")):
        raise ValueError("Upload the OCI API private key defined by the config profile.")

    LOCAL_OCI_CONFIG_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    key_path = LOCAL_OCI_CONFIG_DIR / "{}_private_key.pem".format(profile)
    file_storage.stream.seek(0)
    key_path.write_bytes(file_storage.stream.read())
    os.chmod(key_path, 0o600)

    config_text = (
        "[{profile}]\n"
        "user={user}\n"
        "fingerprint={fingerprint}\n"
        "tenancy={tenancy}\n"
        "region={region}\n"
        "key_file={key_file}\n"
    ).format(
        profile=profile,
        user=user_id,
        fingerprint=fingerprint,
        tenancy=tenancy_id,
        region=region,
        key_file=key_path,
    )
    LOCAL_OCI_CONFIG_FILE.write_text(config_text, encoding="utf-8")
    os.chmod(LOCAL_OCI_CONFIG_FILE, 0o600)

    submitted = dict(config_values)
    submitted["OCI_CONFIG_FILE"] = str(LOCAL_OCI_CONFIG_FILE)
    submitted["OCI_CONFIG_PROFILE"] = profile
    submitted["OCI_REGION"] = region
    save_askme_config(submitted)


def _oci_config_mode(config_values):
    config_file = _normalize_text(config_values.get("OCI_CONFIG_FILE"))
    if config_file and os.path.abspath(os.path.expanduser(config_file)).startswith(str(LOCAL_OCI_CONFIG_DIR)):
        return "local"
    return "existing"


def _upload_bucket_file(config_values, file_storage, object_prefix):
    settings = _require_bucket_settings(config_values)
    object_name = _object_name(object_prefix, getattr(file_storage, "filename", ""))
    client = get_oci_object_storage_client(config_values, timeout=(30, 600))
    file_storage.stream.seek(0)
    client.put_object(
        namespace_name=settings["namespace_name"],
        bucket_name=settings["bucket_name"],
        object_name=object_name,
        put_object_body=file_storage.stream.read(),
    )
    return object_name


def _create_bucket_folder(config_values, folder_name):
    settings = _require_bucket_settings(config_values)
    normalized_folder = _normalize_text(folder_name).strip("/")
    if not normalized_folder:
        raise ValueError("Enter a folder name to create.")
    client = get_oci_object_storage_client(config_values, timeout=(30, 120))
    client.put_object(
        namespace_name=settings["namespace_name"],
        bucket_name=settings["bucket_name"],
        object_name="{}/".format(normalized_folder),
        put_object_body=b"",
    )
    return normalized_folder


def _parse_expiry(value):
    text = _normalize_text(value)
    if not text:
        return datetime.now(timezone.utc) + timedelta(days=7)
    normalized = text.replace("Z", "+00:00")
    expires_at = datetime.fromisoformat(normalized)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at.astimezone(timezone.utc)


def _datetime_from_oci_value(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        text = _normalize_text(value)
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_active_preauthenticated_request(item, now=None):
    expires_at = _datetime_from_oci_value(getattr(item, "time_expires", None))
    if expires_at is None:
        return False
    return expires_at > (now or datetime.now(timezone.utc))


def _create_preauthenticated_request(config_values, form):
    settings = _require_bucket_settings(config_values)
    access_type = _normalize_text(form.get("par_access_type")) or "AnyObjectRead"
    if access_type not in PAR_PREFIX_ACCESS_TYPES:
        raise ValueError("Choose a valid pre-authenticated request access type.")
    oci = import_oci_sdk()
    client = get_oci_object_storage_client(config_values, timeout=(30, 120))
    object_name = _join_folder_object(form.get("par_folder_name"), form.get("par_object_prefix"))
    bucket_listing_action = "ListObjects" if str(form.get("par_enable_listing", "")).lower() in {"1", "true", "yes", "on"} else None
    details = oci.object_storage.models.CreatePreauthenticatedRequestDetails(
        name=_normalize_text(form.get("par_name")) or "heatwave-demo-par",
        access_type=access_type,
        object_name=object_name or None,
        bucket_listing_action=bucket_listing_action,
        time_expires=_parse_expiry(form.get("par_expires_at")),
    )
    response = client.create_preauthenticated_request(
        namespace_name=settings["namespace_name"],
        bucket_name=settings["bucket_name"],
        create_preauthenticated_request_details=details,
    )
    access_uri = _normalize_text(getattr(response.data, "access_uri", ""))
    full_url = "https://objectstorage.{}.oraclecloud.com{}".format(settings["region"], access_uri)
    return full_url


def _delete_preauthenticated_request(config_values, par_id):
    settings = _require_bucket_settings(config_values)
    normalized_id = _normalize_text(par_id)
    if not normalized_id:
        raise ValueError("Choose a pre-authenticated request to delete.")
    client = get_oci_object_storage_client(config_values, timeout=(30, 120))
    client.delete_preauthenticated_request(
        namespace_name=settings["namespace_name"],
        bucket_name=settings["bucket_name"],
        par_id=normalized_id,
    )


def _list_preauthenticated_requests(config_values):
    settings = _bucket_settings(config_values)
    if not settings["bucket_name"] or not settings["namespace_name"]:
        return [], ""
    try:
        client = get_oci_object_storage_client(config_values, timeout=(10, 60))
        response = client.list_preauthenticated_requests(
            namespace_name=settings["namespace_name"],
            bucket_name=settings["bucket_name"],
        )
    except Exception as error:
        return [], str(error)

    data = response.data
    if isinstance(data, list):
        items = data
    else:
        items = getattr(data, "items", None)
        if items is None:
            try:
                items = list(data)
            except TypeError:
                items = []

    rows = []
    now = datetime.now(timezone.utc)
    for item in items or []:
        if not _is_active_preauthenticated_request(item, now):
            continue
        rows.append(
            {
                "id": _normalize_text(getattr(item, "id", "")),
                "name": _normalize_text(getattr(item, "name", "")),
                "access_type": _normalize_text(getattr(item, "access_type", "")),
                "object_name": _normalize_text(getattr(item, "object_name", "")),
                "bucket_listing_action": _normalize_text(getattr(item, "bucket_listing_action", "")),
                "time_created": _normalize_text(getattr(item, "time_created", "")),
                "time_expires": _normalize_text(getattr(item, "time_expires", "")),
            }
        )
    return rows, ""


@app.route("/setup-askme", methods=["GET", "POST"])
@app.route("/oci-configuration", methods=["GET", "POST"])
@login_required
def oci_configuration_page():
    config_values = {}
    last_par_url = ""
    active_tab = _normalize_setup_tab(request.args.get("tab"))
    folder_options = []
    selected_upload_folder = ""
    selected_par_folder = ""
    try:
        setup_askme_db()
        config_values = fetch_askme_config()
        folder_options = _get_cached_folder_options(config_values)
        if request.method == "POST":
            action = request.form.get("setup_action", "save_config")
            active_tab = _normalize_setup_tab(request.form.get("active_tab") or request.args.get("tab"))
            if action == "save_config":
                submitted = {
                    env_var: request.form.get(env_var, "")
                    for env_var, _default_value in ASKME_CONFIG_ROWS
                }
                save_askme_config(submitted)
                flash("Updated OCI configuration settings in askme.config.", "success")
                return redirect(url_for("oci_configuration_page", tab="bucket"))
            if action == "use_existing_oci_config":
                _save_existing_oci_config_reference(config_values, request.form)
                flash("Updated the OCI config file reference.", "success")
                return redirect(url_for("oci_configuration_page", tab="oci-config"))
            if action == "store_local_oci_config":
                _store_local_oci_config(config_values, request.form, request.files.get("private_key_file"))
                flash("Stored OCI config and private key in the local git-ignored oci_config folder.", "success")
                return redirect(url_for("oci_configuration_page", tab="oci-config"))
            if action == "test_oci_config":
                namespace = _test_oci_config(config_values)
                flash("OCI authentication succeeded. Namespace: {}".format(namespace or "-"), "success")
                active_tab = "oci-config"
            if action == "populate_upload_folders":
                folder_options = _list_bucket_folders(config_values)
                _set_cached_folder_options(config_values, folder_options)
                selected_upload_folder = _normalize_text(request.form.get("upload_folder_name")).strip("/")
                flash("Loaded {} folder option(s) from the bucket.".format(len(folder_options)), "success")
                active_tab = "upload"
            if action == "create_upload_folder":
                selected_upload_folder = _create_bucket_folder(config_values, request.form.get("new_upload_folder_name"))
                folder_options = _merge_folder_option(folder_options, selected_upload_folder)
                _set_cached_folder_options(config_values, folder_options)
                flash("Created folder: {}".format(selected_upload_folder), "success")
                active_tab = "upload"
            if action == "populate_par_folders":
                folder_options = _list_bucket_folders(config_values)
                _set_cached_folder_options(config_values, folder_options)
                selected_par_folder = _normalize_text(request.form.get("par_folder_name")).strip("/")
                flash("Loaded {} folder option(s) from the bucket.".format(len(folder_options)), "success")
                active_tab = "preauth"
            if action == "upload_object":
                object_name = _upload_bucket_file(
                    config_values,
                    request.files.get("bucket_file"),
                    request.form.get("upload_folder_name"),
                )
                flash("Uploaded object: {}".format(object_name), "success")
                return redirect(url_for("oci_configuration_page", tab="upload"))
            if action == "create_par":
                selected_par_folder = _normalize_text(request.form.get("par_folder_name")).strip("/")
                last_par_url = _create_preauthenticated_request(config_values, request.form)
                flash("Created pre-authenticated URL.", "success")
                active_tab = "preauth"
            if action == "delete_par":
                _delete_preauthenticated_request(config_values, request.form.get("par_id"))
                flash("Deleted pre-authenticated request.", "success")
                return redirect(url_for("oci_configuration_page", tab="preauth"))
    except (mysql.connector.Error, RuntimeError, ValueError) as error:
        flash(str(error), "error")

    config_items = []
    for env_var, default_value in ASKME_CONFIG_ROWS:
        config_items.append(
            {
                "env_var": env_var,
                "env_value": config_values.get(env_var, default_value),
            }
        )

    oci_config = get_oci_config_settings(config_values)
    oci_config["exists"] = bool(oci_config["config_file"] and os.path.exists(oci_config["config_file"]))
    par_rows, par_error = _list_preauthenticated_requests(config_values)

    return render_dashboard(
        "oci_configuration.html",
        page_title="OCI Configuration",
        setup_tabs=SETUP_TABS,
        active_tab=active_tab,
        config_values=config_values,
        oci_config_mode=_oci_config_mode(config_values),
        config_items=config_items,
        bucket_settings=_bucket_settings(config_values),
        oci_config=oci_config,
        par_access_types=PAR_PREFIX_ACCESS_TYPES,
        folder_options=folder_options,
        selected_upload_folder=selected_upload_folder,
        selected_par_folder=selected_par_folder,
        par_rows=par_rows,
        par_error=par_error,
        last_par_url=last_par_url,
        default_par_expires_at=(datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M"),
    )
