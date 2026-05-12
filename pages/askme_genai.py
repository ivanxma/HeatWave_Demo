import importlib
import json
import os
import re
from datetime import datetime
from urllib.parse import quote

import mysql.connector
from flask import flash, redirect, request, session, url_for

from app_context import (
    ASKME_SCHEMA_NAME,
    _normalize_modal_cell,
    _quote_identifier,
    _validate_identifier,
    app,
    choose_default_model,
    close_mysql_connection,
    fetch_askme_config,
    get_connection_config,
    get_generation_models,
    get_oci_object_storage_client,
    login_required,
    mysql_connection,
    render_dashboard,
    run_sql,
    setup_askme_db,
)


ASKME_TABS = [
    {"id": "find-relevant-docs", "label": "Find Relevant Docs"},
    {"id": "free-style-answer", "label": "Free-style Answer"},
    {"id": "answer-summary", "label": "Answer Summary"},
    {"id": "chatbot", "label": "Chatbot"},
    {"id": "knowledge-base-management", "label": "Knowledge Base Management"},
]
ASKME_CONFIG_REQUIRED_KEYS = [
    "OCI_REGION",
    "OCI_BUCKET_NAME",
    "OCI_NAMESPACE",
    "OCI_BUCKET_FOLDER",
]
ASKME_EMBED_MODEL_ID = "multilingual-e5-small"
ASKME_FIND_TOPK = 20
ASKME_ANSWER_TOPK = 3
ASKME_SUMMARY_TOPK = 5
ASKME_UPLOAD_CONNECTION_TIMEOUT_SECONDS = 30
ASKME_UPLOAD_READ_TIMEOUT_SECONDS = 1800
ASKME_UPLOAD_WRITE_TIMEOUT_SECONDS = 1800
ASKME_SUMMARY_PROMPT = """
You are a data summarizer. I will provide a question and relevant context data.
Summarize the parts of the context that best answer the question.

Context:
{context}

Question:
{question}
"""


def _normalize_tab(raw_value):
    value = str(raw_value or ASKME_TABS[0]["id"]).strip().lower()
    allowed = {item["id"] for item in ASKME_TABS}
    return value if value in allowed else ASKME_TABS[0]["id"]


def _normalize_text(value):
    return str(_normalize_modal_cell(value)).strip()


def _normalize_float(value, default_value, minimum=0.0, maximum=1.0):
    try:
        normalized = float(str(value or "").strip())
    except (TypeError, ValueError):
        normalized = float(default_value)
    if normalized < minimum:
        normalized = minimum
    if normalized > maximum:
        normalized = maximum
    return normalized


def _normalize_int(value, default_value, minimum=1, maximum=100):
    try:
        normalized = int(str(value or "").strip())
    except (TypeError, ValueError):
        normalized = int(default_value)
    if normalized < minimum:
        normalized = minimum
    if normalized > maximum:
        normalized = maximum
    return normalized


def _consume_cursor_results(cursor):
    first_value = ""
    while True:
        if cursor.with_rows:
            rows = cursor.fetchall()
            if not first_value and rows and rows[0]:
                first_value = _normalize_text(rows[0][0])
        if not cursor.nextset():
            break
    return first_value


def _collect_cursor_results(cursor, title_prefix="Result Set"):
    datasets = []
    result_index = 1
    while True:
        if cursor.with_rows:
            datasets.append(
                {
                    "title": "{} {}".format(title_prefix, result_index),
                    "columns": list(cursor.column_names or ()),
                    "rows": [
                        [_normalize_modal_cell(value) for value in row]
                        for row in cursor.fetchall()
                    ],
                }
            )
        else:
            statement_text = _normalize_text(getattr(cursor, "statement", ""))
            rowcount_value = _normalize_modal_cell(getattr(cursor, "rowcount", ""))
            if statement_text or rowcount_value not in ("", None, -1):
                datasets.append(
                    {
                        "title": "Status {}".format(result_index),
                        "columns": ["statement", "rowcount"],
                        "rows": [[statement_text, rowcount_value]],
                    }
                )
        result_index += 1
        if not cursor.nextset():
            break
    return datasets


def _safe_json_loads(value, default_value):
    text = _normalize_text(value)
    if not text:
        return default_value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default_value


def _get_generation_models():
    return get_generation_models()


def _filter_selected_tables(available_tables, selected_tables):
    allowed = set(available_tables)
    return [table for table in selected_tables if table in allowed]


def _load_selected_tables(available_tables):
    submitted = request.form.getlist("selected_tables")
    if submitted:
        selected = submitted
    else:
        selected = session.get("askme_selected_tables", [])
    return _filter_selected_tables(available_tables, selected)


def _load_selected_model(models):
    requested = _normalize_text(request.form.get("generate_model_id"))
    if requested and requested in models:
        return requested
    stored = _normalize_text(session.get("askme_selected_model"))
    if stored and stored in models:
        return stored
    return choose_default_model(models)


def _save_askme_preferences(selected_tables, selected_model):
    session["askme_selected_tables"] = list(selected_tables)
    session["askme_selected_model"] = str(selected_model or "")


def _get_chat_history():
    history = session.get("askme_chat_history", [])
    return history if isinstance(history, list) else []


def _set_chat_history(history):
    session["askme_chat_history"] = list(history or [])


def _get_askme_config_status(config_values):
    missing = [key for key in ASKME_CONFIG_REQUIRED_KEYS if not _normalize_text(config_values.get(key))]
    return {"missing_keys": missing, "is_ready": not missing}


def _require_askme_config(config_values):
    status = _get_askme_config_status(config_values)
    if status["missing_keys"]:
        raise ValueError(
            "Configure {} in Admin > OCI Configuration before using Askme GenAI.".format(
                ", ".join(status["missing_keys"])
            )
        )


def _import_oci():
    try:
        return importlib.import_module("oci")
    except Exception as error:
        raise RuntimeError(
            "The OCI SDK is not available. Install the project requirements first."
        ) from error


def _get_object_storage_client(config_values):
    return get_oci_object_storage_client(config_values, timeout=(30, 600))


def _build_bucket_prefix(base_prefix, table_name):
    prefix = str(base_prefix or "").strip().strip("/")
    if not prefix:
        return table_name
    if prefix.endswith(("/", "-", "_")):
        return "{}{}".format(prefix.rstrip("/"), table_name)
    return "{}/{}".format(prefix, table_name)


def _upload_files_to_object_storage(files, config_values, table_name):
    client = _get_object_storage_client(config_values)
    namespace_name = _normalize_text(config_values.get("OCI_NAMESPACE"))
    bucket_name = _normalize_text(config_values.get("OCI_BUCKET_NAME"))
    prefix = _build_bucket_prefix(config_values.get("OCI_BUCKET_FOLDER"), table_name)
    uploaded_count = 0
    uploaded_objects = []
    try:
        for file_storage in files:
            filename = os.path.basename(str(file_storage.filename or "").strip())
            if not filename:
                continue
            object_name = "{}/{}".format(prefix.rstrip("/"), filename)
            file_storage.stream.seek(0)
            client.put_object(
                namespace_name=namespace_name,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=file_storage.stream.read(),
            )
            uploaded_count += 1
            uploaded_objects.append(object_name)
    except Exception as error:
        raise RuntimeError(
            "Uploading files to OCI Object Storage failed. Verify the AskME bucket settings and OCI access."
        ) from error
    return prefix, uploaded_count, uploaded_objects


def _delete_object_storage_prefix(config_values, prefix):
    client = _get_object_storage_client(config_values)
    namespace_name = _normalize_text(config_values.get("OCI_NAMESPACE"))
    bucket_name = _normalize_text(config_values.get("OCI_BUCKET_NAME"))
    oci = _import_oci()
    try:
        response = oci.pagination.list_call_get_all_results(
            client.list_objects,
            namespace_name=namespace_name,
            bucket_name=bucket_name,
            prefix=prefix.rstrip("/") + "/",
        )
        for item in response.data.objects:
            client.delete_object(
                namespace_name=namespace_name,
                bucket_name=bucket_name,
                object_name=item.name,
            )
    except Exception as error:
        raise RuntimeError(
            "Deleting AskME files from OCI Object Storage failed. Verify the AskME bucket settings and OCI access."
        ) from error


def _list_askme_vector_tables():
    return _list_askme_tables()


def _list_askme_tables():
    rows = run_sql(
        """
        select table_name as table_name_value
        from information_schema.tables
        where table_schema = %s
          and table_name <> 'config'
        order by table_name
        """,
        (ASKME_SCHEMA_NAME,),
        include_database=False,
    )
    return [str(row[0]) for row in rows]


def _group_chunks_by_url(chunks):
    grouped = {}
    for chunk in chunks:
        url = str(chunk.get("url") or "")
        grouped.setdefault(url, []).append(chunk)
    results = []
    for url, items in grouped.items():
        ordered = sorted(items, key=lambda item: float(item.get("similarity_score") or 0), reverse=True)
        results.append({"url": url, "chunks": ordered})
    results.sort(
        key=lambda item: max(float(chunk.get("similarity_score") or 0) for chunk in item["chunks"]),
        reverse=True,
    )
    return results


def _build_chunk_dataset(chunks):
    return {
        "columns": ["index_name", "file_name", "chunk_id", "similarity_score", "content_chunk"],
        "rows": [
            [
                chunk.get("index_name", ""),
                chunk.get("file_name", ""),
                chunk.get("chunk_id", ""),
                "{:.4f}".format(float(chunk.get("similarity_score") or 0)),
                chunk.get("content_chunk", ""),
            ]
            for chunk in chunks
        ],
    }


def _build_table_dataset(table_names):
    return {
        "columns": ["table_name"],
        "rows": [[name] for name in table_names],
    }


def _askme_upload_connection_config():
    config = dict(get_connection_config(include_database=False))
    config["connection_timeout"] = ASKME_UPLOAD_CONNECTION_TIMEOUT_SECONDS
    config["read_timeout"] = ASKME_UPLOAD_READ_TIMEOUT_SECONDS
    config["write_timeout"] = ASKME_UPLOAD_WRITE_TIMEOUT_SECONDS
    return config


def _search_similar_chunks(question, table_names, *, topk, min_similarity_score):
    cnx = None
    cursor = None
    results = []
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute("CALL sys.ML_MODEL_LOAD(%s, NULL)", (ASKME_EMBED_MODEL_ID,))
        _consume_cursor_results(cursor)
        cursor.execute(
            "SELECT sys.ML_EMBED_ROW(%s, JSON_OBJECT('model_id', %s)) INTO @input_embedding",
            (question, ASKME_EMBED_MODEL_ID),
        )
        _consume_cursor_results(cursor)
        cursor.execute("SET group_concat_max_len = 65535")
        _consume_cursor_results(cursor)

        for table_name in table_names:
            normalized_table = _validate_identifier(table_name, "Vector table")
            sql_text = """
                SELECT
                    %s AS index_name,
                    t.document_name,
                    topk_chks.segment_number AS chunk_id,
                    GROUP_CONCAT(t.segment ORDER BY t.segment_number SEPARATOR ' ') AS content_chunk,
                    MAX(topk_chks.similarity_score) AS similarity_score
                FROM (
                    SELECT
                        document_id,
                        segment_number,
                        (1 - DISTANCE(segment_embedding, @input_embedding, 'COSINE')) AS similarity_score
                    FROM {schema_name}.{table_name}
                    ORDER BY similarity_score DESC, document_id, segment_number
                    LIMIT %s
                ) topk_chks
                JOIN {schema_name}.{table_name} t
                  ON t.document_id = topk_chks.document_id
                WHERE t.segment_number >= CAST(topk_chks.segment_number AS SIGNED) - 0
                  AND t.segment_number <= CAST(topk_chks.segment_number AS SIGNED) + 1
                GROUP BY t.document_id, t.document_name, topk_chks.segment_number, t.metadata
                HAVING similarity_score > %s
                ORDER BY similarity_score DESC, document_name, chunk_id
            """.format(
                schema_name=_quote_identifier(ASKME_SCHEMA_NAME),
                table_name=_quote_identifier(normalized_table),
            )
            cursor.execute(sql_text, (normalized_table, int(topk), float(min_similarity_score)))
            for row in cursor.fetchall():
                results.append(
                    {
                        "index_name": _normalize_text(row[0] if len(row) > 0 else ""),
                        "file_name": os.path.basename(_normalize_text(row[1] if len(row) > 1 else "")),
                        "url": _normalize_text(row[1] if len(row) > 1 else ""),
                        "chunk_id": _normalize_text(row[2] if len(row) > 2 else ""),
                        "content_chunk": _normalize_text(row[3] if len(row) > 3 else ""),
                        "similarity_score": float(row[4] if len(row) > 4 and row[4] is not None else 0),
                    }
                )

        results.sort(key=lambda item: item["similarity_score"], reverse=True)
        return results[:topk]
    finally:
        close_mysql_connection(cnx)


def _generate_rag_answer(question, table_names, model_id):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        options_json = json.dumps(
            {
                "vector_store": [
                    "{}.{}".format(_quote_identifier(ASKME_SCHEMA_NAME), _quote_identifier(table_name))
                    for table_name in table_names
                ],
                "model_options": {"model_id": model_id},
                "n_citations": ASKME_ANSWER_TOPK,
                "retrieval_options": {"segment_overlap": 2},
            }
        )
        cursor.execute("CALL sys.ML_RAG(%s, @output, %s)", (question, options_json))
        _consume_cursor_results(cursor)
        cursor.execute(
            """
            SELECT
                JSON_UNQUOTE(JSON_EXTRACT(@output, '$.text')) AS answer_value,
                JSON_EXTRACT(@output, '$.citations') AS citations_value
            """
        )
        row = cursor.fetchone() or ("", "[]")
        citations = []
        for cite in _safe_json_loads(row[1] if len(row) > 1 else "[]", []):
            citations.append(
                {
                    "index_name": "",
                    "file_name": os.path.basename(_normalize_text(cite.get("document_name"))),
                    "url": _normalize_text(cite.get("document_name")),
                    "chunk_id": "",
                    "content_chunk": _normalize_text(cite.get("segment")),
                    "similarity_score": 1 - float(cite.get("distance") or 0),
                }
            )
        return _normalize_text(row[0] if len(row) > 0 else ""), citations
    finally:
        close_mysql_connection(cnx)


def _generate_summary_answer(question, chunks, model_id):
    context_parts = []
    current_size = 0
    for chunk in chunks:
        text = _normalize_text(chunk.get("content_chunk"))
        if not text:
            continue
        if current_size + len(text) > 12000:
            remaining = max(0, 12000 - current_size)
            if remaining:
                context_parts.append(text[:remaining])
            break
        context_parts.append(text)
        current_size += len(text)
    prompt = ASKME_SUMMARY_PROMPT.format(question=question, context="\n\n".join(context_parts))
    rows = run_sql(
        "SELECT sys.ML_GENERATE(%s, JSON_OBJECT('model_id', %s))",
        (prompt, model_id),
        include_database=False,
    )
    if not rows:
        return ""
    response_text = _normalize_text(rows[0][0])
    parsed = _safe_json_loads(response_text, {})
    if isinstance(parsed, dict):
        return _normalize_text(parsed.get("text"))
    return response_text


def _run_chatbot(question, table_names, model_id, prior_history):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        chat_options = {
            "tables": [{"schema_name": ASKME_SCHEMA_NAME, "table_name": table_name} for table_name in table_names],
            "model_options": {"model_id": model_id},
            "chat_history": prior_history,
        }
        cursor.execute("SET @chat_options = %s", (json.dumps(chat_options),))
        _consume_cursor_results(cursor)
        cursor.execute("CALL sys.HEATWAVE_CHAT(%s)", (question,))
        _consume_cursor_results(cursor)
        cursor.execute(
            """
            SELECT
                JSON_EXTRACT(COALESCE(@chat_options, '{}'), '$.chat_history') AS history_value,
                JSON_EXTRACT(COALESCE(@chat_options, '{}'), '$.documents') AS documents_value
            """
        )
        row = cursor.fetchone() or ("[]", "[]")
        history = _safe_json_loads(row[0] if len(row) > 0 else "[]", [])
        documents = _safe_json_loads(row[1] if len(row) > 1 else "[]", [])
        answer_text = ""
        if history:
            answer_text = _normalize_text(history[-1].get("chat_bot_message"))
        citations = []
        for item in documents:
            citations.append(
                {
                    "index_name": _normalize_text(item.get("table_name")),
                    "file_name": os.path.basename(_normalize_text(item.get("id"))),
                    "url": _normalize_text(item.get("id")),
                    "chunk_id": _normalize_text(item.get("chunk_id")),
                    "content_chunk": _normalize_text(item.get("segment")),
                    "similarity_score": float(item.get("similarity_score") or 0),
                }
            )
        return answer_text, history, citations
    finally:
        close_mysql_connection(cnx)


def _create_vector_store(table_name, files, config_values):
    normalized_table = _validate_identifier(table_name, "Vector table")
    prefix, uploaded_count, uploaded_objects = _upload_files_to_object_storage(
        files,
        config_values,
        normalized_table,
    )
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(_askme_upload_connection_config())
        cursor = cnx.cursor()
        bucket_name = _normalize_text(config_values.get("OCI_BUCKET_NAME"))
        namespace_name = _normalize_text(config_values.get("OCI_NAMESPACE"))
        input_json = json.dumps(
            [
                {
                    "db_name": ASKME_SCHEMA_NAME,
                    "tables": [
                        {
                            "table_name": normalized_table,
                            "engine_attribute": {
                                "dialect": {
                                    "format": "auto_unstructured",
                                    "is_strict_mode": False,
                                },
                                "file": [
                                    {
                                        "uri": "oci://{}@{}/{}".format(
                                            bucket_name,
                                            namespace_name,
                                            quote(object_name, safe="/-_.~"),
                                        )
                                    }
                                    for object_name in uploaded_objects
                                ],
                            },
                        }
                    ],
                }
            ]
        )
        options_json = json.dumps({"mode": "normal", "output": "silent"})
        cursor.execute("SET @input_list = %s", (input_json,))
        _consume_cursor_results(cursor)
        cursor.execute("SET @options = %s", (options_json,))
        _consume_cursor_results(cursor)
        cursor.execute("CALL sys.heatwave_load(@input_list, @options)")
        procedure_datasets = _collect_cursor_results(cursor, title_prefix="HeatWave Load Result")
        cnx.commit()
        return {
            "table_name": normalized_table,
            "prefix": prefix,
            "uploaded_count": uploaded_count,
            "procedure_datasets": procedure_datasets,
        }
    except mysql.connector.Error as error:
        try:
            _delete_object_storage_prefix(config_values, prefix)
        except Exception:
            pass
        if getattr(error, "errno", None) in {2006, 2013}:
            raise RuntimeError(
                "The MySQL connection was lost while HeatWave was building the vector table. "
                "The upload may still be processing on the server. Check the AskME table list or retry after a short wait."
            ) from error
        raise RuntimeError("Vector table creation failed: {}".format(error)) from error
    except Exception as error:
        try:
            _delete_object_storage_prefix(config_values, prefix)
        except Exception:
            pass
        raise RuntimeError("Vector table creation failed: {}".format(error)) from error
    finally:
        close_mysql_connection(cnx)


def _drop_vector_table(table_name):
    normalized_table = _validate_identifier(table_name, "Vector table")
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute(
            "DROP TABLE IF EXISTS {}.{}".format(
                _quote_identifier(ASKME_SCHEMA_NAME),
                _quote_identifier(normalized_table),
            )
        )
        cnx.commit()
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


@app.route("/askme-genai", methods=["GET", "POST"])
@login_required
def askme_genai_page():
    active_tab = _normalize_tab(request.values.get("tab"))
    config_values = {}
    config_status = {"missing_keys": [], "is_ready": False}
    generation_models = []
    available_tables = []
    managed_tables = []
    selected_tables = []
    selected_model = ""
    chat_history = _get_chat_history()

    relevant_question = ""
    free_style_question = ""
    summary_question = ""
    chat_question = ""
    relevant_min_similarity = 0.4
    relevant_topk = ASKME_FIND_TOPK
    answer_text = ""
    answer_chunks = []
    management_message = ""
    management_status = ""
    create_result = {}
    create_result_tabs = []

    try:
        setup_askme_db()
        config_values = fetch_askme_config()
        config_status = _get_askme_config_status(config_values)
        generation_models = _get_generation_models()
        available_tables = _list_askme_vector_tables()
        managed_tables = _list_askme_tables()
        selected_tables = _load_selected_tables(available_tables)
        selected_model = _load_selected_model(generation_models)

        if request.method == "POST":
            _save_askme_preferences(selected_tables, selected_model)
            action = _normalize_text(request.form.get("askme_action"))

            if action == "find_relevant_docs":
                relevant_question = _normalize_text(request.form.get("question"))
                relevant_min_similarity = _normalize_float(request.form.get("min_similarity_score"), 0.4)
                relevant_topk = _normalize_int(request.form.get("topk"), ASKME_FIND_TOPK)
                _require_askme_config(config_values)
                if not selected_tables:
                    raise ValueError("Select at least one vector table.")
                if not relevant_question:
                    raise ValueError("Enter a question first.")
                answer_chunks = _search_similar_chunks(
                    relevant_question,
                    selected_tables,
                    topk=relevant_topk,
                    min_similarity_score=relevant_min_similarity,
                )
                flash("Relevant documents loaded.", "success")
            elif action == "free_style_answer":
                free_style_question = _normalize_text(request.form.get("question"))
                _require_askme_config(config_values)
                if not selected_tables:
                    raise ValueError("Select at least one vector table.")
                if not free_style_question:
                    raise ValueError("Enter a question first.")
                answer_text, answer_chunks = _generate_rag_answer(
                    free_style_question,
                    selected_tables,
                    selected_model,
                )
                flash("Askme answer generated.", "success")
            elif action == "answer_summary":
                summary_question = _normalize_text(request.form.get("question"))
                _require_askme_config(config_values)
                if not selected_tables:
                    raise ValueError("Select at least one vector table.")
                if not summary_question:
                    raise ValueError("Enter a question first.")
                answer_chunks = _search_similar_chunks(
                    summary_question,
                    selected_tables,
                    topk=ASKME_SUMMARY_TOPK,
                    min_similarity_score=0.0,
                )
                answer_text = _generate_summary_answer(summary_question, answer_chunks, selected_model)
                flash("Summary generated.", "success")
            elif action == "chatbot":
                chat_question = _normalize_text(request.form.get("question"))
                _require_askme_config(config_values)
                if not selected_tables:
                    raise ValueError("Select at least one vector table.")
                if not chat_question:
                    raise ValueError("Enter a message first.")
                answer_text, chat_history, answer_chunks = _run_chatbot(
                    chat_question,
                    selected_tables,
                    selected_model,
                    chat_history,
                )
                _set_chat_history(chat_history)
                flash("Chatbot response generated.", "success")
            elif action == "clear_chat":
                chat_history = []
                _set_chat_history(chat_history)
                flash("Chat history cleared.", "success")
            elif action == "kb_create":
                _require_askme_config(config_values)
                table_name = _normalize_text(request.form.get("table_name"))
                files = [item for item in request.files.getlist("files") if item and item.filename]
                if not table_name:
                    raise ValueError("Enter a vector table name.")
                if not files:
                    raise ValueError("Choose one or more files to upload.")
                create_result = _create_vector_store(table_name, files, config_values)
                create_result_tabs = [
                    {
                        "id": "create-result-{}".format(index + 1),
                        "label": dataset.get("title") or "Result {}".format(index + 1),
                        "dataset": dataset,
                    }
                    for index, dataset in enumerate(create_result.get("procedure_datasets") or [])
                ]
                available_tables = _list_askme_vector_tables()
                managed_tables = _list_askme_tables()
                selected_tables = _filter_selected_tables(
                    available_tables,
                    list(dict.fromkeys(selected_tables + [create_result["table_name"]])),
                )
                _save_askme_preferences(selected_tables, selected_model)
                management_message = "Created vector table `{}` from {} uploaded file(s).".format(
                    create_result["table_name"],
                    create_result["uploaded_count"],
                )
                management_status = "success"
                flash("Vector table created.", "success")
            elif action == "kb_delete":
                _require_askme_config(config_values)
                table_name = _normalize_text(request.form.get("delete_table_name"))
                if not table_name:
                    raise ValueError("Choose a vector table to delete.")
                _drop_vector_table(table_name)
                _delete_object_storage_prefix(
                    config_values,
                    _build_bucket_prefix(config_values.get("OCI_BUCKET_FOLDER"), table_name),
                )
                available_tables = _list_askme_vector_tables()
                managed_tables = _list_askme_tables()
                selected_tables = _filter_selected_tables(available_tables, selected_tables)
                _save_askme_preferences(selected_tables, selected_model)
                management_message = "Deleted vector table `{}` and its object storage folder.".format(table_name)
                management_status = "success"
                flash("Vector table deleted.", "success")
            elif action == "kb_reset":
                _require_askme_config(config_values)
                if not managed_tables:
                    raise ValueError("No AskME vector tables were found.")
                deleted_count = 0
                for table_name in list(managed_tables):
                    _drop_vector_table(table_name)
                    _delete_object_storage_prefix(
                        config_values,
                        _build_bucket_prefix(config_values.get("OCI_BUCKET_FOLDER"), table_name),
                    )
                    deleted_count += 1
                available_tables = _list_askme_vector_tables()
                managed_tables = _list_askme_tables()
                selected_tables = []
                _save_askme_preferences(selected_tables, selected_model)
                management_message = "Reset AskME knowledge base by deleting {} vector table(s).".format(
                    deleted_count
                )
                management_status = "success"
                flash("Knowledge base reset.", "success")
            elif action:
                raise ValueError("Unsupported Askme action.")
    except (ValueError, RuntimeError, mysql.connector.Error) as error:
        flash(str(error), "error")

    citations_by_url = _group_chunks_by_url(answer_chunks)
    selected_tables_dataset = _build_table_dataset(selected_tables)
    available_tables_dataset = _build_table_dataset(available_tables)
    managed_tables_dataset = _build_table_dataset(managed_tables)
    chunk_dataset = _build_chunk_dataset(answer_chunks)

    return render_dashboard(
        "askme_genai.html",
        page_title="Askme GenAI",
        tabs=ASKME_TABS,
        active_tab=active_tab,
        config_status=config_status,
        generation_models=generation_models,
        selected_model=selected_model,
        available_tables=available_tables,
        selected_tables=selected_tables,
        selected_tables_dataset=selected_tables_dataset,
        available_tables_dataset=available_tables_dataset,
        managed_tables_dataset=managed_tables_dataset,
        relevant_question=relevant_question,
        relevant_min_similarity=relevant_min_similarity,
        relevant_topk=relevant_topk,
        free_style_question=free_style_question,
        summary_question=summary_question,
        chat_question=chat_question,
        answer_text=answer_text,
        answer_chunks_dataset=chunk_dataset,
        citations_by_url=citations_by_url,
        chat_history=chat_history,
        management_message=management_message,
        management_status=management_status,
        create_result=create_result,
        create_result_tabs=create_result_tabs,
        config_values=config_values,
    )
