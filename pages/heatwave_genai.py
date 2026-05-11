import importlib
import json
import os
import re
import ssl
import urllib.request

import mysql.connector
from flask import flash, request, session

from app_context import (
    _normalize_modal_cell,
    _mysql_quote,
    _quote_identifier,
    _table_exists,
    app,
    choose_default_model,
    close_mysql_connection,
    get_connection_config,
    get_embedding_models,
    get_generation_models,
    login_required,
    mysql_connection,
    render_dashboard,
    run_sql,
    run_sql_with_columns,
)


GENAI_TABS = [
    {"id": "create-kb", "label": "Create KB"},
    {"id": "search-kb", "label": "Search KB"},
]
DEFAULT_EMBED_MODEL = "multilingual-e5-small"
DEFAULT_GENERATE_MODEL = ""
DEFAULT_VECTOR_TABLE = "web_embeddings"
DEFAULT_SOURCE_URL = "https://en.wikipedia.org/wiki/Oracle_Corporation"
DEFAULT_SEARCH_QUERY = "How do I create a HeatWave knowledge base?"
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}


def _normalize_genai_tab(raw_value):
    value = str(raw_value or "create-kb").strip().lower()
    return value if value in {tab["id"] for tab in GENAI_TABS} else "create-kb"


def _safe_int(raw_value, default_value, *, minimum=1, maximum=None):
    try:
        value = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        value = default_value
    if value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _normalize_text(value):
    return str(_normalize_modal_cell(value)).strip()


def _validate_object_name(value, label):
    normalized = _normalize_text(value)
    if not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        raise ValueError("{} contains unsupported characters.".format(label))
    return normalized


def _is_checked(value):
    return str(value or "").strip().lower() in {"1", "true", "y", "yes", "on"}


def _optional_import(module_name):
    try:
        return importlib.import_module(module_name), None
    except Exception as error:  # pragma: no cover - import errors depend on environment
        return None, str(error)


def _require_modules(*module_names):
    modules = {}
    errors = []
    for module_name in module_names:
        module, error = _optional_import(module_name)
        if error:
            errors.append("{} ({})".format(module_name, error))
        else:
            modules[module_name] = module
    if errors:
        raise RuntimeError(
            "Missing required package(s): {}. Install the updated requirements first.".format(
                ", ".join(errors)
            )
        )
    return modules


def _fetch_url_text(source_url):
    request_obj = urllib.request.Request(source_url, headers=HTTP_HEADERS)
    certifi_module, _ = _optional_import("certifi")
    ssl_context = None
    if certifi_module is not None:
        ssl_context = ssl.create_default_context(cafile=certifi_module.where())
    else:
        ssl_context = ssl.create_default_context()
    try:
        with urllib.request.urlopen(request_obj, timeout=90, context=ssl_context) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except ssl.SSLCertVerificationError as error:
        raise RuntimeError(
            "HTTPS certificate verification failed while fetching the URL: {}. "
            "Install the updated requirements to use certifi, or fix the local CA trust store.".format(error)
        ) from error


def _extract_text_blocks(html_text):
    bs4_module, _ = _optional_import("bs4")
    if bs4_module is not None:
        soup = bs4_module.BeautifulSoup(html_text, "html.parser")
        for element in soup(["script", "style", "noscript"]):
            element.decompose()
        blocks = []
        for element in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
            text = " ".join(element.get_text(" ", strip=True).split())
            if text:
                blocks.append(text)
        if blocks:
            return blocks

    stripped = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", html_text)
    stripped = re.sub(r"(?s)<[^>]+>", " ", stripped)
    plain_text = re.sub(r"\s+", " ", stripped).strip()
    return [plain_text] if plain_text else []


def _chunk_blocks(blocks, chunk_size=1400):
    chunks = []
    current = []
    current_length = 0
    for block in blocks:
        normalized_block = _normalize_text(block)
        if not normalized_block:
            continue
        if len(normalized_block) > chunk_size:
            words = normalized_block.split()
            split_chunk = []
            split_length = 0
            for word in words:
                additional = len(word) + (1 if split_chunk else 0)
                if split_length + additional > chunk_size and split_chunk:
                    chunks.append(" ".join(split_chunk))
                    split_chunk = [word]
                    split_length = len(word)
                else:
                    split_chunk.append(word)
                    split_length += additional
            if split_chunk:
                if current:
                    chunks.append("\n\n".join(current))
                    current = []
                    current_length = 0
                chunks.append(" ".join(split_chunk))
            continue

        additional = len(normalized_block) + (2 if current else 0)
        if current and current_length + additional > chunk_size:
            chunks.append("\n\n".join(current))
            current = [normalized_block]
            current_length = len(normalized_block)
        else:
            current.append(normalized_block)
            current_length += additional
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def _vector_to_string(vector_value):
    if vector_value is None:
        return "[]"
    if isinstance(vector_value, str):
        text = vector_value.strip()
        if text.startswith("[") and text.endswith("]"):
            return text
        return "[{}]".format(text)
    if isinstance(vector_value, (bytes, bytearray)):
        text = vector_value.decode("utf-8", errors="replace").strip()
        if text.startswith("[") and text.endswith("]"):
            return text
        return "[{}]".format(text)
    try:
        return "[{}]".format(",".join(str(value) for value in vector_value))
    except TypeError:
        return "[{}]".format(str(vector_value))


def _generate_text(prompt, model_id):
    rows = run_sql(
        """
        select sys.ML_GENERATE(
            %s,
            JSON_OBJECT('task', 'generation', 'model_id', %s)
        ) as response_value
        """,
        (prompt, model_id),
        include_database=False,
    )
    return rows[0][0] if rows else ""


def _format_generated_answer(raw_value):
    text_value = _normalize_text(raw_value)
    if not text_value:
        return ""
    try:
        parsed_value = json.loads(text_value)
    except json.JSONDecodeError:
        return text_value
    if isinstance(parsed_value, dict):
        extracted_text = parsed_value.get("text")
        if extracted_text is not None:
            return _normalize_text(extracted_text).replace("\\n", "\n")
        return json.dumps(parsed_value, indent=2)
    return text_value


def _ensure_vector_table(schema_name, table_name):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute(
            """
            create table if not exists {schema_name}.{table_name} (
                id bigint unsigned auto_increment,
                content longtext,
                vec vector(2048) null,
                source_url varchar(4000),
                primary key (id)
            )
            """.format(
                schema_name=_quote_identifier(schema_name),
                table_name=_quote_identifier(table_name),
            )
        )
        cursor.execute(
            """
            alter table {schema_name}.{table_name}
            modify column vec vector(2048) null
            """.format(
                schema_name=_quote_identifier(schema_name),
                table_name=_quote_identifier(table_name),
            )
        )
        cursor.execute(
            """
            drop table if exists {schema_name}.{trx_table_name}
            """.format(
                schema_name=_quote_identifier(schema_name),
                trx_table_name=_quote_identifier("{}_trx".format(table_name)),
            )
        )
        cursor.execute(
            """
            create table {schema_name}.{trx_table_name} (
                id bigint unsigned,
                content longtext,
                primary key (id)
            )
            """.format(
                schema_name=_quote_identifier(schema_name),
                trx_table_name=_quote_identifier("{}_trx".format(table_name)),
            )
        )
        cnx.commit()
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _ensure_schema(schema_name):
    normalized_schema_name = _validate_object_name(schema_name, "Schema name")
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute(
            "create database if not exists {}".format(_quote_identifier(normalized_schema_name))
        )
        cnx.commit()
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _insert_content_rows(schema_name, table_name, chunks, source_url):
    cnx = None
    cursor = None
    inserted = 0
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        sql_text = """
            insert into {schema_name}.{table_name} (content, source_url)
            values (%s, %s)
        """.format(
            schema_name=_quote_identifier(schema_name),
            table_name=_quote_identifier(table_name),
        )
        for chunk in chunks:
            cursor.execute(sql_text, (chunk, source_url))
            inserted += 1
        cnx.commit()
        return inserted
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


def _get_last_vector_row_id(schema_name, table_name):
    rows = run_sql(
        """
        select coalesce(max(id), 0)
        from {schema_name}.{table_name}
        """.format(
            schema_name=_quote_identifier(schema_name),
            table_name=_quote_identifier(table_name),
        ),
        include_database=False,
    )
    return int(rows[0][0] or 0) if rows else 0


def _call_embed_sp(cursor, schema_name, table_name, last_id, embed_model_id):
    trx_table_name = "{}_trx".format(table_name)
    cursor.execute(
        """
        insert into {schema_name}.{trx_table_name} (id, content)
        select id, content
        from {schema_name}.{table_name}
        where id > %s
        """.format(
            schema_name=_quote_identifier(schema_name),
            trx_table_name=_quote_identifier(trx_table_name),
            table_name=_quote_identifier(table_name),
        ),
        (last_id,),
    )
    cursor.execute(
        """
        call sys.ML_EMBED_TABLE({content_column}, {vector_column}, JSON_OBJECT("model_id", {embed_model_id}))
        """.format(
            content_column=_mysql_quote("{}.{}.content".format(schema_name, trx_table_name)),
            vector_column=_mysql_quote("{}.{}.vec".format(schema_name, trx_table_name)),
            embed_model_id=_mysql_quote(embed_model_id),
        )
    )
    while cursor.nextset():
        pass
    cursor.execute(
        """
        update {schema_name}.{table_name} a, {schema_name}.{trx_table_name} b
        set a.vec = b.vec
        where a.id = b.id
        """.format(
            schema_name=_quote_identifier(schema_name),
            table_name=_quote_identifier(table_name),
            trx_table_name=_quote_identifier(trx_table_name),
        )
    )


def _embed_search_question(question_text, embed_model_id):
    rows = run_sql(
        """
        select sys.ML_EMBED_ROW(%s, JSON_OBJECT('model_id', %s))
        """,
        (question_text, embed_model_id),
        include_database=False,
    )
    return rows[0][0] if rows else None


def _fetch_kb_summary(schema_name, table_name):
    return run_sql_with_columns(
        """
        select source_url, count(*) as chunk_count
        from {schema_name}.{table_name}
        group by source_url
        order by chunk_count desc, source_url
        """.format(
            schema_name=_quote_identifier(schema_name),
            table_name=_quote_identifier(table_name),
        ),
        include_database=False,
    )


def _search_vectors(schema_name, table_name, question_embedding, search_limit):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        vector_text = _vector_to_string(question_embedding)
        cursor.execute(
            """
            select id, content, source_url
            from {schema_name}.{table_name}
            order by vector_distance(vec, string_to_vector(%s)) desc
            limit %s
            """.format(
                schema_name=_quote_identifier(schema_name),
                table_name=_quote_identifier(table_name),
            ),
            (vector_text, search_limit),
        )
        return [list(row) for row in cursor.fetchall()]
    finally:
        close_mysql_connection(cnx)


def _build_documents_table(rows):
    return {
        "columns": ["id", "snippet", "url"],
        "rows": [[row[0], row[1], row[2]] for row in rows],
    }


def _fetch_supported_embedding_models():
    return get_embedding_models()


def _choose_default_embedding_model(models):
    if DEFAULT_EMBED_MODEL in models:
        return DEFAULT_EMBED_MODEL
    return models[0] if models else DEFAULT_EMBED_MODEL


def _fetch_supported_generation_models():
    return get_generation_models()


def _fetch_available_schemas():
    rows = run_sql(
        """
        select schema_name
        from information_schema.schemata
        order by schema_name
        """,
        include_database=False,
    )
    return [str(row[0]).strip() for row in rows if row and row[0]]


def _fetch_schemas_with_table(table_name):
    rows = run_sql(
        """
        select table_schema
        from information_schema.tables
        where table_name = %s
        order by table_schema
        """,
        (table_name,),
        include_database=False,
    )
    return [str(row[0]).strip() for row in rows if row and row[0]]


def create_knowledge_base_from_client_content(
    schema_name,
    table_name,
    contents,
    source_url,
    embed_model_id,
    create_schema=False,
):
    normalized_schema_name = _validate_object_name(schema_name, "Schema name")
    normalized_table_name = _validate_object_name(table_name, "Table name")
    if create_schema:
        _ensure_schema(normalized_schema_name)
    _ensure_vector_table(normalized_schema_name, normalized_table_name)

    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        last_id = _get_last_vector_row_id(normalized_schema_name, normalized_table_name)
        inserted_count = 0
        insert_sql = """
            insert into {schema_name}.{table_name} (content, source_url)
            values (%s, %s)
        """.format(
            schema_name=_quote_identifier(normalized_schema_name),
            table_name=_quote_identifier(normalized_table_name),
        )
        for chunk in contents:
            cursor.execute(insert_sql, (chunk, source_url))
            inserted_count += 1
        cnx.commit()

        _call_embed_sp(cursor, normalized_schema_name, normalized_table_name, last_id, embed_model_id)
        cnx.commit()
        return {
            "chunk_count": len(contents),
            "inserted_count": inserted_count,
            "source_url": source_url,
            "embed_model_id": embed_model_id,
        }
    except mysql.connector.Error:
        raise
    finally:
        close_mysql_connection(cnx)


@app.route("/heatwave-genai", methods=["GET", "POST"])
@login_required
def heatwave_genai_page():
    source = request.form if request.method == "POST" else request.args
    active_tab = _normalize_genai_tab(source.get("tab", "create-kb"))
    page_error = ""

    try:
        available_schemas = _fetch_available_schemas()
        schemas_with_table = _fetch_schemas_with_table(table_name=_normalize_text(source.get("table_name", DEFAULT_VECTOR_TABLE)) or DEFAULT_VECTOR_TABLE)
        embedding_models = _fetch_supported_embedding_models()
        generation_models = _fetch_supported_generation_models()
    except mysql.connector.Error as error:
        available_schemas = []
        schemas_with_table = []
        embedding_models = []
        generation_models = []
        page_error = str(error)
        flash(page_error, "error")

    default_embed_model = _choose_default_embedding_model(embedding_models)
    default_generate_model = choose_default_model(generation_models)
    schema_name = _normalize_text(source.get("schema_name", session.get("connection_profile", {}).get("database", "")))
    table_name = _normalize_text(source.get("table_name", DEFAULT_VECTOR_TABLE)) or DEFAULT_VECTOR_TABLE
    create_new_db = _is_checked(source.get("create_new_db", ""))
    embed_model_id = _normalize_text(source.get("embed_model_id", default_embed_model)) or default_embed_model
    generate_model_id = _normalize_text(source.get("generate_model_id", default_generate_model)) or default_generate_model
    source_url = _normalize_text(source.get("source_url", DEFAULT_SOURCE_URL)) or DEFAULT_SOURCE_URL
    search_query = _normalize_text(source.get("search_query", DEFAULT_SEARCH_QUERY)) or DEFAULT_SEARCH_QUERY
    search_limit = _safe_int(source.get("search_limit", "10"), 10, minimum=1, maximum=50)

    create_result = None
    create_summary = {"columns": [], "rows": []}
    create_chunks_preview = {"columns": [], "rows": []}
    search_answer = ""
    search_documents = {"columns": [], "rows": []}
    search_candidates = {"columns": [], "rows": []}

    if schema_name and schema_name not in available_schemas:
        available_schemas = [schema_name] + available_schemas
    search_table_exists = bool(schema_name and _table_exists(schema_name, table_name))
    if embed_model_id and embed_model_id not in embedding_models:
        embedding_models = [embed_model_id] + embedding_models
    if generate_model_id and generate_model_id not in generation_models:
        generation_models = [generate_model_id] + generation_models

    if request.method == "POST":
        action = _normalize_text(request.form.get("genai_action", ""))
        try:
            if not schema_name:
                raise ValueError("Enter a target schema name.")
            if active_tab == "create-kb" and action == "create_kb":
                if not source_url:
                    raise ValueError("Enter a source URL.")
                html_text = _fetch_url_text(source_url)
                text_blocks = _extract_text_blocks(html_text)
                chunks = _chunk_blocks(text_blocks)
                if not chunks:
                    raise ValueError("No text content could be extracted from the URL.")
                create_result = create_knowledge_base_from_client_content(
                    schema_name,
                    table_name,
                    chunks,
                    source_url,
                    embed_model_id,
                    create_new_db,
                )
                create_summary = _fetch_kb_summary(schema_name, table_name)
                create_chunks_preview = {
                    "columns": ["chunk_index", "content"],
                    "rows": [[index + 1, chunk] for index, chunk in enumerate(chunks[:10])],
                }
                flash("Knowledge base content embedded and stored.", "success")

            elif active_tab == "search-kb" and action == "search_kb":
                if not search_query:
                    raise ValueError("Enter a search question.")
                if not search_table_exists:
                    raise ValueError("The selected schema does not contain the knowledge base table {}.".format(table_name))
                if not generate_model_id:
                    raise ValueError("No supported generation model was found for this connection.")
                question_embedding = _embed_search_question(search_query, embed_model_id)
                if question_embedding is None:
                    raise ValueError("The search question embedding could not be generated.")
                candidate_rows = _search_vectors(schema_name, table_name, question_embedding, search_limit)
                if not candidate_rows:
                    raise ValueError("No vector rows were found in the selected knowledge base table.")
                context_documents = [row[1] for row in candidate_rows]
                prompt = (
                    "Text:\n{documents}\n\n"
                    "Question:\n{question}\n\n"
                    "Answer the question in simple format based on the Text provided. "
                    "If the Text does not provide the answer, reply that the answer is not available."
                ).format(
                    documents="\n\n".join(context_documents),
                    question=search_query,
                )
                search_answer = _format_generated_answer(_generate_text(prompt, generate_model_id))
                search_documents = _build_documents_table(candidate_rows)
                search_candidates = _build_documents_table(candidate_rows)
                flash("Knowledge base search completed.", "success")
        except (ValueError, RuntimeError, OSError, urllib.error.URLError, mysql.connector.Error) as error:
            page_error = str(error)
            flash(page_error, "error")

    return render_dashboard(
        "heatwave_genai.html",
        page_title="GenAI",
        tabs=GENAI_TABS,
        active_tab=active_tab,
        available_schemas=available_schemas,
        schemas_with_table=schemas_with_table,
        schema_name=schema_name,
        table_name=table_name,
        search_table_exists=search_table_exists,
        create_new_db=create_new_db,
        embed_model_id=embed_model_id,
        embedding_models=embedding_models,
        generate_model_id=generate_model_id,
        generation_models=generation_models,
        source_url=source_url,
        search_query=search_query,
        search_limit=search_limit,
        create_result=create_result,
        create_summary=create_summary,
        create_chunks_preview=create_chunks_preview,
        search_answer=search_answer,
        search_documents=search_documents,
        search_candidates=search_candidates,
        page_error=page_error,
    )
