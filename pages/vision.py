import base64
import json

import mysql.connector
from flask import flash, request

from app_context import (
    answer_query_on_image,
    app,
    choose_default_model,
    get_vision_models,
    login_required,
    render_dashboard,
)


@app.route("/vision", methods=["GET", "POST"])
@login_required
def vision_page():
    answer = ""
    question = ""
    preview_data_url = ""
    llm_models = []
    selected_model = ""

    try:
        llm_models = get_vision_models()
        selected_model = choose_default_model(llm_models)

        if request.method == "POST":
            question = request.form.get("question", "").strip()
            selected_model = request.form.get("llm", "").strip() or selected_model
            uploaded_file = request.files.get("image_file")

            if not uploaded_file or not uploaded_file.filename:
                flash("Upload an image file.", "warning")
            elif not question:
                flash("Enter a question.", "warning")
            elif not selected_model:
                flash("No supported generation models were found for this connection.", "error")
            else:
                image_bytes = uploaded_file.read()
                mime_type = uploaded_file.mimetype or "image/png"
                encoded_image = base64.b64encode(image_bytes).decode("utf-8")
                preview_data_url = "data:{mime};base64,{body}".format(
                    mime=mime_type,
                    body=encoded_image,
                )
                raw_response = answer_query_on_image(question, selected_model, encoded_image)
                payload = json.loads(raw_response or "{}")
                answer = payload.get("text", "")
    except mysql.connector.Error as error:
        flash(str(error), "error")
    except json.JSONDecodeError:
        flash("The vision response could not be parsed.", "error")

    return render_dashboard(
        "vision.html",
        page_title="HWVision",
        llm_models=llm_models,
        selected_model=selected_model,
        question=question,
        answer=answer,
        preview_data_url=preview_data_url,
    )
