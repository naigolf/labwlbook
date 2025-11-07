from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify
import os
import pdfplumber
from PyPDF2 import PdfReader
from zipfile import ZipFile

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route('/')
def index():
    processed_files = os.listdir(OUTPUT_FOLDER)
    return render_template("index.html", processed_files=processed_files)

@app.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist("pdf_files")
    for file in files:
        if file.filename.endswith(".pdf"):
            path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(path)
    return redirect(url_for("index"))

@app.route('/process', methods=['POST'])
def process_files():
    pdf_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.endswith(".pdf")]
    processed_files = []

    for pdf_file in pdf_files:
        pdf_path = os.path.join(UPLOAD_FOLDER, pdf_file)
        text_path = os.path.join(OUTPUT_FOLDER, pdf_file.replace(".pdf", ".txt"))

        # ดึงข้อความจาก PDF
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""

        with open(text_path, "w", encoding="utf-8") as f:
            f.write(text)

        processed_files.append(pdf_file)

    return jsonify({
        "status": "done",
        "processed_files": processed_files
    })

@app.route('/download_all')
def download_all():
    zip_path = "all_texts.zip"
    with ZipFile(zip_path, "w") as zipf:
        for file in os.listdir(OUTPUT_FOLDER):
            zipf.write(os.path.join(OUTPUT_FOLDER, file), arcname=file)
    return send_file(zip_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
