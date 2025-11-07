from flask import Flask, render_template, request, send_file, redirect, url_for
import os
import zipfile
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files["file"]
    if not file:
        return "No file uploaded", 400

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    # เปิดไฟล์ PDF
    reader = PdfReader(filepath)

    for i, page in enumerate(reader.pages):
        writer = PdfWriter()
        writer.add_page(page)

        # สร้างชื่อไฟล์ใหม่แต่ละหน้า
        output_path = os.path.join(OUTPUT_FOLDER, f"{os.path.splitext(file.filename)[0]}_page_{i+1}.pdf")
        with open(output_path, "wb") as f:
            writer.write(f)

    return redirect(url_for("download_all"))

@app.route("/download_all")
def download_all():
    zip_path = os.path.join(OUTPUT_FOLDER, "all_results.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in os.listdir(OUTPUT_FOLDER):
            if file.endswith(".pdf"):
                zipf.write(os.path.join(OUTPUT_FOLDER, file), arcname=file)
    return send_file(zip_path, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
