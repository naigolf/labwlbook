import os
import zipfile
from flask import Flask, render_template, request, send_file
import pdfplumber
from PyPDF2 import PdfWriter
from werkzeug.utils import secure_filename

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
    if "pdf_file" not in request.files:
        return "No file part"

    file = request.files["pdf_file"]
    if file.filename == "":
        return "No selected file"

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    # เริ่มประมวลผล PDF
    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages):
            writer = PdfWriter()
            writer.add_page(page.to_pdf().pages[0])  # ใช้ PyPDF2 เขียนออก
            output_path = os.path.join(OUTPUT_FOLDER, f"page_{i+1}.pdf")
            with open(output_path, "wb") as f_out:
                writer.write(f_out)

    # รวมเป็น ZIP
    zip_filename = "results.zip"
    zip_path = os.path.join(OUTPUT_FOLDER, zip_filename)
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file_name in os.listdir(OUTPUT_FOLDER):
            if file_name.endswith(".pdf"):
                zipf.write(os.path.join(OUTPUT_FOLDER, file_name), file_name)

    return send_file(zip_path, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
