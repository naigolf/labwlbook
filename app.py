from flask import Flask, render_template, request, send_file, jsonify
import os
import zipfile
from werkzeug.utils import secure_filename
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
    # ตรวจสอบว่า form มีไฟล์ไหม
    if "file" not in request.files:
        return "No file part", 400

    file = request.files["file"]
    if file.filename == "":
        return "No selected file", 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    # ลบไฟล์เก่าที่ OUTPUT_FOLDER ก่อนเริ่มใหม่
    for f in os.listdir(OUTPUT_FOLDER):
        os.remove(os.path.join(OUTPUT_FOLDER, f))

    # เปิดและแยกหน้า PDF
    reader = PdfReader(filepath)
    for i, page in enumerate(reader.pages):
        writer = PdfWriter()
        writer.add_page(page)

        output_name = f"{os.path.splitext(filename)[0]}_page_{i+1}.pdf"
        output_path = os.path.join(OUTPUT_FOLDER, output_name)

        with open(output_path, "wb") as f_out:
            writer.write(f_out)

    # สร้างไฟล์ ZIP รวมทั้งหมด
    zip_path = os.path.join(OUTPUT_FOLDER, "all_results.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for pdf_file in os.listdir(OUTPUT_FOLDER):
            if pdf_file.endswith(".pdf"):
                zipf.write(os.path.join(OUTPUT_FOLDER, pdf_file), arcname=pdf_file)

    # ส่งกลับ JSON สำหรับ frontend แสดงปุ่มดาวน์โหลด
    return jsonify({"status": "success", "download_url": "/download_all"})


@app.route("/download_all")
def download_all():
    zip_path = os.path.join(OUTPUT_FOLDER, "all_results.zip")
    if not os.path.exists(zip_path):
        return "No results found", 404
    return send_file(zip_path, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
