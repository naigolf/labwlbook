from flask import Flask, render_template, request, send_file, jsonify
import os, re, zipfile
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def extract_order_id(text):
    match = re.search(r"Order ID[: ]+(\d+)", text)
    return match.group(1) if match else None

def extract_barcode(text):
    match = re.search(r"\b\d{10,18}\b", text)
    return match.group(0) if match else None

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    # ตรวจสอบไฟล์
    if "file" not in request.files:
        return "No file part", 400

    file = request.files["file"]
    if file.filename == "":
        return "No selected file", 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    # เริ่มประมวลผล PDF
    reader = PdfReader(filepath)
    writers = {}
    last_order_id = None
    last_sku = None

    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            lines = text.splitlines()

            # ตรวจ Order ID
            order_id = extract_order_id(text)
            if order_id:
                last_order_id = order_id
            else:
                order_id = last_order_id

            # ตรวจ Barcode
            barcode = extract_barcode(text)

            # หาค่า SKU
            sku = None
            if barcode is None and last_sku is not None:
                sku = last_sku
            else:
                for idx, line in enumerate(lines):
                    if "Product Name" in line and "Seller SKU" in line:
                        if idx + 1 < len(lines):
                            product_line = lines[idx + 1].strip()
                            parts = product_line.split()
                            if len(parts) >= 2:
                                sku = parts[-2]
                        break

            if not sku:
                sku = last_sku if last_sku else f"UNKNOWN_{i}"

            sku = sku.replace("/", "_").replace("\\", "_").strip()
            last_sku = sku

            group_key = f"{order_id}_{sku}"

            # รวมหน้าในกลุ่มเดียวกัน
            if group_key not in writers:
                writers[group_key] = PdfWriter()

            pdf_reader = PdfReader(filepath)
            writers[group_key].add_page(pdf_reader.pages[i])

    # สร้างไฟล์ PDF แยกแต่ละกลุ่ม
    for group_key, writer in writers.items():
        output_path = os.path.join(OUTPUT_FOLDER, f"{group_key}.pdf")
        with open(output_path, "wb") as f:
            writer.write(f)

    # สร้าง ZIP สำหรับดาวน์โหลดทั้งหมด
    zip_path = os.path.join(OUTPUT_FOLDER, "results.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in os.listdir(OUTPUT_FOLDER):
            if file.endswith(".pdf") and file != "results.zip":
                zipf.write(os.path.join(OUTPUT_FOLDER, file), arcname=file)

    return send_file(zip_path, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
