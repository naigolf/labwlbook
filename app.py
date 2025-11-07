from flask import Flask, render_template, request, send_file, redirect, url_for
import os, re, zipfile, pdfplumber
from PyPDF2 import PdfWriter, PdfReader

app = Flask(__name__)

# --- สร้างโฟลเดอร์ที่จำเป็น ---
UPLOAD_FOLDER = "uploads"
SORTED_DIR = "sorted"
CONSOLIDATED_DIR = "consolidated_by_sku"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(SORTED_DIR, exist_ok=True)
os.makedirs(CONSOLIDATED_DIR, exist_ok=True)

# -------------------------------
# ฟังก์ชันช่วย
# -------------------------------
def extract_order_id(text):
    match = re.search(r"Order ID[: ]+(\d+)", text)
    return match.group(1) if match else None

def extract_barcode(text):
    match = re.search(r"\b\d{10,18}\b", text)
    return match.group(0) if match else None


# -------------------------------
# หน้าแรก (อัปโหลดไฟล์)
# -------------------------------
@app.route("/")
def index():
    return render_template("index.html")


# -------------------------------
# อัปโหลดและประมวลผล PDF
# -------------------------------
@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    if not file or file.filename == "":
        return "❌ No file uploaded", 400

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    # ✅ ขั้นตอน 1: แยก PDF ตาม Order ID + SKU
    reader = PdfReader(filepath)
    writers = {}
    last_order_id = None
    last_sku = None

    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            lines = text.splitlines()

            order_id = extract_order_id(text)
            if order_id:
                last_order_id = order_id
            else:
                order_id = last_order_id

            barcode = extract_barcode(text)
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
            if group_key not in writers:
                writers[group_key] = PdfWriter()
            writers[group_key].add_page(reader.pages[i])

    for group_key, writer in writers.items():
        output_path = os.path.join(SORTED_DIR, f"{group_key}.pdf")
        with open(output_path, "wb") as f:
            writer.write(f)

    # ✅ ขั้นตอน 2: รวมไฟล์ตาม Primary SKU
    order_id_to_primary_sku = {}
    for filename in os.listdir(SORTED_DIR):
        if filename.endswith(".pdf"):
            parts = filename.rsplit("_", 1)
            if len(parts) == 2:
                order_id = parts[0]
                sku = parts[1].replace(".pdf", "")
                if order_id not in order_id_to_primary_sku:
                    order_id_to_primary_sku[order_id] = sku

    sku_writers = {}
    for filename in os.listdir(SORTED_DIR):
        if filename.endswith(".pdf"):
            parts = filename.rsplit("_", 1)
            if len(parts) == 2:
                order_id = parts[0]
                primary_sku = order_id_to_primary_sku.get(order_id)
                if primary_sku:
                    if primary_sku not in sku_writers:
                        sku_writers[primary_sku] = PdfWriter()
                    reader = PdfReader(os.path.join(SORTED_DIR, filename))
                    for page in reader.pages:
                        sku_writers[primary_sku].add_page(page)

    for primary_sku, writer in sku_writers.items():
        output_path = os.path.join(CONSOLIDATED_DIR, f"{primary_sku}.pdf")
        with open(output_path, "wb") as f:
            writer.write(f)

    # ✅ ขั้นตอน 3: รวมไฟล์ทั้งหมดเป็น ZIP
    zip_path = os.path.join(CONSOLIDATED_DIR, "all_results.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in os.listdir(CONSOLIDATED_DIR):
            if file.endswith(".pdf"):
                zipf.write(os.path.join(CONSOLIDATED_DIR, file), arcname=file)

    return send_file(zip_path, as_attachment=True)


# -------------------------------
# รันแอป
# -------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
