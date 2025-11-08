from flask import Flask, render_template, request, send_file, jsonify, url_for
import os, re, zipfile, pdfplumber, threading, uuid, traceback, time
from PyPDF2 import PdfWriter, PdfReader
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_UPLOAD = "uploads"
BASE_SORTED = "sorted"
BASE_CONSOLIDATED = "consolidated_by_sku"
BASE_ZIPPED = "zipped_archives"

for d in [BASE_UPLOAD, BASE_SORTED, BASE_CONSOLIDATED, BASE_ZIPPED]:
    os.makedirs(d, exist_ok=True)

jobs = {}


# ---------- helper functions ----------
def extract_order_id(text):
    m = re.search(r"Order ID[: ]+(\d+)", text)
    return m.group(1) if m else None

def extract_barcode(text):
    m = re.search(r"\b\d{10,18}\b", text)
    return m.group(0) if m else None


def create_zip_background(job_id, consolidated_files, job_consolidated, job_zipped):
    """Runs in background to create zip and finalize job."""
    try:
        jobs[job_id]["message"] = "Creating ZIP archive..."
        zip_name = f"results_{job_id}.zip"
        zip_path = os.path.join(job_zipped, zip_name)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for f in consolidated_files:
                p = os.path.join(job_consolidated, f)
                if os.path.exists(p):
                    zipf.write(p, arcname=f)

        jobs[job_id]["zip"] = zip_path
        jobs[job_id]["progress"] = 100
        jobs[job_id]["status"] = "done"
        jobs[job_id]["message"] = "Completed ✅"
        print(f"[{job_id}] ✅ ZIP created successfully at {zip_path}")

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error creating zip: {e}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print(f"[{job_id}] ❌ ZIP creation failed:", e)
        print(traceback.format_exc())

#
# ⬇️ --- แก้ไขฟังก์ชันนี้อีกครั้งครับ --- ⬇️
#
def process_pdf_job(job_id, uploaded_path, original_filename):
    """Main processing job (splitting, grouping, merging)."""
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["progress"] = 0
        jobs[job_id]["message"] = "Preparing..."

        job_sorted = os.path.join(BASE_SORTED, job_id)
        job_consolidated = os.path.join(BASE_CONSOLIDATED, job_id)
        job_zipped = os.path.join(BASE_ZIPPED, job_id)
        for d in [job_sorted, job_consolidated, job_zipped]:
            os.makedirs(d, exist_ok=True)

        # --- Pass 1: Map pages to groups (Memory Efficient) ---
        jobs[job_id]["message"] = "Analyzing pages..."
        
        page_groups = {} 
        last_order_id, last_sku = None, None
        total_pages = 0

        # *** จุดนี้ถูกต้อง *** 'pdfplumber' ใช้ 'with' ได้
        with pdfplumber.open(uploaded_path) as pdf:
            total_pages = len(pdf.pages) if pdf.pages else 0
            if total_pages == 0:
                raise Exception("PDF file is empty or corrupted.")

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
                                parts = lines[idx + 1].split()
                                if len(parts) >= 2:
                                    sku = parts[-2]
                            break

                if not sku:
                    sku = last_sku if last_sku else f"UNKNOWN_{i}"

                sku = sku.replace("/", "_").replace("\\", "_").strip()
                last_sku = sku

                if order_id and sku:
                    group_key = f"{order_id}_{sku}"
                    page_groups.setdefault(group_key, []).append(i) # Add page index
                else:
                    print(f"Warning: missing order_id or sku on page {i}")

                if total_pages > 0:
                    jobs[job_id]["progress"] = int((i + 1) / total_pages * 50) 

        # --- Pass 2: Save sorted files (Memory Efficient) ---
        jobs[job_id]["message"] = "Splitting files..."
        
        # *** แก้ไขจุดที่ 1 ***
        # 'PdfReader' ใช้ 'with' ไม่ได้ ให้เปิดธรรมดา
        reader = PdfReader(uploaded_path)
        try:
            for group_key, page_indices in page_groups.items():
                writer = PdfWriter() 
                for page_index in page_indices:
                    try:
                        writer.add_page(reader.pages[page_index])
                    except Exception as e:
                        print(f"Error adding page {page_index} to {group_key}: {e}")
                
                out_path = os.path.join(job_sorted, f"{group_key}.pdf")
                with open(out_path, "wb") as f:
                    writer.write(f)
        finally:
            reader.close() # และสั่ง .close() เองตอนจบ
        
        jobs[job_id]["progress"] = 70 

        # --- Pass 3: Consolidate (Fixing file handle leaks) ---
        jobs[job_id]["message"] = "Mapping primary SKUs..."
        order_id_to_primary_sku = {}
        for filename in os.listdir(job_sorted):
            if filename.endswith(".pdf"):
                parts = filename.rsplit("_", 1)
                if len(parts) == 2:
                    order_id = parts[0]
                    sku = parts[1].replace(".pdf", "")
                    if order_id not in order_id_to_primary_sku:
                        order_id_to_primary_sku[order_id] = sku

        jobs[job_id]["message"] = "Consolidating by primary SKU..."
        grouped_files_by_primary_sku = {}
        for filename in os.listdir(job_sorted):
            if filename.endswith(".pdf"):
                file_path = os.path.join(job_sorted, filename)
                parts = filename.rsplit("_", 1)
                if len(parts) == 2:
                    order_id = parts[0]
                    primary_sku = order_id_to_primary_sku.get(order_id)
                    if primary_sku:
                        grouped_files_by_primary_sku.setdefault(primary_sku, []).append((order_id, file_path))

        sku_writers = {}
        for primary_sku, files_list in grouped_files_by_primary_sku.items():
            files_list.sort(key=lambda x: x[0])
            writer = PdfWriter()
            for order_id, file_path in files_list:
                # *** แก้ไขจุดที่ 2 ***
                # ใช้ try...finally เพื่อให้แน่ใจว่า .close()
                r = None 
                try:
                    r = PdfReader(file_path)
                    for p in r.pages:
                        writer.add_page(p)
                except Exception as e:
                    print(f"Error merging {file_path}: {e}")
                finally:
                    if r:
                        r.close() # สั่ง .close() เอง
                        
            if len(writer.pages) > 0:
                sku_writers[primary_sku] = writer

        jobs[job_id]["progress"] = 85 

        jobs[job_id]["message"] = "Saving consolidated PDFs..."
        consolidated_files = []
        for primary_sku, writer in sku_writers.items():
            out_file = os.path.join(job_consolidated, f"{primary_sku}.pdf")
            with open(out_file, "wb") as f:
                writer.write(f)
            consolidated_files.append(f"{primary_sku}.pdf")

        jobs[job_id]["files"] = consolidated_files
        jobs[job_id]["progress"] = 90
        jobs[job_id]["message"] = "Finalizing (zipping)..."

        threading.Thread(
            target=create_zip_background,
            args=(job_id, consolidated_files, job_consolidated, job_zipped),
            daemon=True
        ).start()

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error: {str(e)}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print("Error in process_pdf_job:", e)
        print(traceback.format_exc())
#
# ⬆️ --- สิ้นสุดฟังก์ชันที่แก้ไข --- ⬆️
#

# ---------- Flask routes ----------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" in request.files:
        file = request.files["file"]
    else:
        return "No file", 400

    if file.filename == "":
        return "No filename", 400

    filename = secure_filename(file.filename)
    saved_path = os.path.join(BASE_UPLOAD, filename)
    file.save(saved_path)

    job_id = uuid.uuid4().hex
    jobs[job_id] = {"status": "pending", "progress": 0, "message": "Queued", "files": [], "zip": None}

    threading.Thread(target=process_pdf_job, args=(job_id, saved_path, filename), daemon=True).start()

    return jsonify({"job_id": job_id, "status_url": url_for("job_status", job_id=job_id)}), 202


@app.route("/status/<job_id>")
def job_status(job_id):
    info = jobs.get(job_id)
    if not info:
        return jsonify({"error": "no such job"}), 404
    result = {
        "status": info["status"],
        "progress": info.get("progress", 0),
        "message": info.get("message", ""),
        "files": info.get("files", []),
        "zip": None
    }
    if info.get("zip"):
        result["zip"] = url_for("download_job_zip", job_id=job_id)
    if info.get("status") == "error":
        result["traceback"] = info.get("traceback")
    return jsonify(result)


@app.route("/download/<job_id>")
def download_job_zip(job_id):
    info = jobs.get(job_id)
    if not info or not info.get("zip"):
        return "No such job or zip not ready", 404
    return send_file(info["zip"], as_attachment=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
