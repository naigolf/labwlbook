from flask import Flask, render_template, request, send_file, jsonify, url_for
import os, re, zipfile, pdfplumber, threading, uuid, traceback
from PyPDF2 import PdfWriter, PdfReader
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_UPLOAD = "uploads"
BASE_SORTED = "sorted"
BASE_CONSOLIDATED = "consolidated_by_sku"
BASE_ZIPPED = "zipped_archives"

# ensure base dirs exist
for d in [BASE_UPLOAD, BASE_SORTED, BASE_CONSOLIDATED, BASE_ZIPPED]:
    os.makedirs(d, exist_ok=True)

# in-memory job store (simple). Structure:
# jobs[job_id] = {
#   "status": "pending"|"running"|"done"|"error",
#   "progress": 0,
#   "message": "...",
#   "files": [...],  # consolidated files names
#   "zip": zip_path or None,
# }
jobs = {}

# helper regex functions (same logic from your colab)
def extract_order_id(text):
    m = re.search(r"Order ID[: ]+(\d+)", text)
    return m.group(1) if m else None

def extract_barcode(text):
    m = re.search(r"\b\d{10,18}\b", text)
    return m.group(0) if m else None

def process_pdf_job(job_id, uploaded_path, original_filename):
    """
    Do the same steps as your Colab:
      - split pages -> sorted by order_id + sku into job-specific sorted dir
      - map primary sku per order id and consolidate by sku
      - zip consolidated results
    Update jobs[job_id]["progress"] as pages processed.
    """
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["progress"] = 0
        jobs[job_id]["message"] = "Preparing..."

        # make job-specific folders
        job_sorted = os.path.join(BASE_SORTED, job_id)
        job_consolidated = os.path.join(BASE_CONSOLIDATED, job_id)
        job_zipped = os.path.join(BASE_ZIPPED, job_id)
        for d in [job_sorted, job_consolidated, job_zipped]:
            os.makedirs(d, exist_ok=True)

        # Read total pages to compute progress increments
        with pdfplumber.open(uploaded_path) as pdf_for_count:
            total_pages = len(pdf_for_count.pages) if pdf_for_count.pages else 0

        # Step 1: split pages into grouped writers
        jobs[job_id]["message"] = "Splitting pages..."
        reader = PdfReader(uploaded_path)
        writers = {}
        last_order_id = None
        last_sku = None
        processed_pages = 0

        with pdfplumber.open(uploaded_path) as pdf:
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

                if order_id and sku:
                    group_key = f"{order_id}_{sku}"
                    if group_key not in writers:
                        writers[group_key] = PdfWriter()
                    # add page from reader to writer
                    try:
                        writers[group_key].add_page(reader.pages[i])
                    except Exception as e:
                        # skip problematic page but keep going
                        print(f"Error adding page {i}: {e}")
                else:
                    print(f"Warning: missing order_id or sku on page {i}")

                # update progress
                processed_pages += 1
                if total_pages > 0:
                    jobs[job_id]["progress"] = int(processed_pages / total_pages * 100)
                else:
                    jobs[job_id]["progress"] = 0

        # save sorted PDFs
        jobs[job_id]["message"] = "Saving sorted PDFs..."
        for group_key, writer in writers.items():
            out_path = os.path.join(job_sorted, f"{group_key}.pdf")
            with open(out_path, "wb") as f:
                writer.write(f)

        # Step 2: map each order to its primary sku
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

        # Step 3: group by primary sku and consolidate
        jobs[job_id]["message"] = "Consolidating by primary SKU..."
        sku_writers = {}
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

        # sort by order_id then merge pages
        for primary_sku, files_list in grouped_files_by_primary_sku.items():
            files_list.sort(key=lambda x: x[0])
            writer = PdfWriter()
            for order_id, file_path in files_list:
                try:
                    reader = PdfReader(file_path)
                    for p in reader.pages:
                        writer.add_page(p)
                except Exception as e:
                    print(f"Error merging {file_path}: {e}")
            if len(writer.pages) > 0:
                sku_writers[primary_sku] = writer

        # save consolidated PDFs
        jobs[job_id]["message"] = "Saving consolidated PDFs..."
        consolidated_files = []
        for primary_sku, writer in sku_writers.items():
            out_file = os.path.join(job_consolidated, f"{primary_sku}.pdf")
            with open(out_file, "wb") as f:
                writer.write(f)
            consolidated_files.append(f"{primary_sku}.pdf")

        jobs[job_id]["files"] = consolidated_files

        # zip consolidated folder
        jobs[job_id]["message"] = "Creating ZIP archive..."
        zip_name = f"results_{job_id}.zip"
        zip_path = os.path.join(job_zipped, zip_name)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for f in consolidated_files:
                p = os.path.join(job_consolidated, f)
                zipf.write(p, arcname=f)

        jobs[job_id]["zip"] = zip_path
        jobs[job_id]["progress"] = 100
        jobs[job_id]["status"] = "done"
        jobs[job_id]["message"] = "Completed"

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error: {str(e)}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print("Error in process_pdf_job:", e)
        print(traceback.format_exc())


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    # supports single file or multiple named 'pdf_files'
    if "file" in request.files:
        file = request.files["file"]
        files = [file]
    elif "pdf_files" in request.files:
        files = request.files.getlist("pdf_files")
    else:
        return "No file part", 400

    # For this app we expect single upload (but code supports multiple)
    if len(files) == 0:
        return "No files", 400

    file = files[0]
    if file.filename == "":
        return "No filename", 400

    filename = secure_filename(file.filename)
    saved_path = os.path.join(BASE_UPLOAD, filename)
    file.save(saved_path)

    # create job
    job_id = uuid.uuid4().hex
    jobs[job_id] = {"status": "pending", "progress": 0, "message": "Queued", "files": [], "zip": None}

    # start background thread
    thread = threading.Thread(target=process_pdf_job, args=(job_id, saved_path, filename), daemon=True)
    thread.start()

    # return job id to client
    return jsonify({"job_id": job_id, "status_url": url_for("job_status", job_id=job_id)}), 202


@app.route("/status/<job_id>")
def job_status(job_id):
    info = jobs.get(job_id)
    if not info:
        return jsonify({"error": "no such job"}), 404
    # return progress, message, files and download url if done
    result = {
        "status": info["status"],
        "progress": info.get("progress", 0),
        "message": info.get("message", ""),
        "files": info.get("files", []),
        "zip": None
    }
    if info.get("zip"):
        # expose download endpoint
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
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
