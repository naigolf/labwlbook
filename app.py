from flask import Flask, render_template, request, redirect, url_for, send_file
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
SORTED_FOLDER = 'sorted'
CONSOLIDATED_FOLDER = 'consolidated_by_sku'
ZIPPED_FOLDER = 'zipped_archives' # New folder for zipped files

# Ensure necessary folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(SORTED_FOLDER, exist_ok=True)
os.makedirs(CONSOLIDATED_FOLDER, exist_ok=True)
os.makedirs(ZIPPED_FOLDER, exist_ok=True)

# --- PDF Sorting Logic (Adapted from previous notebook steps) ---
def sort_pdf_by_order_and_sku(input_pdf_path, output_dir):
    reader = PdfReader(input_pdf_path)

    writers = {}
    last_order_id = None
    last_sku = None

    def extract_order_id(text):
        match = re.search(r"Order ID[: ]+(\d+)", text)
        return match.group(1) if match else None

    def extract_barcode(text):
        # Barcode: ตัวเลขติดกันยาว เช่น 10–18 หลัก
        match = re.search(r"\b\d{10,18}\b", text)
        return match.group(0) if match else None

    with pdfplumber.open(input_pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            lines = text.splitlines()

            # ตรวจ Order ID
            order_id = extract_order_id(text)
            if order_id:
                last_order_id = order_id
            else:
                order_id = last_order_id  # ถ้าหน้านี้ไม่มี order id → ใช้ของหน้าเดิม

            # ตรวจว่ามี barcode หรือเปล่า
            barcode = extract_barcode(text)

            sku = None

            # หากหน้าไม่มี barcode → ให้ใช้ sku หน้าเดิม
            if barcode is None and last_sku is not None:
                sku = last_sku
            else:
                # หา SKU จากตารางสินค้า
                for idx, line in enumerate(lines):
                    if "Product Name" in line and "Seller SKU" in line:
                        if idx + 1 < len(lines):
                            product_line = lines[idx + 1].strip()
                            parts = product_line.split()
                            if len(parts) >= 2:
                                sku = parts[-2]    # ใช้ Seller SKU ตัวแรกเท่านั้น
                        break

            if not sku:
                sku = last_sku if last_sku else f"UNKNOWN_{i}"

            sku = sku.replace("/", "_").replace("\\", "_").strip()
            last_sku = sku  # จำ SKU ไว้ใช้ในหน้าไม่มี barcode

            if order_id and sku:
                group_key = f"{order_id}_{sku}"
                if group_key not in writers:
                    writers[group_key] = PdfWriter()

                # Add the page from the original reader object
                try:
                    writers[group_key].add_page(reader.pages[i])
                except Exception as e:
                    print(f"Error adding page {i} from {input_pdf_path} to writer {group_key}: {e}")
            else:
                print(f"Warning: Could not determine Order ID or SKU for page {i} in {input_pdf_path}. Skipping.")

    # บันทึกไฟล์
    sorted_files_count = 0
    for group_key, writer in writers.items():
        if len(writer.pages) > 0:
            output_file_path = os.path.join(output_dir, f"{group_key}.pdf")
            with open(output_file_path, "wb") as f:
                writer.write(f)
            sorted_files_count += 1
    return sorted_files_count

# --- PDF Consolidation Logic ---
def consolidate_pdfs_by_sku(sorted_dir, consolidated_output_dir):
    order_id_to_primary_sku_map = {}

    # 2a, 2b, 2c: Map Order IDs to Primary SKUs
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                sku_with_ext = parts[1]
                sku = sku_with_ext.replace('.pdf', '')

                if order_id not in order_id_to_primary_sku_map:
                    order_id_to_primary_sku_map[order_id] = sku

    # 2d, 2e, 2f, 2g: Group files by primary SKU and order ID
    grouped_files_by_primary_sku = {}
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
            file_path = os.path.join(sorted_dir, filename)
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                primary_sku = order_id_to_primary_sku_map.get(order_id)

                if primary_sku:
                    if primary_sku not in grouped_files_by_primary_sku:
                        grouped_files_by_primary_sku[primary_sku] = []
                    grouped_files_by_primary_sku[primary_sku].append((order_id, file_path))
                else:
                    print(f"Warning: No primary SKU found for Order ID {order_id} from file {filename}. Skipping this file for grouping.")

    # 2h: Consolidate files by primary SKU
    consolidated_files_count = 0
    for primary_sku, files_list in grouped_files_by_primary_sku.items():
        # 2h i: Sort files_list by order_id to ensure consistent merging order
        files_list.sort(key=lambda x: x[0])

        writer = PdfWriter()

        for order_id, file_path in files_list:
            try:
                reader = PdfReader(file_path)
                for page in reader.pages:
                    writer.add_page(page)
            except Exception as e:
                print(f"Error processing {os.path.basename(file_path)} for Order ID {order_id}: {e}")

        if len(writer.pages) > 0:
            # 2h iv: Save the consolidated PDF
            output_filename = f"{primary_sku}.pdf"
            output_file_path = os.path.join(consolidated_output_dir, output_filename)
            with open(output_file_path, "wb") as f:
                writer.write(f)
            consolidated_files_count += 1
    return consolidated_files_count

# --- Zip Archive Creation Logic ---
def create_zip_archive(source_dir, output_zip_path):
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.basename(file_path))
    return output_zip_path

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'pdf_files' not in request.files:
        return redirect(request.url)

    files = request.files.getlist('pdf_files')
    uploaded_count = 0
    total_sorted_pdfs_count = 0

    # Clear previous sorted and consolidated files to ensure fresh processing
    for folder in [SORTED_FOLDER, CONSOLIDATED_FOLDER]:
        for item in os.listdir(folder):
            item_path = os.path.join(folder, item)
            if os.path.isfile(item_path):
                os.remove(item_path)

    for file in files:
        if file and file.filename:
            filename = file.filename
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(file_path)
            uploaded_count += 1

            # Call the sorting function for each uploaded PDF
            num_sorted = sort_pdf_by_order_and_sku(file_path, SORTED_FOLDER)
            total_sorted_pdfs_count += num_sorted

    # After all files are uploaded and sorted, consolidate them
    total_consolidated_pdfs_count = consolidate_pdfs_by_sku(SORTED_FOLDER, CONSOLIDATED_FOLDER)

    zip_filename = f"consolidated_pdfs_{os.urandom(4).hex()}.zip"
    output_zip_path = os.path.join(ZIPPED_FOLDER, zip_filename)
    zip_archive_path = create_zip_archive(CONSOLIDATED_FOLDER, output_zip_path)

    if uploaded_count > 0:
        # Construct the download URL
        download_url = url_for('download_zip', filename=zip_filename)
        return f"<h1>Successfully uploaded {uploaded_count} files!</h1><p>Processed and sorted {total_sorted_pdfs_count} PDF files by Order ID and SKU. Consolidated {total_consolidated_pdfs_count} PDF files by primary SKU. <a href=\"{download_url}\">Download Consolidated PDFs</a><br><a href=\"/\">Upload more</a></p>"
    else:
        return f"<h1>No files selected or uploaded.</h1><p><a href=\"/\">Try again</a></p>"

@app.route('/download/<filename>')
def download_zip(filename):
    return send_file(os.path.join(ZIPPED_FOLDER, filename), as_attachment=True)

# To run the app (only for development purposes, typically in a separate script or with 'flask run')
# if __name__ == '__main__':
#    app.run(debug=True, port=5000)
