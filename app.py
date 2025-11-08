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
        
        page_groups = {} # e.g., {'123_SKU-A': [0, 1], '123_SKU-B': [2]}
        last_order_id, last_sku = None, None
        total_pages = 0

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
                    # 0-50% for analysis
                    jobs[job_id]["progress"] = int((i + 1) / total_pages * 50) 

        # --- Pass 2: Save sorted files (Memory Efficient) ---
        jobs[job_id]["message"] = "Splitting files..."
        
        # Use 'with' for the reader to ensure it's closed
        with PdfReader(uploaded_path) as reader:
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
        
        # 50-70% for splitting
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
                try:
                    # *** THIS IS THE FIX ***
                    # Use 'with' to auto-close the file reader
                    with PdfReader(file_path) as r: 
                        for p in r.pages:
                            writer.add_page(p)
                except Exception as e:
                    print(f"Error merging {file_path}: {e}")
            if len(writer.pages) > 0:
                sku_writers[primary_sku] = writer

        # 70-85% for consolidation
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

        # Run ZIP creation in background
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
