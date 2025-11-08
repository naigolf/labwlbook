def process_pdf_job(job_id, uploaded_path, original_filename):
    """Main processing job (splitting, grouping, merging)."""
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["progress"] = 0
        jobs[job_id]["message"] = "Preparing..."
        print(f"[{job_id}] 🟡 Starting process for {original_filename}")

        job_sorted = os.path.join(BASE_SORTED, job_id)
        job_consolidated = os.path.join(BASE_CONSOLIDATED, job_id)
        job_zipped = os.path.join(BASE_ZIPPED, job_id)
        for d in [job_sorted, job_consolidated, job_zipped]:
            os.makedirs(d, exist_ok=True)

        # count pages
        with pdfplumber.open(uploaded_path) as pdf_for_count:
            total_pages = len(pdf_for_count.pages) if pdf_for_count.pages else 0
        print(f"[{job_id}] Total pages: {total_pages}")

        jobs[job_id]["message"] = "Splitting pages..."
        reader = PdfReader(uploaded_path)
        writers = {}
        last_order_id, last_sku = None, None
        processed_pages = 0

        with pdfplumber.open(uploaded_path) as pdf:
            for i, page in enumerate(pdf.pages):
                try:
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
                        if group_key not in writers:
                            writers[group_key] = PdfWriter()
                        try:
                            writers[group_key].add_page(reader.pages[i])
                        except Exception as e:
                            print(f"[{job_id}] ⚠️ Error adding page {i}: {e}")
                    else:
                        print(f"[{job_id}] ⚠️ Missing order_id or sku on page {i}")

                    processed_pages += 1
                    if total_pages > 0:
                        jobs[job_id]["progress"] = int(processed_pages / total_pages * 70)
                except Exception as e:
                    print(f"[{job_id}] ❌ Error processing page {i}: {e}")

        print(f"[{job_id}] ✅ Split complete ({len(writers)} groups)")

        # save sorted
        jobs[job_id]["message"] = "Saving sorted PDFs..."
        for group_key, writer in writers.items():
            try:
                out_path = os.path.join(job_sorted, f"{group_key}.pdf")
                with open(out_path, "wb") as f:
                    writer.write(f)
            except Exception as e:
                print(f"[{job_id}] ❌ Error saving sorted {group_key}: {e}")

        print(f"[{job_id}] ✅ Sorted PDFs saved")

        # map primary SKUs
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

        # consolidate
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
                    r = PdfReader(file_path)
                    for p in r.pages:
                        writer.add_page(p)
                except Exception as e:
                    print(f"[{job_id}] ⚠️ Error merging {file_path}: {e}")
            if len(writer.pages) > 0:
                sku_writers[primary_sku] = writer

        print(f"[{job_id}] ✅ Consolidation complete ({len(sku_writers)} SKUs)")

        jobs[job_id]["message"] = "Saving consolidated PDFs..."
        consolidated_files = []
        for primary_sku, writer in sku_writers.items():
            try:
                out_file = os.path.join(job_consolidated, f"{primary_sku}.pdf")
                with open(out_file, "wb") as f:
                    writer.write(f)
                consolidated_files.append(f"{primary_sku}.pdf")
            except Exception as e:
                print(f"[{job_id}] ❌ Error saving consolidated {primary_sku}: {e}")

        jobs[job_id]["files"] = consolidated_files
        jobs[job_id]["progress"] = 90
        jobs[job_id]["message"] = "Finalizing (zipping)..."

        print(f"[{job_id}] ⏳ Starting ZIP background thread ({len(consolidated_files)} files)...")
        threading.Thread(
            target=create_zip_background,
            args=(job_id, consolidated_files, job_consolidated, job_zipped),
            daemon=True
        ).start()

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error: {str(e)}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print(f"[{job_id}] ❌ Fatal error:", e)
        print(traceback.format_exc())


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 10000))  # Render จะส่ง PORT มาให้ตอนรัน
    app.run(host='0.0.0.0', port=port)


