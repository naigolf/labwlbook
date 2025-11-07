import os
import pdfplumber
from PyPDF2 import PdfReader

# สร้างโฟลเดอร์เก็บไฟล์ PDF
os.makedirs("files", exist_ok=True)

def extract_text_from_pdf(file_path):
    """ดึงข้อความจาก PDF ด้วย pdfplumber"""
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"
    return text

if __name__ == "__main__":
    print("🚀 Running PDF extraction script...")
    
    pdf_folder = "files"
    pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith(".pdf")]

    if not pdf_files:
        print("❌ ไม่พบไฟล์ PDF ในโฟลเดอร์ /files")
    else:
        for pdf_file in pdf_files:
            path = os.path.join(pdf_folder, pdf_file)
            text = extract_text_from_pdf(path)
            print(f"✅ Extracted from: {pdf_file}")
            print("---- ตัวอย่างเนื้อหา ----")
            print(text[:500])  # แสดงแค่ 500 ตัวอักษรแรก
            print("------------------------")

    print("🎉 Done.")
