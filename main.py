import os
import io
import json
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai
from pdf2image import convert_from_bytes
from PIL import Image

# --- Configuration & Startup Check ---
try:
    GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
    genai.configure(api_key=GEMINI_API_KEY)
except KeyError:
    raise RuntimeError("FATAL: GEMINI_API_KEY environment variable not set.")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

model = genai.GenerativeModel("gemini-1.5-flash-latest")

# --- PROMPTS ---

def create_discovery_prompt():
    """Creates a prompt to discover the column headers from the document."""
    return """
    Analyze the provided image of a ledger or bill.
    Your first task is to identify all the unique charge or expense column headers in the table.
    List only the names of these charge columns.
    Return the result as a clean, single-line, comma-separated string.
    Example: Property Tax,Water Charges,Sinking Fund,Maint. Charges
    """

def create_extraction_prompt(discovered_headers):
    """Creates a dynamic prompt with robust instructions for handling messy data."""
    return f"""
    You are an expert data entry clerk. Analyze the provided image of a ledger.
    Based on the following charge categories that were discovered from this document: {', '.join(discovered_headers)}
    Your task is to extract information for every single member listed and structure the data into a valid JSON array.

    RULES FOR EXTRACTION:
    1. For each member, you MUST extract their "Wing", "Unit No", and "Member Name".
    2. If a "Member Name" is not explicitly given for a row, set its value to null.
    3. For each discovered charge category, extract the corresponding monetary value for the member.
    4. If a member does not have a value for a specific charge (i.e., the cell is blank), you MUST represent it as null in the output.
    5. The final output must be a clean, raw JSON array and nothing else.

    The JSON schema you must follow is:
    [
        {{
            "Wing": "string or null",
            "Unit No": "string or null",
            "Member Name": "string or null",
            "Charges": {{
                "{discovered_headers[0]}": "float or null",
                "{discovered_headers[1]}": "float or null",
                "...and so on for all discovered headers"
            }}
        }}
    ]
    """

def create_direct_export_prompt():
    """Creates a simpler prompt for direct-to-Excel export."""
    return """
    You are an expert at table data extraction.
    Analyze the provided image and identify the main table.
    Extract all the data from the table exactly as it appears, row by row.
    Return the result as a valid JSON array of objects, where each object represents a row.
    Use the table's headers as the keys for each object.
    Do not add, omit, or transform any data. Preserve the exact structure and content.
    The output must only be the raw JSON array.
    """

# --- Reusable function to handle file upload and image conversion ---
async def get_image_from_upload(file: UploadFile):
    if not file.content_type in ["image/jpeg", "image/png", "application/pdf"]:
        raise HTTPException(status_code=400, detail="Unsupported file type.")
    
    file_bytes = await file.read()

    if file.content_type == "application/pdf":
        try:
            images = convert_from_bytes(file_bytes, first_page=1, last_page=1)
            if not images:
                raise HTTPException(status_code=400, detail="Could not extract an image from the PDF.")
            return images[0]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"PDF processing failed: {e}")
    else:
        return Image.open(io.BytesIO(file_bytes))


# --- ENDPOINT 1: PROCESS DOCUMENT INTO SPECIFIC TEMPLATE ---
@app.post("/process-document/")
async def process_document(file: UploadFile = File(...)):
    image_to_process = await get_image_from_upload(file)

    try:
        discovery_prompt = create_discovery_prompt()
        response = model.generate_content([discovery_prompt, image_to_process])
        header_text = response.text.strip()
        discovered_headers = [h.strip() for h in header_text.split(',') if h.strip()]
        if not discovered_headers:
             raise HTTPException(status_code=400, detail="Could not identify any expense headers in the document.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to discover headers from the document: {e}")

    try:
        extraction_prompt = create_extraction_prompt(discovered_headers)
        response = model.generate_content([extraction_prompt, image_to_process])
        json_text = response.text.strip().replace("```json", "").replace("```", "")
        extracted_data = json.loads(json_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during data extraction: {e}")

    all_rows = []
    for member in extracted_data:
        # **FIXED LINE IS HERE** - Added the missing '}' at the end of this dictionary definition
        flat_row = {
            "Bill Number": f"{member.get('Wing', '') or ''}-{member.get('Unit No', '') or ''}".strip('-'),
            "Bill Date": None,
            "Vendor Code": None,
            "Due Date": None,
            "Narration": member.get("Member Name"),
            "CGST Tax Ledger Code": None, "CGST Amount": None,
            "SGST Tax Ledger Code": None, "SGST Amount": None,
            "IGST Tax Ledger Code": None, "IGST Amount": None,
            "TDS Code": None, "TDS Amount": None
        }
        charges = member.get("Charges", {})
        for i, header in enumerate(discovered_headers):
            column_number = i + 1
            if column_number > 10: break
            amount = charges.get(header)
            flat_row[f"Expense Code {column_number}"] = header
            flat_row[f"Expense Amount {column_number}"] = amount
        start_index = len(discovered_headers)
        if start_index < 10:
            for i in range(start_index, 10):
                column_number = i + 1
                flat_row[f"Expense Code {column_number}"] = None
                flat_row[f"Expense Amount {column_number}"] = None
        all_rows.append(flat_row)

    if not all_rows: raise HTTPException(status_code=400, detail="No data could be processed.")
    df = pd.DataFrame(all_rows)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=converted_template.csv"})


# --- ENDPOINT 2: EXPORT DOCUMENT DIRECTLY TO EXCEL ---
@app.post("/export-to-excel/")
async def export_to_excel(file: UploadFile = File(...)):
    image_to_process = await get_image_from_upload(file)

    try:
        direct_export_prompt = create_direct_export_prompt()
        response = model.generate_content([direct_export_prompt, image_to_process])
        json_text = response.text.strip().replace("```json", "").replace("```", "")
        extracted_data = json.loads(json_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during data extraction: {e}")

    if not extracted_data:
        raise HTTPException(status_code=400, detail="No data could be extracted for Excel export.")
    
    df = pd.DataFrame(extracted_data)

    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, sheet_name="Extracted Data")
    excel_buffer.seek(0)
    
    return StreamingResponse(excel_buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=exported_data.xlsx"})