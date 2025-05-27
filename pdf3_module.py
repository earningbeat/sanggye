import os, cv2, numpy as np, time, json, tempfile, requests
from PIL import Image
from pdf2image import convert_from_path
import fitz  # PyMuPDF
import io
import threading
import queue

# ────────── 환경 설정 ──────────
POPPLER_PATH = r"C:\poppler-24.08.0\Library\bin"
CLOVA_SECRET = "Und3WXZoZHFCRWVac3dDakxOVXRWUWRyeHFIWkhnZGw="
CLOVA_URL = (
    "https://2rh8z013r1.apigw.ntruss.com/custom/v1/"
    "38994/804883a0d318aac5f8e653ef2d42f823e6cbef3c13fb4a1af83dbd4d1cfeb044/general"
)
# ──────────────────────────────

def enhance_image(pil_img):
    """이미지를 numpy 배열로 변환하고 품질을 향상시킵니다."""
    # 이미지를 numpy 배열로 변환
    img_array = np.array(pil_img)
    
    # 그레이스케일 변환
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    
    # 노이즈 제거
    denoised = cv2.fastNlMeansDenoising(gray)
    
    # 대비 향상
    enhanced = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8)).apply(denoised)
    
    # 이진화
    _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    return Image.fromarray(binary)

def clova_ocr(img_path):
    """네이버 클로바 OCR을 사용하여 이미지에서 텍스트를 추출합니다."""
    headers = {"X-OCR-SECRET": CLOVA_SECRET}
    payload = {
        "images": [{"format": "png", "name": "page"}],
        "requestId": "uuid",
        "version": "V2",
        "timestamp": int(time.time() * 1000)
    }

    # 파일 바이트를 미리 읽어 메모리에 보관
    with open(img_path, "rb") as f:
        img_bytes = f.read()

    files = {
        "message": (None, json.dumps(payload), "application/json"),
        "file": ("page.png", img_bytes, "application/octet-stream"),
    }

    res = requests.post(CLOVA_URL, headers=headers, files=files, timeout=60)
    res.raise_for_status()
    return res.json()["images"][0].get("fields", [])

def extract_departments_with_pages(ocr_text):
    """OCR 텍스트에서 부서명과 해당 페이지 번호를 추출합니다."""
    departments = []
    
    for page_idx, page_text in enumerate(ocr_text):
        lines = page_text.split('\n')
        for i, line in enumerate(lines):
            if line.strip() == "부서명" and i + 1 < len(lines):
                dept = lines[i + 1].strip()
                if dept:  # 빈 문자열이 아닌 경우만 추가
                    departments.append((dept, page_idx + 1))  # 페이지 번호는 1부터 시작
    
    return departments

def process_pdf(pdf_input, progress_callback=None):
    """PDF를 처리하여 OCR 결과와 부서명 정보를 반환합니다.
    
    Args:
        pdf_input: PDF 파일 경로 또는 BytesIO 객체
        progress_callback: 진행률 콜백 함수, (current, total) 인자를 받음
        
    Returns:
        처리 결과 딕셔너리
    """
    try:
        # BytesIO 객체인 경우 임시 파일로 저장
        if isinstance(pdf_input, io.BytesIO):
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(pdf_input.getvalue())
                pdf_path = temp_file.name
        else:
            pdf_path = pdf_input

        # PDF를 이미지로 변환
        pages = convert_from_path(pdf_path, dpi=300, poppler_path=POPPLER_PATH)
        total_pages = len(pages)
        
        # 임시 파일 삭제 (BytesIO 경우)
        if isinstance(pdf_input, io.BytesIO):
            os.unlink(pdf_path)
        
        # 진행률 콜백 호출
        if progress_callback:
            progress_callback(0, total_pages)
        
        # 빈 PDF 문서 생성
        out_pdf = fitz.Document()
        out_pdf.set_metadata({
            "title": "Searchable PDF",
            "author": "OCR Converter",
            "subject": "OCR Processed Document",
            "keywords": "OCR, Searchable PDF",
            "creator": "PDF OCR Converter",
            "producer": "PyMuPDF",
            "format": "PDF/A-1b"
        })

        ocr_text = []
        for idx, p in enumerate(pages, 1):
            # 진행률 업데이트
            if progress_callback:
                progress_callback(idx, total_pages)
            
            # 이미지 처리
            enhanced = enhance_image(p)
            
            # 임시 파일 사용 후 즉시 삭제
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
                tmp_img = tmp_file.name
            
            try:
                enhanced.save(tmp_img, "PNG")
                fields = clova_ocr(tmp_img)
            finally:
                # 임시 파일 삭제
                if os.path.exists(tmp_img):
                    os.unlink(tmp_img)

            # 새 페이지 생성
            page = out_pdf.new_page(width=enhanced.width, height=enhanced.height)
            
            # 이미지 삽입
            img_buffer = io.BytesIO()
            enhanced.convert("RGB").save(img_buffer, format="PNG")
            img_bytes = img_buffer.getvalue()
            page.insert_image(fitz.Rect(0, 0, enhanced.width, enhanced.height), 
                             stream=img_bytes)

            # OCR 텍스트 저장
            ocr_text.append("\n".join(f['inferText'] for f in fields))
            
            # 메모리 해제
            del enhanced
            del img_buffer
            del img_bytes
        
        # 부서명과 페이지 번호 추출
        departments_with_pages = extract_departments_with_pages(ocr_text)
        
        return {
            "status": "success",
            "pdf": out_pdf,
            "ocr_text": ocr_text,
            "departments_with_pages": departments_with_pages
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

def save_processed_pdf(pdf_doc, save_path):
    """처리된 PDF를 저장합니다."""
    pdf_doc.save(
        save_path,
        garbage=4,  # 최대 압축
        deflate=True,  # 압축 사용
        clean=True,  # 불필요한 객체 제거
        ascii=False,  # 바이너리 형식 사용
        linear=True,  # 선형화 PDF
        pretty=False,  # 가독성보다 성능 우선
    )
    return save_path

def save_ocr_text(ocr_text, save_path):
    """OCR 텍스트를 저장합니다."""
    with open(save_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(f"--- Page {i+1} ---\n{txt}" for i, txt in enumerate(ocr_text)))
    return save_path

def save_department_list(departments_with_pages, save_path):
    """부서명 목록을 저장합니다."""
    with open(save_path, "w", encoding="utf-8") as f:
        for dept, page in departments_with_pages:
            f.write(f"{dept} (페이지 {page})\n")
    return save_path

def extract_department_pdf(src_pdf, dept_name, page_num, save_path):
    """특정 부서의 PDF 페이지를 추출하여 저장합니다."""
    try:
        # 원본 PDF 열기
        src_pdf_doc = fitz.Document(src_pdf)
        
        # 새 PDF 생성
        dept_pdf = fitz.Document()
        
        # 선택된 페이지 복사 (0-based 인덱스로 변환)
        dept_pdf.insert_pdf(src_pdf_doc, from_page=page_num-1, to_page=page_num-1)
        
        # 저장
        dept_pdf.save(save_path)
        
        return {"status": "success", "path": save_path}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}