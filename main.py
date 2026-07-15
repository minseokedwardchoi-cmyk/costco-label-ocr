"""
코스트코 제품 라벨 자동 OCR -> 구글 시트 저장 시스템 (Azure Read API 버전)
========================================================================

Microsoft Azure AI Vision의 Read API를 사용합니다.
Azure Read API는 무료 티어(F0)에서 월 5,000건까지 완전 무료로 제공되므로
월 1,500장 규모에는 비용이 전혀 들지 않습니다.
(무료 티어는 분당 20건 속도 제한이 있어 1,500장 처리에 약 75~90분이 걸립니다.
 사람 개입 없이 알아서 끝나는 배치 작업이므로 문제 없습니다.)

동작 방식
---------
1. Google Drive 폴더에서 이미지 전체를 조회한다 (페이지네이션 처리).
2. 구글 시트에 이미 기록된 파일ID 목록을 읽어, 아직 처리하지 않은 이미지만 추린다
   (GitHub Actions처럼 실행 환경이 매번 새로 시작되는 곳에서도 구글 시트 자체가
   "이미 처리한 파일" 목록의 기준이 되므로 로컬 상태 파일이 필요 없다).
3. 각 이미지를 Azure Read API로 OCR 처리한다 (단어별 신뢰도 점수 포함).
4. OCR 결과 텍스트에서 항목을 정규식으로 뽑는다 (식품/비식품 구분 없이 항상
   같은 방식). 상품코드/제품명(한국어)/가격은 모든 SKU에 공통인 핵심 컬럼이고,
   제품명(영어)/중량/단가는 사진에 그 정보가 있을 때만 채워지는 비핵심 컬럼이다.
5. 결과를 Google Sheets에 새 행으로 추가한다.
6. 동시 처리(멀티스레드)로 여러 장을 병렬로 돌리되, Azure 무료 티어의
   분당 20건 제한을 넘지 않도록 속도를 자동 조절한다.
7. 일시적 오류는 자동 재시도한다 (사람 개입 없이 완주하는 것이 목표).

설정값은 전부 환경변수로 받는다 (README.md 참고). 로컬에서 실행할 때는
`.env` 파일에 값을 채우고 `python-dotenv`가 자동으로 로드한다.
GitHub Actions에서는 리포지토리 Secrets 값이 환경변수로 주입된다.
"""

import io
import os
import re
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
from PIL import Image
import pillow_heif

pillow_heif.register_heif_opener()  # PIL이 HEIC/HEIF 파일도 열 수 있도록 등록 (아이폰 기본 사진 포맷)

load_dotenv()

# ============ CONFIG (환경변수로 설정, README.md 참고) ============
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "service_account.json")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = os.environ.get("SHEET_NAME", "시트1")

AZURE_VISION_ENDPOINT = os.environ.get("AZURE_VISION_ENDPOINT")
AZURE_VISION_KEY = os.environ.get("AZURE_VISION_KEY")

# Azure F0(무료 티어) 제한: 분당 20건. 여유를 두고 18건/분으로 제한.
AZURE_MAX_CALLS_PER_MINUTE = int(os.environ.get("AZURE_MAX_CALLS_PER_MINUTE", "18"))
CONCURRENT_WORKERS = int(os.environ.get("CONCURRENT_WORKERS", "4"))
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.65"))
LOW_CONFIDENCE_WORD_RATIO = float(os.environ.get("LOW_CONFIDENCE_WORD_RATIO", "0.15"))
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# 핵심 컬럼: 식품/비식품 가릴 것 없이 모든 SKU에 공통으로 채워지는 것들.
CORE_PRICE_FIELDS = ["상품코드", "제품명(한국어)", "가격"]
# 비핵심 컬럼: 사진에 찍혀있으면 채우고, 없으면 빈 칸으로 남긴다.
OPTIONAL_PRICE_FIELDS = ["제품명(영어)", "중량", "단가"]
PRICE_FIELDS = CORE_PRICE_FIELDS + OPTIONAL_PRICE_FIELDS

COLUMN_ORDER = ["파일ID", "파일명", "처리일시", "원문텍스트"] + PRICE_FIELDS


def require_config():
    missing = [
        name
        for name in ["DRIVE_FOLDER_ID", "SPREADSHEET_ID", "AZURE_VISION_ENDPOINT", "AZURE_VISION_KEY"]
        if not globals()[name]
    ]
    if not GOOGLE_SERVICE_ACCOUNT_JSON and not os.path.exists(SERVICE_ACCOUNT_FILE):
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON (또는 service_account.json 파일)")
    if missing:
        print("다음 환경변수/파일이 설정되지 않았습니다. README.md를 참고해 설정해주세요:")
        for name in missing:
            print(f"  - {name}")
        sys.exit(1)


# ---------------- 속도 제한기 (분당 N건) ----------------
class RateLimiter:
    def __init__(self, max_calls_per_minute):
        self.max_calls = max_calls_per_minute
        self.calls = deque()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            while self.calls and now - self.calls[0] > 60:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = 60 - (now - self.calls[0]) + 0.1
            else:
                sleep_time = 0
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.calls.append(time.time())


rate_limiter = RateLimiter(AZURE_MAX_CALLS_PER_MINUTE)


# ---------------- 인증 / 클라이언트 ----------------
def get_credentials():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        import json
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )


def load_processed_ids(sheet):
    """구글 시트의 '파일ID' 열(1번 컬럼)에 이미 기록된 값들을 처리 완료 목록으로 삼는다."""
    ids = sheet.col_values(1)[1:]  # 헤더 제외
    return set(ids)


# ---------------- Drive: 전체 이미지 조회 (페이지네이션 포함) ----------------
HEIC_MIME_TYPES = ("image/heic", "image/heif")

def list_all_images(drive_service):
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and "
        "(mimeType = 'image/jpeg' or mimeType = 'image/png' "
        "or mimeType = 'image/heic' or mimeType = 'image/heif') and trashed = false"
    )
    files = []
    page_token = None
    while True:
        results = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return files


def download_image(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def convert_heic_to_jpeg(image_bytes: bytes) -> bytes:
    """Azure Read API는 HEIC/HEIF를 지원하지 않으므로(아이폰 기본 사진 포맷),
    OCR에 보내기 전에 JPEG로 변환한다."""
    image = Image.open(io.BytesIO(image_bytes))
    out = io.BytesIO()
    image.convert("RGB").save(out, format="JPEG", quality=92)
    return out.getvalue()


def is_heic(file_info: dict) -> bool:
    if file_info.get("mimeType") in HEIC_MIME_TYPES:
        return True
    return file_info.get("name", "").lower().endswith((".heic", ".heif"))


# ---------------- Azure Read API OCR (재시도 포함) ----------------
def ocr_image_azure(image_bytes, max_retries=4):
    """
    Azure Read API 호출. 비동기 방식(제출 -> 폴링)이라 두 단계로 이뤄진다.
    반환값: (전체 텍스트, 단어별 신뢰도 리스트)
    """
    submit_url = f"{AZURE_VISION_ENDPOINT.rstrip('/')}/vision/v3.2/read/analyze"
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_VISION_KEY,
        "Content-Type": "application/octet-stream",
    }

    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            resp = requests.post(submit_url, headers=headers, data=image_bytes, timeout=30)

            if resp.status_code == 429:  # 속도 제한 초과 -> 대기 후 재시도
                wait_s = int(resp.headers.get("Retry-After", "10"))
                time.sleep(wait_s)
                continue

            resp.raise_for_status()
            operation_url = resp.headers["Operation-Location"]

            # 폴링: 결과가 나올 때까지 대기
            for _ in range(30):
                time.sleep(1.5)
                poll = requests.get(
                    operation_url,
                    headers={"Ocp-Apim-Subscription-Key": AZURE_VISION_KEY},
                    timeout=30,
                )
                poll.raise_for_status()
                result = poll.json()
                if result.get("status") == "succeeded":
                    return _extract_text_and_confidence(result)
                if result.get("status") == "failed":
                    raise RuntimeError("Azure OCR 작업 실패")
            raise RuntimeError("Azure OCR 결과 대기 시간 초과")

        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # 지수 백오프

    raise RuntimeError("Azure OCR 재시도 횟수 초과")


def _extract_text_and_confidence(result):
    """
    Azure가 반환하는 줄 순서는 항상 사진의 실제 위→아래 순서와 일치하지는
    않는다 (특히 바코드/상품코드 구역, 제품명 구역, 가격 구역처럼 서로 떨어진
    영역이 있는 사진에서 순서가 뒤섞이는 경우가 있다). 우리 파싱 로직은 "N번째
    줄" 식으로 순서에 의존하므로, 각 줄의 bounding box 중 가장 작은 y좌표(윗쪽
    끝)를 기준으로 재정렬해서 실제 시각적 순서에 가깝게 맞춘다.
    """
    entries = []  # (top_y 또는 None, text)
    confidences = []
    for page in result.get("analyzeResult", {}).get("readResults", []):
        for line in page.get("lines", []):
            text_val = line.get("text", "")
            bbox = line.get("boundingBox") or []
            top_y = min(bbox[1::2]) if len(bbox) >= 8 else None
            entries.append((top_y, text_val))
            for word in line.get("words", []):
                conf = word.get("confidence")
                if conf is not None:
                    confidences.append(conf)

    positioned = sorted((e for e in entries if e[0] is not None), key=lambda e: e[0])
    unpositioned = [e for e in entries if e[0] is None]
    full_text = "\n".join(text_val for _, text_val in positioned + unpositioned)
    return full_text, confidences


def needs_review(confidences):
    if not confidences:
        return True  # 텍스트를 아예 못 읽었으면 당연히 검토 필요
    low_count = sum(1 for c in confidences if c < CONFIDENCE_THRESHOLD)
    ratio = low_count / len(confidences)
    return ratio >= LOW_CONFIDENCE_WORD_RATIO


def split_price_tag_cards(text: str):
    """한 사진에 상품 가격표 카드가 여러 개 찍혀있을 수 있다. 상품코드로 보이는
    줄과 가격으로 보이는 줄이 각각 2개 이상이면 카드가 여러 개 있다고 보고,
    각 상품코드 줄을 기준으로 텍스트를 나눈다 (다음 상품코드 줄 직전까지가 한
    카드). 카드가 1개뿐이면(대부분의 경우) 원래 텍스트를 그대로 돌려준다.
    반환값: (조각 텍스트 리스트, 여러 장 감지 여부)
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    code_indices = [i for i, l in enumerate(lines) if re.fullmatch(r"\d{4,8}", l)]
    price_count = sum(1 for l in lines if re.match(r"^\d{1,3}(?:,\d{3})+\s*\S{0,2}$", l))

    if len(code_indices) < 2 or price_count < 2:
        return [text], False

    segments = []
    for idx, code_i in enumerate(code_indices):
        end = code_indices[idx + 1] if idx + 1 < len(code_indices) else len(lines)
        segments.append("\n".join(lines[code_i:end]))
    return segments, True


# 끝에 \b(단어 경계)를 안 붙인다: "1.2kgx1개"처럼 단위 글자 바로 뒤에 곱셈
# 표시(x/X/×)나 한글이 공백 없이 바로 붙는 경우가 흔한데, 영문자 뒤에 다른
# 영문자/한글이 바로 오면 \b가 경계로 인정하지 않아 매칭이 실패하기 때문이다
# (Python 정규식의 \w는 한글도 "단어 문자"로 취급해서 "kg후"도 \b가 안 걸림).
WEIGHT_PATTERN = re.compile(r"\d+(\.\d+)?\s*(g|ml|kg|l|m)", re.IGNORECASE)
PRICE_LINE_PATTERN = re.compile(r"^(\d{1,3}(?:,\d{3})+)\s*\S{0,2}$")
DISCOUNT_LINE_PATTERN = re.compile(r"^-[\d,]+\s*원?$")


# ---------------- 항목 파싱: 상품코드 / 한국어 제품명 / 가격 ----------------
def parse_price_fields(text: str) -> dict:
    """
    가격표 레이아웃(위에서 아래 순서): 상품코드(숫자만 있는 줄) -> 한글 제품명
    (2~3줄) -> [식품이면 중량 1줄] -> 영문 제품명(대문자 1줄) -> [단가] -> 판매가.
    시트에 남기는 건 상품코드/제품명(한국어)/가격 3개뿐이지만, 중량·영문
    제품명·단가 줄은 한국어 제품명이 어디서 끝나는지, 가격 후보 중 뭘 걸러야
    하는지 판단하는 경계로는 여전히 필요해서 내부적으로만 찾는다.
    (text는 이미 ocr_image_azure 단계에서 bounding box 기준으로 재정렬되어 있어서
    실제 시각적 순서와 거의 일치한다 - 아래 로직은 이 순서를 그대로 신뢰한다.)
    """
    result = {f: "" for f in PRICE_FIELDS}
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    code_idx = None
    for i, line in enumerate(lines):
        if re.fullmatch(r"\d{4,8}", line):
            result["상품코드"] = line
            code_idx = i
            break

    weight_idx = None
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        if WEIGHT_PATTERN.search(line):
            result["중량"] = line
            weight_idx = i
            break

    danga_idx = next((i for i, line in enumerate(lines) if "단가" in line), None)

    english_idx = None
    seen_korean_line = False
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        if re.search(r"[가-힣]", line):
            seen_korean_line = True
        # "NATIONAL GEOGRAPHIC", "RICOLA", "MIKAKUTO"처럼 브랜드명이 코드 바로
        # 다음 줄에 대문자로 찍히는 경우가 많다. 단어 수 조건("2단어 이상")만으로는
        # "NATIONAL GEOGRAPHIC"처럼 두 단어짜리 브랜드명을 못 걸러서 진짜 영어
        # 제품명(예: "NG WATERPROOF BAG 2PK")보다 먼저 잘못 채택돼버린다.
        # 실제 레이아웃은 항상 "코드 -> (브랜드명) -> 한글 제품명 -> 영어 제품명"
        # 순서이므로, 한글이 포함된 줄을 최소 한 번은 지나친 뒤부터만 영어 제품명
        # 후보로 인정한다 - 이러면 단어 수와 상관없이 브랜드명 줄이 걸러진다.
        if not seen_korean_line:
            continue
        # "TROLLI ALL IN ONE 1.2KG"처럼 실제 영어 제품명 끝에 중량이 같이 붙어
        # 나오는 경우가 있어서, 중량 패턴이 포함된 줄을 통째로 걸러내면 안 된다.
        # 대신 아래 "대문자 연속 2글자 + 2단어 이상" 조건만으로 순수 중량 단독
        # 줄("50G X 12", "1.2KG" 등)은 이미 충분히 걸러진다 - 그런 줄은 단어가
        # 1개뿐이거나 대문자가 서로 떨어져 있어서 조건을 통과하지 못한다.
        if (
            re.fullmatch(r"[A-Z0-9 .,'&\-]{4,}", line)
            and re.search(r"[A-Z]{2,}", line)
            and len(line.split()) >= 2
        ):
            english_idx = i
            result["제품명(영어)"] = line
            break

    if code_idx is not None:
        # 한글 제품명은 상품코드 다음 줄부터, 중량/영문명/단가 중 가장 먼저 나오는
        # 줄 전까지로 본다 (셋 다 없으면 끝까지). 식품은 보통 중량이 경계가 되고,
        # 비식품은 중량이 없으니 영문명이 바로 경계가 된다.
        # 이 구간에는 "RICOLA"처럼 한글이 아닌 브랜드명 줄이 섞여 있을 수 있는데,
        # 실제로는 한글 제품명의 일부이므로("RICOLA 레몬민트 허브캔디") 한글 포함
        # 여부로 거르지 않고 구간 안의 모든 줄을 그대로 합친다.
        boundary_candidates = [i for i in (weight_idx, english_idx, danga_idx) if i is not None]
        end = min(boundary_candidates) if boundary_candidates else len(lines)
        korean_lines = lines[code_idx + 1:end]
        result["제품명(한국어)"] = " ".join(korean_lines).strip()

    # "단가 / 10G"처럼 기준 단위가 함께 찍혀있으므로, 가격만 뽑으면 몇 g당 가격인지
    # 알 수 없다. "217원" 대신 "217원/10g" 형태로 단위까지 같이 기록한다
    # (기준 단위는 상품마다 다르므로 사진에서 그대로 읽어와야 정확하다).
    danga_price = ""
    danga_price_line_idx = None  # 가격 탐색에서 이 줄은 다시 쓰지 않도록 인덱스로 기억
    if danga_idx is not None:
        unit_match = re.search(r"(\d+)\s*(g|ml|kg|l|m)", lines[danga_idx], re.IGNORECASE)
        unit = f"{unit_match.group(1)}{unit_match.group(2).lower()}" if unit_match else ""
        for offset, line in enumerate(lines[danga_idx:danga_idx + 3]):
            m = re.search(r"[\d,]{2,}\s*원", line)
            if m:
                danga_price = m.group(0)
                danga_price_line_idx = danga_idx + offset
                break
        if danga_price:
            result["단가"] = f"{danga_price}/{unit}" if unit else danga_price

    # 가격은 "12,990원"처럼 천단위 콤마가 있는 큰 금액이다. Azure OCR이 "원" 글자를
    # 가끔 다른 문자(예: "z")로 잘못 읽는 경우가 있어("7,990z"), "원" 글자 자체보다
    # 콤마 포함 숫자 형태를 우선 신뢰한다. 단가는 보통 콤마 없는 2~3자리 숫자라서
    # ("217원", "89원") 이 방식으로도 서로 헷갈리지 않는다.
    # - 단가로 이미 쓴 줄(danga_price_line_idx)은 절대 가격으로 다시 쓰지 않는다
    #   (같은 값이 단가/가격 두 칸에 중복으로 들어가는 것 방지).
    # - "할인행사" 문구나 "-4,500원"처럼 할인폭을 나타내는 줄이 나오면, 그 뒤에
    #   나오는 가격(할인가)은 절대 선택하지 않는다 - 정가만 "가격"으로 취급한다.
    def find_price(candidate_lines):
        for i, line in enumerate(candidate_lines):
            if i == danga_price_line_idx:
                continue
            m = PRICE_LINE_PATTERN.match(line)
            if m:
                return f"{m.group(1)}원"
        return ""

    discount_idx = next(
        (i for i, l in enumerate(lines) if "할인" in l or DISCOUNT_LINE_PATTERN.match(l)),
        None,
    )
    if discount_idx is not None:
        # 할인 표시 이전 구간에서 먼저 찾고(=정가), 없으면 그래도 뭔가는 남겨야 하니
        # 전체에서 찾는다(할인 표시가 우연히 다른 문구와 겹쳤을 경우의 대비).
        result["가격"] = find_price(lines[:discount_idx]) or find_price(lines)
    else:
        result["가격"] = find_price(lines)

    if not result["가격"]:
        # 콤마 없는 가격도 드물게 있을 수 있으니, "원" 글자가 정상적으로 인식된
        # 나머지 금액 중에서 찾는 것으로 대비한다.
        all_prices = [m.group(0) for m in re.finditer(r"[\d,]{2,}\s*원", text)]
        remaining_prices = [p for p in all_prices if p != danga_price]
        if remaining_prices:
            result["가격"] = max(remaining_prices, key=lambda p: int(re.sub(r"[^\d]", "", p) or "0"))

    return result


# ---------------- 시트 저장 ----------------
sheet_lock = threading.Lock()  # gspread 동시 append 충돌 방지


def build_row_dict(file_id, filename, fields, raw_text):
    row_dict = {
        "파일ID": file_id,
        "파일명": filename,
        "처리일시": time.strftime("%Y-%m-%d %H:%M:%S"),
        "원문텍스트": raw_text,
    }
    row_dict.update(fields)  # 상품코드/제품명(한국어)/가격
    return row_dict


def append_rows_to_sheet(sheet, row_dicts):
    rows = [[row_dict.get(col, "") for col in COLUMN_ORDER] for row_dict in row_dicts]
    with sheet_lock:
        # table_range를 A1로 명시하지 않으면 gspread가 시트 전체를 스캔해서
        # "표"의 위치를 스스로 추측하는데, 헤더와 실제로 append하는 행의 폭이
        # 어긋나 있으면(예: 헤더 마이그레이션이 덜 된 상태) 이 자동 추측이 틀어져서
        # 다음 행이 점점 더 오른쪽 컬럼에서 시작되는 식으로 계속 밀릴 수 있다.
        # A1로 고정해서 항상 A열 기준으로만 이어붙이게 만든다.
        sheet.append_rows(rows, value_input_option="USER_ENTERED", table_range="A1")


def _find_missing_columns(existing, column_order):
    """existing의 모든 컬럼이 column_order 안에 같은 상대 순서로 들어있으면
    (즉 existing이 column_order의 부분수열이면) [(끼워넣을 위치, 컬럼명), ...]을
    돌려준다 (맨 뒤에 추가되는 경우도 포함). 순서가 바뀌었거나 컬럼이 삭제된
    경우처럼 단순 "부분수열 + 추가"로 설명 안 되면 None을 돌려준다."""
    missing = []
    ei = 0
    for ci, col in enumerate(column_order):
        if ei < len(existing) and existing[ei] == col:
            ei += 1
        else:
            missing.append((ci, col))
    if ei != len(existing):
        return None
    return missing


def ensure_header(sheet):
    existing = sheet.row_values(1)
    if not existing:
        sheet.append_row(COLUMN_ORDER, value_input_option="USER_ENTERED")
        return
    if existing == COLUMN_ORDER:
        return

    missing = _find_missing_columns(existing, COLUMN_ORDER)
    if missing is not None:
        # 기존 헤더의 컬럼들이 COLUMN_ORDER 안에 전부 같은 순서로 들어있고,
        # 새 컬럼만 몇 개 늘어난 상황이다 (맨 뒤에 추가됐든, 중간에 끼어들었든).
        # 새 컬럼이 들어갈 자리에 빈 열을 실제로 끼워넣어서 기존 데이터 행이
        # 절대 안 밀리게 만든다 (뒤 인덱스부터 끼워야 앞쪽 인덱스가 안 틀어짐).
        for col_idx, _name in sorted(missing, key=lambda x: -x[0]):
            if col_idx < len(existing):
                sheet.insert_cols([[]], col=col_idx + 1)
        sheet.update(values=[COLUMN_ORDER], range_name="A1")
        added = [name for _, name in missing]
        print(f"시트 헤더에 새 컬럼 {len(added)}개를 추가했습니다: {', '.join(added)}")
    else:
        # 컬럼 순서가 바뀌었거나 삭제된 경우 - 자동으로 지우면 기존 데이터가 밀릴 수
        # 있으므로 건드리지 않는다. 헤더 행을 수동으로 맞춰주세요.
        print("경고: 시트 1행 헤더가 COLUMN_ORDER와 다릅니다. 데이터 보호를 위해 자동으로 지우지 않았으니, "
              "헤더 행을 아래 순서로 직접 맞춰주세요:")
        print("  " + " | ".join(COLUMN_ORDER))


# ---------------- 파일 1건 처리 ----------------
# googleapiclient의 service 객체(내부 httplib2 클라이언트)는 스레드 세이프하지 않다.
# 여러 스레드가 하나의 service 객체를 공유해서 동시에 요청을 보내면 연결이 깨져
# SSL 오류나 심하면 메모리 손상(crash)까지 발생한다. 스레드마다 별도 service 객체를
# 만들어 쓰도록 스레드 로컬로 캐싱한다.
_thread_local = threading.local()


def get_thread_drive_service(creds):
    if not hasattr(_thread_local, "drive_service"):
        _thread_local.drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _thread_local.drive_service


def process_one_file(creds, sheet, file_info):
    name, file_id = file_info["name"], file_info["id"]
    drive_service = get_thread_drive_service(creds)
    image_bytes = download_image(drive_service, file_id)
    if is_heic(file_info):
        image_bytes = convert_heic_to_jpeg(image_bytes)
    text, confidences = ocr_image_azure(image_bytes)
    low_confidence = needs_review(confidences)

    # 상품코드/가격 패턴이 2개 이상씩 없으면 카드가 1개뿐이라고 보고 전체
    # 텍스트를 그대로 돌려주므로, 가격표가 아닌 사진에도 안전하게 쓸 수 있다.
    segments, multi_card = split_price_tag_cards(text)

    row_dicts = []
    product_names = []
    for seg_text in segments:
        fields = parse_price_fields(seg_text)
        row_dicts.append(build_row_dict(file_id, name, fields, seg_text))
        product_names.append(fields.get("제품명(한국어)") or "")

    append_rows_to_sheet(sheet, row_dicts)
    product_summary = " / ".join(p for p in product_names if p)
    return name, product_summary, low_confidence or multi_card


# ---------------- 메인 실행 ----------------
def run_once():
    require_config()

    creds = get_credentials()
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        # SHEET_NAME이 가리키는 탭이 없으면(수동으로 이름을 바꿨거나 지운 경우 등)
        # 매 실행마다 에러로 죽는 대신, 그 이름으로 새 탭을 만들어서 계속 진행한다.
        print(f"경고: 시트 탭 '{SHEET_NAME}'을 찾을 수 없어서 새로 만듭니다. "
              "기존에 다른 이름의 탭에 데이터가 있었다면 그 데이터는 이 탭에 없으니 확인해주세요.")
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=26)
    ensure_header(sheet)

    processed_ids = load_processed_ids(sheet)
    all_files = list_all_images(drive_service)
    new_files = [f for f in all_files if f["id"] not in processed_ids]

    if not new_files:
        print("처리할 새 이미지가 없습니다.")
        return

    print(f"신규 이미지 {len(new_files)}건 발견. 처리를 시작합니다...")
    print(f"(Azure 무료 티어 속도 제한: 분당 {AZURE_MAX_CALLS_PER_MINUTE}건 -> "
          f"예상 소요 시간 약 {len(new_files) / AZURE_MAX_CALLS_PER_MINUTE:.0f}분)")

    success_count = 0
    failed = []

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = {
            executor.submit(process_one_file, creds, sheet, f): f
            for f in new_files
        }
        for future in as_completed(futures):
            f = futures[future]
            try:
                name, product, flag = future.result()
                success_count += 1
                flag_str = " [검토필요]" if flag else ""
                print(f"  완료: {name} -> {product or '(제품명 인식 실패)'}{flag_str}")
            except Exception as e:
                failed.append((f["name"], str(e)))
                print(f"  실패: {f['name']} -> {e}")

    print(f"\n완료: {success_count}건 성공, {len(failed)}건 실패")
    if failed:
        print("실패 목록 (다음 실행 시 자동 재시도됩니다 - 시트에 기록되지 않은 파일ID는 신규로 취급됨):")
        for name, err in failed:
            print(f"  - {name}: {err}")


if __name__ == "__main__":
    run_once()
