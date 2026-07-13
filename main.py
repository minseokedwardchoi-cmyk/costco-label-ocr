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
4. OCR 결과 텍스트에서 "한글표시사항" 항목들을 정규식으로 파싱한다.
5. 신뢰도가 낮은 단어가 일정 비율 이상이면 "검토필요" 플래그를 남긴다
   (비닐 포장재 반사/글레어 등으로 인식이 애매한 사진을 자동으로 걸러내기 위함).
6. 결과를 Google Sheets에 새 행으로 추가한다.
7. 동시 처리(멀티스레드)로 여러 장을 병렬로 돌리되, Azure 무료 티어의
   분당 20건 제한을 넘지 않도록 속도를 자동 조절한다.
8. 일시적 오류는 자동 재시도한다 (사람 개입 없이 완주하는 것이 목표).

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

FIELD_LABELS = [
    "제품명",
    "식품유형",
    "내용량",
    "수입원 및 소재지",
    "원산지 및 제조회사",
    "소비기한",
    "원재료명",
    "보관방법",
    "반품 및 교환장소",
    "포장재질",
]

# 실제 라벨 사진에는 FIELD_LABELS와 문구가 완전히 똑같지 않은 경우가 많다
# (예: "수입원 및 소재지" 대신 "수입업소", "원산지 및 제조회사"가 "원산지"/"제조회사"로
# 따로 찍히는 경우 등). 표준 필드 하나에 여러 이형(異形) 라벨을 매핑해서 인식률을 높인다.
LABEL_ALIASES = {
    "제품명": ["제품명"],
    "식품유형": ["식품유형", "식품의 유형"],
    "내용량": ["내용량"],
    "수입원 및 소재지": ["수입원 및 소재지", "수입업소", "수입원", "수입판매원", "수입자"],
    "원산지 및 제조회사": ["원산지 및 제조회사", "원산지", "제조회사", "제조원", "제조사"],
    "소비기한": ["소비기한", "유통기한"],
    "원재료명": ["원재료명", "원재료"],
    "보관방법": ["보관방법"],
    "반품 및 교환장소": ["반품 및 교환장소", "반품/교환장소", "교환장소"],
    "포장재질": ["포장재질", "재질"],
}

# 사진 두 종류를 같은 Drive 폴더/시트에서 같이 처리한다:
#   1) 제품 뒷면 표시사항 라벨 -> FIELD_LABELS
#   2) 코스트코 매대 가격표 (상품코드/가격 등) -> PRICE_TAG_FIELDS
# 텍스트에 "단가"라는 단어가 있으면 가격표로 판단한다 (가격표에만 나오는 고정 문구).
PRICE_TAG_FIELDS = [
    "상품코드",
    "제품명(한국어)",
    "제품명(영어)",
    "중량",
    "단가",
    "가격",
]

# 파일ID를 맨 앞에 둔다: 구글 시트 자체가 "이미 처리한 파일" 목록의 기준이 되므로
# 파일명이 중복되더라도(예: IMG_0001.jpg가 여러 장) 고유한 Drive 파일ID로 정확히 식별한다.
# PRICE_TAG_FIELDS는 맨 뒤에 붙인다: 기존 컬럼 순서를 그대로 유지해야, 이미 쌓인 데이터 행이
# 밀리지 않고 헤더 행만 안전하게 갱신할 수 있다 (ensure_header 참고).
COLUMN_ORDER = [
    "파일ID",
    "파일명",
    "처리일시",
] + FIELD_LABELS + [
    "알레르기정보",
    "바코드",
    "검토필요",
    "원문텍스트",
] + PRICE_TAG_FIELDS


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
def list_all_images(drive_service):
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and "
        "(mimeType = 'image/jpeg' or mimeType = 'image/png') and trashed = false"
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
    lines_text = []
    confidences = []
    for page in result.get("analyzeResult", {}).get("readResults", []):
        for line in page.get("lines", []):
            lines_text.append(line.get("text", ""))
            for word in line.get("words", []):
                conf = word.get("confidence")
                if conf is not None:
                    confidences.append(conf)
    full_text = "\n".join(lines_text)
    return full_text, confidences


def needs_review(confidences):
    if not confidences:
        return True  # 텍스트를 아예 못 읽었으면 당연히 검토 필요
    low_count = sum(1 for c in confidences if c < CONFIDENCE_THRESHOLD)
    ratio = low_count / len(confidences)
    return ratio >= LOW_CONFIDENCE_WORD_RATIO


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", s)


# ---------------- 항목 파싱: 제품 뒷면 표시사항 라벨 ----------------
def parse_product_label_fields(text: str) -> dict:
    """
    실제 라벨 사진은 "제품명: 값"처럼 콜론이 붙어있는 경우도 있지만,
    "제품명"이 콜론 없이 한 줄에 단독으로 찍히고 값은 다음 줄들에 이어지는
    경우가 더 흔하다. 그래서 줄 단위로 훑으면서, 한 줄이 (콜론 유무와 상관없이)
    라벨 이형 중 하나와 일치하면 그 줄을 헤더로 보고, 다음 헤더가 나오기 전까지의
    줄들을 값으로 묶는다. 같은 표준 필드에 여러 헤더가 매칭되면(예: "원산지"와
    "제조회사"가 둘 다 "원산지 및 제조회사"로 매핑) " / "로 이어붙인다.
    """
    result = {label: "" for label in FIELD_LABELS}
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    headers = []  # [(줄 인덱스, 표준필드명, 같은 줄에 붙어있던 값 또는 None)]
    for i, line in enumerate(lines):
        matched = None
        for field, aliases in LABEL_ALIASES.items():
            for alias in aliases:
                inline = re.match(rf"^{re.escape(alias)}\s*[:：]\s*(.+)$", line)
                if inline:
                    matched = (field, inline.group(1).strip())
                    break
                if _norm(line) == _norm(alias):
                    matched = (field, None)
                    break
            if matched:
                break
        if matched:
            headers.append((i, matched[0], matched[1]))

    for idx, (line_i, field, inline_value) in enumerate(headers):
        if inline_value:
            value = inline_value
        else:
            start = line_i + 1
            end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
            value = " ".join(lines[start:end]).strip(" •,")
        if not value:
            continue
        result[field] = f"{result[field]} / {value}" if result[field] else value

    return result


# ---------------- 항목 파싱: 코스트코 매대 가격표 ----------------
def parse_price_tag_fields(text: str) -> dict:
    """
    가격표 레이아웃(위에서 아래 순서): 상품코드(숫자만 있는 줄) -> 한글 제품명
    (여러 줄) -> 중량("50G X 12" 같은 단위 포함 줄) -> 영문 제품명(대문자 줄)
    -> "단가 / 10G" 같은 헤더 줄 + 소단위 가격 -> 실제 판매가(가장 큰 금액).
    """
    result = {f: "" for f in PRICE_TAG_FIELDS}
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    code_idx = None
    for i, line in enumerate(lines):
        if re.fullmatch(r"\d{4,8}", line):
            result["상품코드"] = line
            code_idx = i
            break

    weight_pattern = re.compile(r"\d+(\.\d+)?\s*(g|ml|kg|l)\b", re.IGNORECASE)
    weight_idx = None
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        if weight_pattern.search(line):
            result["중량"] = line
            weight_idx = i
            break

    if code_idx is not None:
        end = weight_idx if weight_idx is not None else len(lines)
        korean_lines = [l for l in lines[code_idx + 1:end] if re.search(r"[가-힣]", l)]
        result["제품명(한국어)"] = " ".join(korean_lines).strip()

    if weight_idx is not None:
        for line in lines[weight_idx + 1:]:
            if re.fullmatch(r"[A-Z0-9 .,'&\-]{4,}", line) and re.search(r"[A-Z]", line):
                result["제품명(영어)"] = line
                break

    # "단가 / 10G"처럼 기준 단위가 함께 찍혀있으므로, 가격만 뽑으면 몇 g당 가격인지
    # 알 수 없다. "217원" 대신 "217원/10g" 형태로 단위까지 같이 기록한다
    # (기준 단위는 상품마다 다르므로 사진에서 그대로 읽어와야 정확하다).
    danga_idx = next((i for i, line in enumerate(lines) if "단가" in line), None)
    danga_price = ""
    if danga_idx is not None:
        unit_match = re.search(r"(\d+)\s*(g|ml|kg|l)\b", lines[danga_idx], re.IGNORECASE)
        unit = f"{unit_match.group(1)}{unit_match.group(2).lower()}" if unit_match else ""
        for line in lines[danga_idx:danga_idx + 3]:
            m = re.search(r"[\d,]{2,}\s*원", line)
            if m:
                danga_price = m.group(0)
                break
        if danga_price:
            result["단가"] = f"{danga_price}/{unit}" if unit else danga_price

    # 가격은 "12,990원"처럼 천단위 콤마가 있는 큰 금액이다. Azure OCR이 "원" 글자를
    # 가끔 다른 문자(예: "z")로 잘못 읽는 경우가 있어("7,990z"), "원" 글자 자체보다
    # 콤마 포함 숫자 형태를 우선 신뢰한다. 단가는 보통 콤마 없는 2~3자리 숫자라서
    # ("217원", "89원") 이 방식으로도 서로 헷갈리지 않는다.
    price_line_pattern = re.compile(r"^(\d{1,3}(?:,\d{3})+)\s*\S{0,2}$")
    for line in lines:
        m = price_line_pattern.match(line)
        if m:
            result["가격"] = f"{m.group(1)}원"
            break

    if not result["가격"]:
        # 콤마 없는 가격도 드물게 있을 수 있으니, "원" 글자가 정상적으로 인식된
        # 나머지 금액 중에서 찾는 것으로 대비한다.
        all_prices = [m.group(0) for m in re.finditer(r"[\d,]{2,}\s*원", text)]
        remaining_prices = [p for p in all_prices if p != danga_price]
        if remaining_prices:
            result["가격"] = max(remaining_prices, key=lambda p: int(re.sub(r"[^\d]", "", p) or "0"))

    return result


def extract_fields(text: str) -> dict:
    """가격표 사진에만 나오는 "단가"라는 고정 문구로 사진 종류를 판별해서 알맞은
    파서로 넘긴다. 두 종류 모두 같은 시트에 쌓이며, 해당 없는 필드는 빈 칸으로 남는다."""
    if "단가" in text:
        fields = parse_price_tag_fields(text)
    else:
        fields = parse_product_label_fields(text)

    allergy_match = re.search(r"([가-힣,\s]{2,20}함유)", text)
    fields["알레르기정보"] = allergy_match.group(1).strip() if allergy_match else ""

    barcode_match = re.search(r"\b(\d[\d\s]{9,15}\d)\b", text)
    fields["바코드"] = re.sub(r"\s+", "", barcode_match.group(1)) if barcode_match else ""

    return fields


# ---------------- 시트 저장 ----------------
sheet_lock = threading.Lock()  # gspread 동시 append 충돌 방지


def build_row_dict(file_id, filename, fields, raw_text, review_flag):
    row_dict = {
        "파일ID": file_id,
        "파일명": filename,
        "처리일시": time.strftime("%Y-%m-%d %H:%M:%S"),
        "검토필요": "⚠️ 검토필요" if review_flag else "",
        "원문텍스트": raw_text,
    }
    row_dict.update(fields)  # 제품 라벨 필드 또는 가격표 필드 + 알레르기정보/바코드
    return row_dict


def append_to_sheet(sheet, row_dict):
    row = [row_dict.get(col, "") for col in COLUMN_ORDER]
    with sheet_lock:
        sheet.append_row(row, value_input_option="USER_ENTERED")


def ensure_header(sheet):
    existing = sheet.row_values(1)
    if not existing:
        sheet.append_row(COLUMN_ORDER, value_input_option="USER_ENTERED")
    elif existing == COLUMN_ORDER:
        return
    elif COLUMN_ORDER[: len(existing)] == existing:
        # 기존 헤더가 새 COLUMN_ORDER의 앞부분과 정확히 일치한다 = 컬럼이 맨 뒤에
        # 추가되기만 한 안전한 확장이다. 기존 데이터 행은 전혀 밀리지 않으므로
        # 헤더 행만 새 컬럼명을 포함하도록 갱신한다.
        added = COLUMN_ORDER[len(existing):]
        sheet.update(values=[COLUMN_ORDER], range_name="A1")
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
    text, confidences = ocr_image_azure(image_bytes)
    fields = extract_fields(text)
    flag = needs_review(confidences)
    row_dict = build_row_dict(file_id, name, fields, text, flag)
    append_to_sheet(sheet, row_dict)
    product_name = fields.get("제품명") or fields.get("제품명(한국어)") or ""
    return name, product_name, flag


# ---------------- 메인 실행 ----------------
def run_once():
    require_config()

    creds = get_credentials()
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
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
