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
# 같은 Drive 폴더에 코스트코/트레이더스 가격표 사진이 섞여 올라온다. 형식을
# 자동 판별해서(detect_retailer 참고) 각자의 시트 탭에 나눠 기록한다.
TRADERS_SHEET_NAME = os.environ.get("TRADERS_SHEET_NAME", "트레이더스")
# 코스트코/트레이더스 원본 시트는 그대로 두고, 같은 데이터를 제품군(예: 올리브유)
# 단위로 정리해서 보여주는 별도 시트. 원본 시트가 진짜 원본이고 이 시트는 거기서
# 파생된 정리본이라, 분류가 잘못돼도 원본 대조로 언제든 재정리할 수 있다.
# 코스트코/트레이더스는 상품 성격이 서로 달라서(예: 식품 위주 vs 생활용품 위주)
# 제품군 블록도 원본 시트와 마찬가지로 리테일러별로 따로 둔다.
CATEGORY_SHEET_NAME_COSTCO = os.environ.get("CATEGORY_SHEET_NAME_COSTCO", "제품군정리(코스트코)")
CATEGORY_SHEET_NAME_TRADERS = os.environ.get("CATEGORY_SHEET_NAME_TRADERS", "제품군정리(트레이더스)")

AZURE_VISION_ENDPOINT = os.environ.get("AZURE_VISION_ENDPOINT")
AZURE_VISION_KEY = os.environ.get("AZURE_VISION_KEY")

# Azure F0(무료 티어) 제한: 분당 20건. 여유를 두고 18건/분으로 제한.
AZURE_MAX_CALLS_PER_MINUTE = int(os.environ.get("AZURE_MAX_CALLS_PER_MINUTE", "18"))
CONCURRENT_WORKERS = int(os.environ.get("CONCURRENT_WORKERS", "4"))
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.65"))
LOW_CONFIDENCE_WORD_RATIO = float(os.environ.get("LOW_CONFIDENCE_WORD_RATIO", "0.15"))
# =========================================================

SCOPES = [
    # 처리 완료된 사진을 '처리완료' 폴더로 이동하려면 쓰기 권한이 필요해서
    # drive.readonly 대신 전체 drive 스코프를 쓴다 (서비스 계정이 Drive 폴더에
    # 최소 편집자 권한으로 공유되어 있어야 이동이 성공한다).
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# 처리 완료된 사진을 옮겨두는 하위 폴더 이름. 삭제하지 않고 이동만 하므로
# 나중에 재검증이 필요하면 그대로 다시 볼 수 있고, 신규 업로드 폴더
# (DRIVE_FOLDER_ID)는 다음 실행의 조회 대상에서 계속 가벼운 상태로 유지된다.
ARCHIVE_FOLDER_NAME = "처리완료"

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


def get_or_create_archive_folder(drive_service, parent_id):
    """'처리완료' 하위 폴더를 찾아서 id를 돌려주고, 없으면 새로 만든다.
    여러 스레드가 동시에 만들려고 하면 중복 폴더가 생길 수 있으므로,
    스레드 풀을 시작하기 전에 메인 스레드에서 한 번만 호출한다."""
    query = (
        f"'{parent_id}' in parents and name = '{ARCHIVE_FOLDER_NAME}' "
        "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    results = drive_service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    existing = results.get("files", [])
    if existing:
        return existing[0]["id"]
    folder = drive_service.files().create(
        body={
            "name": ARCHIVE_FOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
    ).execute()
    return folder["id"]


def archive_file(drive_service, file_id, archive_folder_id):
    drive_service.files().update(
        fileId=file_id,
        addParents=archive_folder_id,
        removeParents=DRIVE_FOLDER_ID,
        fields="id, parents",
    ).execute()


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


# 진짜 중량("50G X 12", "1.2kgx1개", "900g포장")과 "SML194G"(모델 코드),
# "2gari"(제품명 일부) 같은 우연의 일치를 구분해야 한다:
#   - 앞쪽: (?<![A-Za-z0-9]) - 숫자 바로 앞에 글자/숫자가 없어야 한다.
#     "SML194G"는 194 바로 앞에 "L"이 붙어있어서(코드 일부) 여기서 걸러진다.
#     반면 "50G", "342G"(공백/구두점/줄 시작 뒤)는 통과한다.
#   - 뒤쪽: (?![a-wyz]) - 단위 글자 바로 뒤에 (x/X/× 곱셈 표시 말고) 다른
#     소문자가 더 이어지면 안 된다. "2gari"는 g 뒤에 "ari"가 이어지므로
#     걸러지고, "1.2kgx1개"는 뒤에 오는 게 x라서 (곱셈 표시로) 허용된다.
# 끝에 단어 경계(\b)를 안 쓰는 이유는, "kg후"처럼 단위 뒤에 한글이 공백 없이
# 바로 붙으면 Python 정규식의 \w가 한글도 "단어 문자"로 취급해서 \b가 아예
# 경계로 인정을 안 하기 때문이다 - 그래서 위처럼 직접 조건을 짰다.
WEIGHT_PATTERN = re.compile(r"(?<![A-Za-z0-9])\d+(\.\d+)?\s*(g|ml|kg|l|m)(?![a-wyz])", re.IGNORECASE)
PRICE_LINE_PATTERN = re.compile(r"^(\d{1,3}(?:,\d{3})+)\s*\S{0,2}$")
DISCOUNT_LINE_PATTERN = re.compile(r"^-[\d,]+\s*원?$")

# 중량이 독립된 줄이 아니라 "오리온 오그래놀라 시나몬츄러스 440g* 3개"처럼
# 제품명 끝에 그대로 붙어 나오는 경우가 많다. 중량 단위 뒤에 "* 3개", "x2개",
# "포"처럼 짧은 수량 표시만 더 있고 그걸로 줄이 끝나면, 그 지점부터를 전부
# 중량 쪽으로 떼어낸다.
_TRAILING_QUANTITY_RE = re.compile(r"^[\sx×X*]*\d*\s*(개|포|입|병|팩|ea|EA|Ea)?\.?$")

# "100g당 899원"처럼 "단가"라는 글자 없이 "N단위당 M원" 형태로만 찍히는 경우도
# 있다 (트레이더스뿐 아니라 코스트코 화면 캡처류에서도 나온다). 두 파서 모두
# 이 패턴을 단가 보조 신호로 쓴다.
UNIT_PRICE_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?\s*(?:g|ml|kg|l|m))\s*당\s*([\d,]+)\s*원", re.IGNORECASE
)


def _split_trailing_weight(name: str) -> tuple:
    """제품명 문자열 끝에 중량이 붙어있으면 (중량 뗀 제품명, 중량)을 돌려주고,
    없으면 (원래 이름, "")을 그대로 돌려준다."""
    matches = list(WEIGHT_PATTERN.finditer(name))
    if not matches:
        return name, ""
    last = matches[-1]
    rest = name[last.end():]
    if _TRAILING_QUANTITY_RE.match(rest):
        return name[:last.start()].strip(), name[last.start():].strip()
    return name, ""


# 코드 바로 다음에 진짜 제품명이 아니라 "14.970", "15,990+"처럼 순수 숫자/기호
# 파편(가격·단가 잔여물, 배경에 겹친 다른 태그의 조각)이 섞여 나오는 경우가
# 있다. 그런 파편은 글자가 하나도 없는 토큰이므로, 제품명 문자열을 공백 기준
# 토큰으로 나눠 "글자가 전혀 없는 토큰" 비율이 얼마나 되는지로 깨끗함 정도를
# 매긴다 (0=전부 파편, 1=파편 없음).
_JUNK_TOKEN_RE = re.compile(r"^[\d.,+%=]+$")


def _name_cleanliness(name: str) -> float:
    tokens = name.split()
    if not tokens:
        return 0.0
    junk = sum(1 for t in tokens if _JUNK_TOKEN_RE.match(t))
    return 1 - (junk / len(tokens))


# ---------------- 항목 파싱: 상품코드 / 한국어 제품명 / 가격 ----------------
def parse_price_fields(text: str) -> dict:
    """
    사진 한 장당 항상 한 상품만 뽑는다. 대부분은 상품코드로 보이는 줄이 하나뿐이라
    문제가 없지만, 배경 진열대의 다른 가격표가 흐릿하게 같이 찍히면 그 상품코드
    (또는 우연히 숫자 4~8자리처럼 보이는 잡음)까지 함께 잡혀서 코드가 여러 개
    나올 수 있다. 이 경우 각 코드부터 다음 코드 직전까지를 한 구간으로 나눠
    따로 파싱해보고, 다음 기준으로 가장 그럴듯한 구간 하나를 채택한다:
      1) 그 구간 안에서 제품명·가격이 실제로 같이 발견됐는지 (둘 다 있는 구간이
         우선) - 배경 잡음 코드 구간은 대개 둘 중 하나 이상이 비어있어서 여기서
         걸러진다.
      2) 위 기준이 동점이면(사진에 진짜 상품이 여러 개 찍힌 경우), 추출된
         제품명이 얼마나 "깨끗한"(숫자/기호 파편이 아니라 실제 단어로 이뤄진)
         텍스트인지를 본다 - 상품코드 바로 뒤에 가격·단가 잔여물 같은 숫자
         파편이 섞여 들어간 구간은 이 기준으로 걸러진다.
      3) 그래도 동점이면 가장 먼저 나온 구간(대개 사진이 겨냥한 목표 가격표)을
         채택한다.
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    code_indices = [i for i, l in enumerate(lines) if re.fullmatch(r"\d{4,8}", l)]

    if len(code_indices) <= 1:
        return _parse_fields_from_lines(lines)

    best_fields, best_score = None, None
    for idx, code_i in enumerate(code_indices):
        end = code_indices[idx + 1] if idx + 1 < len(code_indices) else len(lines)
        segment_fields = _parse_fields_from_lines(lines[code_i:end])
        name = segment_fields.get("제품명(한국어)") or ""
        base = (1 if name else 0) + (1 if segment_fields.get("가격") else 0)
        score = (base, _name_cleanliness(name))
        if best_score is None or score > best_score:
            best_score, best_fields = score, segment_fields
    return best_fields


def _parse_fields_from_lines(lines: list) -> dict:
    """
    가격표 레이아웃(위에서 아래 순서): 상품코드(숫자만 있는 줄) -> 한글 제품명
    (2~3줄) -> [식품이면 중량 1줄] -> 영문 제품명(대문자 1줄) -> [단가] -> 판매가.
    시트에 남기는 건 상품코드/제품명(한국어)/가격 3개뿐이지만, 중량·영문
    제품명·단가 줄은 한국어 제품명이 어디서 끝나는지, 가격 후보 중 뭘 걸러야
    하는지 판단하는 경계로는 여전히 필요해서 내부적으로만 찾는다.
    (lines는 이미 ocr_image_azure 단계에서 bounding box 기준으로 재정렬되어 있어서
    실제 시각적 순서와 거의 일치한다 - 아래 로직은 이 순서를 그대로 신뢰한다.)
    """
    result = {f: "" for f in PRICE_FIELDS}

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
        m = WEIGHT_PATTERN.search(line)
        # 중량 앞에 다른 내용이 없어야("이 줄 자체가 중량 표기") 독립된 중량
        # 줄로 인정한다. "오리온 오그래놀라 시나몬츄러스 440g* 3개"처럼 제품명과
        # 한 줄에 붙어 나오면, 줄 전체를 중량으로 삼켜버리면 안 되므로 여기서는
        # 건너뛰고 나중에 _split_trailing_weight()가 제품명 끝에서 따로 떼어낸다.
        # "100g당 1,211원"처럼 단위당가격 표기도 "100g"으로 시작해서 이 조건에
        # 걸리므로, UNIT_PRICE_PATTERN에 매칭되는 줄은 애초에 중량 후보에서 뺀다.
        if m and not line[:m.start()].strip() and not UNIT_PRICE_PATTERN.search(line):
            result["중량"] = line
            weight_idx = i
            break

    danga_idx = next((i for i, line in enumerate(lines) if "단가" in line), None)

    # 건강기능식품류 라벨은 "코드 -> 한글명 -> 정가 -> 영문명 -> 할인정보 -> 단가"처럼
    # 가격이 영문명보다 먼저 나오는 경우가 있다. 가격처럼 보이는 줄(콤마 포함 금액)이
    # 중량/영문명/단가보다 먼저 나오면 그것도 한글 제품명의 경계로 잡아야, 가격이
    # 한글 제품명에 잘못 딸려 들어가는 걸 막을 수 있다.
    price_boundary_idx = None
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        if PRICE_LINE_PATTERN.match(line):
            price_boundary_idx = i
            break

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

    # 한글 제품명은 상품코드 다음 줄부터(코드를 아예 못 찾았으면 맨 처음부터 -
    # 예: 바코드 구역이 프레임에 없는 화면 캡처), 중량/영문명/단가/가격 중 가장
    # 먼저 나오는 줄 전까지로 본다 (넷 다 없으면 끝까지). 식품은 보통 중량이
    # 경계가 되고, 비식품은 중량이 없으니 영문명이 바로 경계가 된다.
    # 이 구간에는 "RICOLA"처럼 한글이 아닌 브랜드명 줄이 섞여 있을 수 있는데,
    # 실제로는 한글 제품명의 일부이므로("RICOLA 레몬민트 허브캔디") 한글 포함
    # 여부로 거르지 않고 구간 안의 모든 줄을 그대로 합친다.
    boundary_candidates = [
        i for i in (weight_idx, english_idx, danga_idx, price_boundary_idx) if i is not None
    ]
    start = code_idx + 1 if code_idx is not None else 0
    end = min(boundary_candidates) if boundary_candidates else len(lines)
    korean_lines = lines[start:end]
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
    if not result["단가"]:
        # "단가"라는 글자 자체가 없어도 "100g당 1,211원"처럼 단위당 가격이
        # 그대로 찍혀있는 경우가 있다.
        unit_price_match = UNIT_PRICE_PATTERN.search("\n".join(lines))
        if unit_price_match:
            unit = unit_price_match.group(1).replace(" ", "")
            result["단가"] = f"{unit_price_match.group(2)}원/{unit}"

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
        all_prices = [m.group(0) for m in re.finditer(r"[\d,]{2,}\s*원", "\n".join(lines))]
        remaining_prices = [p for p in all_prices if p != danga_price]
        if remaining_prices:
            result["가격"] = max(remaining_prices, key=lambda p: int(re.sub(r"[^\d]", "", p) or "0"))

    # 중량이 독립된 줄로 안 나오고 제품명 끝에 붙어 나온 경우("...440g* 3개")를
    # 대비한 마지막 보정. 이미 독립된 줄에서 중량을 찾은 경우는 건드리지 않는다.
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_weight = _split_trailing_weight(result["제품명(한국어)"])
        if trailing_weight:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_weight

    return result


# ---------------- 트레이더스 가격표 파싱 ----------------
# 코스트코는 상품코드(4~8자리)가 텍스트 맨 앞에 오지만, 트레이더스는 맨 앞이
# 제품명이고 상품코드(바코드 번호, 8~13자리)가 바코드 아래 한참 뒤에 나온다.
# 이 위치 차이로 형식을 판별한다 - 자릿수만으로는 8자리에서 겹칠 수 있어서
# "맨 앞 줄이 코드냐"가 더 안정적인 기준이다.
def detect_retailer(text: str) -> str:
    """
    코스트코 상품코드(4~8자리)와 트레이더스 바코드 번호(9~14자리)는 자릿수가
    겹치지 않는다. 등급 표시("한국파기등급") 같은 잡음 줄이 진짜 코드 위에
    끼어들면 "맨 첫 줄"만으로는 오판별하므로(잡음 줄이 코드가 아니니 트레이더스로
    잘못 판정됨), parse_price_fields()의 코드 탐색과 마찬가지로 텍스트 전체에서
    가장 먼저 나오는 "단독 숫자 줄"을 기준으로 삼는다 - 그 줄의 자릿수로 판별.
    화면 캡처처럼 코드/바코드 구역 자체가 원문에 없어서 어느 쪽도 못 찾으면
    코스트코로 기본 처리한다 - 코스트코 파서가 훨씬 많이 검증되어 있어서,
    형식이 불확실할 때 더 안전한 쪽이다.
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    for line in lines:
        if re.fullmatch(r"\d{4,8}", line):
            return "costco"
        if re.fullmatch(r"\d{9,14}", line):
            return "traders"
    return "costco"


TRADERS_CODE_PATTERN = re.compile(r"\d{8,14}")


def parse_traders_fields(text: str) -> dict:
    """
    트레이더스 가격표 레이아웃(코스트코와 반대 순서): 제품명(한국어, 맨 위) ->
    [제품명(영어)] -> [단가, "100g당 899원"처럼 한 줄에 다 있음] -> 특징 문구 ->
    바코드 -> 상품코드(바코드 번호) -> 판매가(맨 아래).
    실사진으로 검증하며 계속 다듬어야 하는 초기 버전이다 - 코스트코 파서도
    실사진 여러 장을 거치며 여러 번 고친 것과 같은 과정이 필요하다.
    """
    result = {f: "" for f in PRICE_FIELDS}
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    if not lines:
        return result

    result["제품명(한국어)"] = lines[0]

    idx = 1
    if (
        idx < len(lines)
        and re.fullmatch(r"[A-Z0-9 .,'&\-]{4,}", lines[idx])
        and re.search(r"[A-Z]{2,}", lines[idx])
    ):
        result["제품명(영어)"] = lines[idx]
        idx += 1

    danga_match = UNIT_PRICE_PATTERN.search(text)
    if danga_match:
        unit = danga_match.group(1).replace(" ", "")
        result["단가"] = f"{danga_match.group(2)}원/{unit}"

    code_idx = None
    for i, line in enumerate(lines):
        if TRADERS_CODE_PATTERN.fullmatch(line):
            result["상품코드"] = line
            code_idx = i
            break

    # 가격: 상품코드(바코드) 줄 이후에 나오는 첫 콤마 형식 금액을 우선 신뢰한다
    # (코스트코와 마찬가지로 가격은 보통 코드/바코드 다음, 맨 아래에 나온다).
    search_lines = lines[code_idx + 1:] if code_idx is not None else lines
    for line in search_lines:
        m = PRICE_LINE_PATTERN.match(line)
        if m:
            result["가격"] = f"{m.group(1)}원"
            break
    if not result["가격"]:
        all_prices = [m.group(0) for m in re.finditer(r"[\d,]{2,}\s*원", text)]
        if all_prices:
            result["가격"] = max(all_prices, key=lambda p: int(re.sub(r"[^\d]", "", p) or "0"))

    # 트레이더스 파서는 중량을 따로 찾는 로직이 없다 - 제품명 끝에 붙어 나온
    # 경우("...1.5kg")만이라도 분리해서 채운다.
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_weight = _split_trailing_weight(result["제품명(한국어)"])
        if trailing_weight:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_weight

    return result


# ---------------- 제품군정리 시트 ----------------
# 제품명(한국어)에 이 키워드 중 하나가 들어있으면 그 제품군으로 분류한다. 아직
# 마주치지 못한 제품군은 자동으로 UNCATEGORIZED_LABEL로 들어가서 데이터가
# 유실되진 않지만, 실제로 찍히는 상품 종류를 봐가며 이 목록을 계속 채워나가야
# 분류율이 올라간다. 길이가 긴 키워드부터 먼저 시도해야 "유기농 엑스트라버진
# 올리브유"가 "올리브유"보다 먼저 매칭되어 더 구체적인 이름으로 분류된다.
CATEGORY_KEYWORDS = [
    "선크림", "폴로티", "스트레치바지", "반팔티", "콜라겐", "비타민",
    "허브캔디", "방수팩", "선글라스", "팝콘치킨", "레티놀",
]
UNCATEGORIZED_LABEL = "미분류"

# 제품군 블록 하나는 "제목 행" + 아래 14개 항목 행으로 구성된다. 상품은 이
# 항목들을 세로로 채운 열 하나로 표현되고(카드형), 같은 제품군의 상품들이
# 옆으로(B, C, D...) 나란히 쌓인다. OCR 상품카드에는 이 중 상품명/규격·단량/
# 판매가/단위단가/상품코드/파일ID/원문텍스트만 있으므로 CATEGORY_FIELD_MAP에
# 있는 행만 채우고 나머지(사진/소싱형태/매출(연)/매출율/산도/원산지/
# 셀링포인트)는 빈 칸으로 남긴다 - 수기로 채우거나 다른 소스에서 나중에
# 채워 넣을 몫이다.
CATEGORY_ROW_LABELS = [
    "사진", "상품명", "소싱형태", "매출(연)", "규격/단량", "판매가",
    "단위단가", "매출율", "산도", "원산지", "셀링포인트", "상품코드",
    "파일ID", "원문텍스트",
]
CATEGORY_FIELD_MAP = {
    "상품명": "제품명(한국어)",
    "규격/단량": "중량",
    "판매가": "가격",
    "단위단가": "단가",
    "상품코드": "상품코드",
    "파일ID": "파일ID",
    "원문텍스트": "원문텍스트",
}


def detect_category(product_name: str) -> str:
    for kw in sorted(CATEGORY_KEYWORDS, key=len, reverse=True):
        if kw in product_name:
            return kw
    return UNCATEGORIZED_LABEL


def _col_letter(col: int) -> str:
    letters = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _scan_category_blocks(values: list):
    """제품군정리 시트의 현재 내용(get_all_values() 결과)을 읽어서
    {제품군명: {"title_row": 제목 행, "field_rows": {항목명: 행번호}, "next_col": 다음 빈 열}}
    과, 새 제품군 블록을 추가할 때 쓸 다음 행 번호를 함께 돌려준다."""
    blocks = {}
    n_labels = len(CATEGORY_ROW_LABELS)
    r = 0
    while r < len(values):
        col_a = values[r][0] if values[r] else ""
        if col_a and col_a not in CATEGORY_ROW_LABELS:
            title_row = r + 1  # 1-indexed 시트 행 번호
            field_rows = {label: title_row + i for i, label in enumerate(CATEGORY_ROW_LABELS, start=1)}
            name_row_idx = field_rows["상품명"] - 1  # 0-indexed
            name_row_values = values[name_row_idx] if name_row_idx < len(values) else []
            next_col = len(name_row_values) + 1 if name_row_values else 2
            blocks[col_a] = {"title_row": title_row, "field_rows": field_rows, "next_col": next_col}
            r += 1 + n_labels
        else:
            r += 1
    next_new_row = len(values) + 3 if values else 1
    return blocks, next_new_row


def update_category_sheet(sheet, row_dicts: list):
    """방금 처리된 상품들을 제품군 블록 형태로 정리해서 기록한다. 코스트코/
    트레이더스 원본 시트는 건드리지 않는 별도 파생 시트라, 여기서 실수가
    나도 원본 대조로 다시 정리할 수 있다. 여러 스레드가 동시에 열 번호를
    계산하면 충돌하므로, OCR 병렬 처리가 다 끝난 뒤 이 함수 하나만 단일
    스레드로 호출한다. 같은 상품코드가 다시 나와도 항상 새 열을 만든다."""
    if not row_dicts:
        return

    values = sheet.get_all_values()
    blocks, next_new_row = _scan_category_blocks(values)
    updates = []

    for row_dict in row_dicts:
        category = detect_category(row_dict.get("제품명(한국어)") or "")
        block = blocks.get(category)
        if block is None:
            title_row = next_new_row
            field_rows = {label: title_row + i for i, label in enumerate(CATEGORY_ROW_LABELS, start=1)}
            block = {"title_row": title_row, "field_rows": field_rows, "next_col": 2}
            blocks[category] = block
            next_new_row = title_row + 1 + len(CATEGORY_ROW_LABELS) + 2
            updates.append({"range": f"A{title_row}", "values": [[category]]})
            updates.append({
                "range": f"A{title_row + 1}:A{title_row + len(CATEGORY_ROW_LABELS)}",
                "values": [[label] for label in CATEGORY_ROW_LABELS],
            })

        col_letter = _col_letter(block["next_col"])
        for label, source_key in CATEGORY_FIELD_MAP.items():
            value = row_dict.get(source_key) or ""
            row = block["field_rows"][label]
            updates.append({"range": f"{col_letter}{row}", "values": [[value]]})
        block["next_col"] += 1

    if updates:
        sheet.batch_update(updates, value_input_option="USER_ENTERED")


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


def process_one_file(creds, sheets, file_info, archive_folder_id):
    name, file_id = file_info["name"], file_info["id"]
    drive_service = get_thread_drive_service(creds)
    image_bytes = download_image(drive_service, file_id)
    if is_heic(file_info):
        image_bytes = convert_heic_to_jpeg(image_bytes)
    text, confidences = ocr_image_azure(image_bytes)
    low_confidence = needs_review(confidences)

    # 코스트코/트레이더스 사진이 같은 Drive 폴더에 섞여 올라오므로, 텍스트
    # 구조를 보고 형식을 판별해 알맞은 파서와 시트로 보낸다 (detect_retailer
    # 참고). 배경에 다른 가격표가 같이 찍혀도(예: 초점 밖 진열대의 옆 상품)
    # 사진 한 장당 항상 메인 상품 하나만 뽑는다.
    retailer = detect_retailer(text)
    if retailer == "traders":
        fields = parse_traders_fields(text)
        sheet = sheets["traders"]
    else:
        fields = parse_price_fields(text)
        sheet = sheets["costco"]
    row_dict = build_row_dict(file_id, name, fields, text)
    append_rows_to_sheet(sheet, [row_dict])

    # 시트 기록이 끝난 뒤에 사진을 '처리완료' 폴더로 옮긴다. 이동이 실패해도
    # 시트 기록(=파일ID 기준 중복 처리 방지)은 이미 끝났으므로 데이터 유실은
    # 아니다 - 그 사진만 원래 폴더에 계속 남아있을 뿐이라 실행을 실패로
    # 처리하지 않고 경고만 남긴다.
    try:
        archive_file(drive_service, file_id, archive_folder_id)
    except Exception as e:
        print(f"  경고: '{name}' 보관 폴더 이동 실패 (시트 기록은 완료됨): {e}")

    return name, fields.get("제품명(한국어)") or "", low_confidence, row_dict, retailer


# ---------------- 메인 실행 ----------------
def run_once():
    require_config()

    creds = get_credentials()
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    def open_or_create_sheet(sheet_name):
        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # 해당 이름의 탭이 없으면(수동으로 이름을 바꿨거나 아직 안 만든 경우 등)
            # 매 실행마다 에러로 죽는 대신, 그 이름으로 새 탭을 만들어서 계속 진행한다.
            print(f"경고: 시트 탭 '{sheet_name}'을 찾을 수 없어서 새로 만듭니다. "
                  "기존에 다른 이름의 탭에 데이터가 있었다면 그 데이터는 이 탭에 없으니 확인해주세요.")
            return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=26)

    sheets = {
        "costco": open_or_create_sheet(SHEET_NAME),
        "traders": open_or_create_sheet(TRADERS_SHEET_NAME),
    }
    for sheet in sheets.values():
        ensure_header(sheet)
    # 제품군정리는 코스트코/트레이더스 원본과 완전히 다른(블록형) 구조라
    # COLUMN_ORDER 기반 ensure_header 대상이 아니다. 원본 시트와 마찬가지로
    # 리테일러별로 따로 둔다.
    category_sheets = {
        "costco": open_or_create_sheet(CATEGORY_SHEET_NAME_COSTCO),
        "traders": open_or_create_sheet(CATEGORY_SHEET_NAME_TRADERS),
    }

    # 코스트코/트레이더스 어느 시트에 기록됐든 이미 처리한 파일이니, 두 시트의
    # 파일ID를 합쳐서 "처리 완료" 목록으로 삼는다 - 안 그러면 한 시트에만 있는
    # 파일이 다른 시트 기준으로는 계속 "신규"로 보여서 중복 처리될 수 있다.
    processed_ids = load_processed_ids(sheets["costco"]) | load_processed_ids(sheets["traders"])
    all_files = list_all_images(drive_service)
    new_files = [f for f in all_files if f["id"] not in processed_ids]

    if not new_files:
        print("처리할 새 이미지가 없습니다.")
        return

    print(f"신규 이미지 {len(new_files)}건 발견. 처리를 시작합니다...")
    print(f"(Azure 무료 티어 속도 제한: 분당 {AZURE_MAX_CALLS_PER_MINUTE}건 -> "
          f"예상 소요 시간 약 {len(new_files) / AZURE_MAX_CALLS_PER_MINUTE:.0f}분)")

    archive_folder_id = get_or_create_archive_folder(drive_service, DRIVE_FOLDER_ID)

    success_count = 0
    failed = []
    processed_row_dicts = {"costco": [], "traders": []}

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = {
            executor.submit(process_one_file, creds, sheets, f, archive_folder_id): f
            for f in new_files
        }
        for future in as_completed(futures):
            f = futures[future]
            try:
                name, product, flag, row_dict, retailer = future.result()
                success_count += 1
                processed_row_dicts[retailer].append(row_dict)
                flag_str = " [검토필요]" if flag else ""
                print(f"  완료: {name} -> {product or '(제품명 인식 실패)'}{flag_str}")
            except Exception as e:
                failed.append((f["name"], str(e)))
                print(f"  실패: {f['name']} -> {e}")

    # 제품군 블록에 열 번호를 매기는 작업은 동시에 하면 충돌하므로, 병렬
    # 처리가 다 끝난 뒤 단일 스레드로 한 번에 처리한다.
    for retailer, row_dicts in processed_row_dicts.items():
        try:
            update_category_sheet(category_sheets[retailer], row_dicts)
        except Exception as e:
            print(f"경고: {retailer} 제품군정리 시트 갱신 실패 (원본 시트 기록은 정상 완료됨): {e}")

    print(f"\n완료: {success_count}건 성공, {len(failed)}건 실패")
    if failed:
        print("실패 목록 (다음 실행 시 자동 재시도됩니다 - 시트에 기록되지 않은 파일ID는 신규로 취급됨):")
        for name, err in failed:
            print(f"  - {name}: {err}")


if __name__ == "__main__":
    run_once()
