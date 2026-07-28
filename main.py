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

import datetime
import hashlib
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
from PIL import Image, ImageOps
import pillow_heif

pillow_heif.register_heif_opener()  # PIL이 HEIC/HEIF 파일도 열 수 있도록 등록 (아이폰 기본 사진 포맷)

load_dotenv()

# ============ CONFIG (환경변수로 설정, README.md 참고) ============
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "service_account.json")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

# 코스트코/트레이더스 사진을 텍스트 내용으로 자동 판별하던 방식(구 detect_retailer)은
# 오분류가 잦아 폐기했다. 대신 Drive 폴더 자체를 리테일러별로 나눠서, 어느 폴더에서
# 온 사진인지로 형식을 확정한다 - 코스트코 폴더에 넣은 사진은 항상 코스트코 파서로,
# 트레이더스 폴더에 넣은 사진은 항상 트레이더스 파서로만 처리한다.
DRIVE_FOLDER_ID_COSTCO = os.environ.get("DRIVE_FOLDER_ID_COSTCO")
DRIVE_FOLDER_ID_TRADERS = os.environ.get("DRIVE_FOLDER_ID_TRADERS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = os.environ.get("SHEET_NAME", "시트1")
TRADERS_SHEET_NAME = os.environ.get("TRADERS_SHEET_NAME", "트레이더스")
# 코스트코/트레이더스 원본 시트는 그대로 두고, 같은 데이터를 제품군(예: 올리브유)
# 단위로 정리해서 보여주는 별도 시트. 원본 시트가 진짜 원본이고 이 시트는 거기서
# 파생된 정리본이라, 분류가 잘못돼도 원본 대조로 언제든 재정리할 수 있다.
# 코스트코/트레이더스는 상품 성격이 서로 달라서(예: 식품 위주 vs 생활용품 위주)
# 제품군 블록도 원본 시트와 마찬가지로 리테일러별로 따로 둔다.
CATEGORY_SHEET_NAME_COSTCO = os.environ.get("CATEGORY_SHEET_NAME_COSTCO", "제품군정리(코스트코)")
CATEGORY_SHEET_NAME_TRADERS = os.environ.get("CATEGORY_SHEET_NAME_TRADERS", "제품군정리(트레이더스)")

# 월별 분리 이전에 이미 기록된 "정리본위치" 값(시트제목 없이 "C15"만 있음)은
# 그 시절 유일했던 시트 이름(위 CATEGORY_SHEET_NAME_*)으로 찾아야 하므로
# sync_back_sourcing()에서 쓴다 - 그 시절 데이터가 남아있는 옛 탭 이름일
# 뿐이고, 지금 새로 만드는 월별 탭 이름과는 별개다(아래 참고).
_LEGACY_CATEGORY_SHEET_TITLE = {"costco": CATEGORY_SHEET_NAME_COSTCO, "traders": CATEGORY_SHEET_NAME_TRADERS}

# 정리본은 촬영월별로 시트를 나눈다("정리본(코스트코)_2026-07"처럼 이 이름
# 뒤에 월을 붙인 제목을 씀 - run_once의 category_sheet_title 참고). 위
# CATEGORY_SHEET_NAME_*(옛 "제품군정리(...)" 단일 시트, 월별 분리 이전
# 데이터 전용)과는 별개의 이름을 쓴다 - 그래야 기존 데이터를 안 건드리고
# 새 탭만 원하는 이름 규칙으로 만들 수 있다.
MONTHLY_CATEGORY_SHEET_NAME_COSTCO = os.environ.get("MONTHLY_CATEGORY_SHEET_NAME_COSTCO", "정리본(코스트코)")
MONTHLY_CATEGORY_SHEET_NAME_TRADERS = os.environ.get("MONTHLY_CATEGORY_SHEET_NAME_TRADERS", "정리본(트레이더스)")

# 자사 상품 카탈로그(MCH1/MC/상품명) 기반 제품군 분류용. 거래처/공급사 이름까지
# 들어있는 사외비 데이터라 이 공개 저장소에는 절대 커밋하지 않고, 서비스
# 계정에 뷰어로 공유해둔 별도의 비공개 구글 시트에서 실행 시점에 읽어온다.
# 값이 없으면 카탈로그 매칭 없이 CATEGORY_KEYWORDS 수동 목록만으로 분류한다.
MC_CATALOG_SPREADSHEET_ID = os.environ.get("MC_CATALOG_SPREADSHEET_ID")
MC_CATALOG_SHEET_NAME = os.environ.get("MC_CATALOG_SHEET_NAME", "시트5")

AZURE_VISION_ENDPOINT = os.environ.get("AZURE_VISION_ENDPOINT")
AZURE_VISION_KEY = os.environ.get("AZURE_VISION_KEY")

# Azure F0(무료 티어) 제한: 분당 20건. 여유를 두고 18건/분으로 제한.
AZURE_MAX_CALLS_PER_MINUTE = int(os.environ.get("AZURE_MAX_CALLS_PER_MINUTE", "18"))

# Azure Read API v3.2가 받아주는 최대 파일 크기(4MB)에 여유를 둔 값.
_AZURE_MAX_IMAGE_BYTES = 4 * 1024 * 1024
_AZURE_SAFE_IMAGE_BYTES = int(_AZURE_MAX_IMAGE_BYTES * 0.9)
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
# 나중에 재검증이 필요하면 그대로 다시 볼 수 있고, 신규 업로드 폴더(코스트코/
# 트레이더스 각각의 DRIVE_FOLDER_ID_*)는 다음 실행의 조회 대상에서 계속 가벼운
# 상태로 유지된다. 리테일러별 원본 폴더 아래에 각자 '처리완료' 하위 폴더를 둔다.
ARCHIVE_FOLDER_NAME = "처리완료"

# 핵심 컬럼: 식품/비식품 가릴 것 없이 모든 SKU에 공통으로 채워지는 것들.
CORE_PRICE_FIELDS = ["상품코드", "제품명(한국어)", "가격"]
# 비핵심 컬럼: 사진에 찍혀있으면 채우고, 없으면 빈 칸으로 남긴다.
OPTIONAL_PRICE_FIELDS = ["제품명(영어)", "중량", "단가"]
PRICE_FIELDS = CORE_PRICE_FIELDS + OPTIONAL_PRICE_FIELDS

# "정리본위치"는 이 카드가 제품군정리(카테고리) 시트의 어느 칸(열+제목행)에
# 기록됐는지를 "C15" 같은 값으로 남겨둔다. 제품군정리 시트 자체에는 추적용
# ID를 두지 않는다는 원칙(7절 참고)을 지키면서도, 카드보다 늦게 올라오는
# 뒷면 사진(소싱형태/병입원산지)을 나중에 그 칸에 소급 반영할 수 있게 해준다.
COLUMN_ORDER = (
    ["파일ID", "파일명", "처리일시", "원문텍스트"] + PRICE_FIELDS
    + ["업로더", "뒷면여부", "촬영시각", "사진해시", "매칭파일ID", "정리본위치"]
)


def require_config():
    missing = [
        name
        for name in [
            "DRIVE_FOLDER_ID_COSTCO",
            "DRIVE_FOLDER_ID_TRADERS",
            "SPREADSHEET_ID",
            "AZURE_VISION_ENDPOINT",
            "AZURE_VISION_KEY",
        ]
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


# 같은 날 촬영된 완전히 동일한 사진이 실수로 두 번 업로드되는 경우를 걸러내기
# 위한 락. same_day_hashes 집합(리테일러별로 하나씩)은 시트에서 미리 읽어온
# 과거 기록에 이번 실행 중 새로 처리한 사진의 (해시, 촬영 날짜)를 스레드
# 여러 개가 동시에 더해가는 구조라 잠금이 필요하다.
same_day_hash_lock = threading.Lock()


def load_same_day_hashes(sheet):
    """시트에 이미 기록된 사진들 중 '사진해시'와 '촬영시각'이 둘 다 있는 행을
    (해시, 촬영 날짜) 쌍의 집합으로 돌려준다. 파일ID(Drive가 업로드마다 새로
    부여하는 값)만으로는 "내용이 완전히 같은 사진을 실수로 두 번 올린 경우"를
    못 잡아내므로, 여기서는 사진 내용(해시)까지 같은지 본다. 다만 날짜까지
    같아야만 중복으로 치므로, 다음 달에 같은 상품을 재촬영해 올리는 경우처럼
    날짜가 다르면 해시가 같아도(예: 우연히 똑같이 나온 사진) 걸리지 않는다 -
    시기별로 SKU 이력을 새로 쌓으려는 목적과 어긋나지 않게 하기 위함이다."""
    idx = {name: COLUMN_ORDER.index(name) for name in ("파일ID", "촬영시각", "사진해시")}
    columns = {name: sheet.col_values(i + 1)[1:] for name, i in idx.items()}  # 헤더 제외
    row_count = len(columns["파일ID"])
    for name in columns:
        if len(columns[name]) < row_count:
            columns[name] += [""] * (row_count - len(columns[name]))

    pairs = set()
    for i in range(row_count):
        capture_time = columns["촬영시각"][i]
        image_hash = columns["사진해시"][i]
        if capture_time and image_hash:
            pairs.add((image_hash, capture_time[:10]))
    return pairs


# ---------------- Drive: 전체 이미지 조회 (페이지네이션 포함) ----------------
def list_all_images(drive_service, folder_id):
    query = (
        f"'{folder_id}' in parents and "
        "(mimeType = 'image/jpeg' or mimeType = 'image/png' "
        "or mimeType = 'image/heic' or mimeType = 'image/heif') and trashed = false"
    )
    files = []
    seen_ids = set()
    page_token = None
    while True:
        results = drive_service.files().list(
            q=query,
            # owners(emailAddress): 앞뒤 사진 매칭에 쓸 "누가 올렸는지" 신호.
            # 개인 Drive 폴더(공유 드라이브 아님)에 각자 계정으로 올리는
            # 구조라 업로드한 사람 소유로 파일이 생성되므로 이 필드에 실제
            # 업로더 이메일이 찍힌다.
            fields="nextPageToken, files(id, name, mimeType, owners(emailAddress))",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        # Drive API 페이지네이션 도중 폴더 내용이 바뀌면(사진이 계속 업로드되는
        # 도중에 조회하는 경우 등) 같은 파일이 서로 다른 페이지에 중복으로
        # 걸려나오는 경우가 실제로 있다 - 같은 파일ID가 시트에 두 번 기록되는
        # 문제로 이어지므로 여기서 미리 걸러낸다.
        for f in results.get("files", []):
            if f["id"] not in seen_ids:
                seen_ids.add(f["id"])
                files.append(f)
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


def archive_file(drive_service, file_id, archive_folder_id, source_folder_id):
    drive_service.files().update(
        fileId=file_id,
        addParents=archive_folder_id,
        removeParents=source_folder_id,
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


def normalize_image_for_ocr(image_bytes: bytes) -> bytes:
    """Azure Read API로 보내기 전에 두 가지를 항상 맞춘다.

    1) HEIC/HEIF(아이폰 기본 사진 포맷)는 Azure가 지원하지 않으므로 JPEG로
       변환한다.
    2) 휴대폰 카메라는 센서가 담은 원본 픽셀을 항상 가로로 저장하고 "실제로는
       이만큼 돌려서 봐야 한다"는 EXIF 방향 정보만 따로 붙이는 경우가 많다.
       사진 뷰어/갤러리 앱은 이 태그를 자동으로 반영해서 똑바로 보여주지만,
       Azure Read API는 이 태그를 반영하지 않고 원본 픽셀 그대로 받아들인다
       (Microsoft 공식 문서에도 나온 알려진 제약) - 그래서 사람 눈엔 멀쩡한
       사진도 OCR 엔진에는 옆으로 누운 채로 전달되어 인식률이 떨어질 수 있다.
       ImageOps.exif_transpose()로 픽셀 자체를 세우고 방향 태그는 지운
       뒤(그래야 아래에서 새로 저장할 때 이중으로 안 돌아간다) 보낸다.

    HEIC든 JPEG/PNG든 상관없이 항상 거친다 - 방향이 이미 정상인 사진은
    exif_transpose()가 그대로 돌려주므로 아무 변화가 없다.

    3) Azure Read API v3.2는 4MB가 넘는 이미지를 그냥 거부한다(400). 최신
       스마트폰의 원본 해상도 사진(4284x5712급)은 92% 품질로 재인코딩해도
       4MB를 훌쩍 넘는 경우가 실사진(2026-01-01 트레이더스 업로드분 다수)에서
       확인됐다 - 이런 사진은 Azure 호출이 재시도까지 다 실패하고 '실패'
       목록에만 조용히 남아서 시트에도 안 쓰이고 '처리완료' 폴더로도 안
       옮겨진다. 사용자 입장에서는 사진이 통째로 안 읽히고 폴더에 그대로
       남아있는 것처럼 보인다(실제로 그런 상태다). 화질을 단계적으로 낮춰서
       상한 아래로 맞추고, 그래도 안 되면 마지막 수단으로 가로/세로를
       줄인다 - 글자 인식에는 압축 품질보다 해상도가 더 중요해서, 해상도
       축소는 화질 저하보다 뒤에 시도한다."""
    image = Image.open(io.BytesIO(image_bytes))
    image = ImageOps.exif_transpose(image)
    rgb = image.convert("RGB")

    out = io.BytesIO()
    rgb.save(out, format="JPEG", quality=92)
    encoded = out.getvalue()

    for quality in (85, 75, 65):
        if len(encoded) <= _AZURE_SAFE_IMAGE_BYTES:
            break
        out = io.BytesIO()
        rgb.save(out, format="JPEG", quality=quality)
        encoded = out.getvalue()

    while len(encoded) > _AZURE_SAFE_IMAGE_BYTES and min(rgb.size) > 1000:
        rgb = rgb.resize((int(rgb.width * 0.85), int(rgb.height * 0.85)), Image.LANCZOS)
        out = io.BytesIO()
        rgb.save(out, format="JPEG", quality=75)
        encoded = out.getvalue()

    return encoded


# EXIF DateTimeOriginal(0x9003)은 최상위 IFD가 아니라 "Exif" 서브 IFD(0x8769)
# 안에 있다 - Image.getexif()만으로는 안 보이고 get_ifd()로 한 번 더 들어가야
# 한다. DateTimeDigitized(0x9004)는 촬영 직후 자동 백업 등으로 아주 드물게
# DateTimeOriginal이 비어있을 때의 보조값.
_EXIF_IFD_EXIF = 0x8769
_EXIF_DATETIME_ORIGINAL = 0x9003
_EXIF_DATETIME_DIGITIZED = 0x9004
_EXIF_DATETIME_FALLBACK = 0x0132  # 최상위 IFD의 DateTime(파일 수정시각에 가까움, 최후 보조값)

# 삼성 카메라 기본 파일명(20260713_111558.jpg)에 박힌 촬영시각. EXIF가 없는
# 드문 경우의 2순위 폴백.
_FILENAME_TIMESTAMP_PATTERN = re.compile(r"(20\d{2})(\d{2})(\d{2})[_-](\d{2})(\d{2})(\d{2})")
# 아이폰 기본 파일명(IMG_1234.HEIC)의 일련번호 - 절대시각이 아니라 기기
# 안에서만(사진 라이브러리 전체 기준으로) 유효한 카운터지만, 촬영 순서와는
# 항상 일치한다. 원래는 이 이유로 순서 판별 대상에서 뺐었는데, 남이 전달해준
# 사진처럼 EXIF가 아예 없고 YYYYMMDD_HHMMSS 형식도 아닌 경우(아이폰 원본
# 파일명 그대로 전달됨)엔 이거라도 없으면 순서를 전혀 알 수 없어서 3순위
# 폴백으로 추가한다. 실제 시각이 아니라 "상대적 순서"만 의미있으므로 임의
# 기준 시점(2000-01-01)에 번호(초)를 더한 가짜 시각으로 바꿔서 돌려준다 -
# 같은 배치 안에서는 정렬이 맞지만, 이 폴백을 쓴 사진과 진짜 EXIF/2순위
# 파일명 패턴을 쓴 사진이 한 그룹에 섞이면(같은 업로더가 올린 사진 중 일부만
# EXIF가 없는 경우) 상대적 순서가 깨질 수 있다는 한계가 있다.
_FILENAME_IMG_SEQUENCE_PATTERN = re.compile(r"IMG[_-]?(\d{3,6})", re.IGNORECASE)


def extract_capture_time(image_bytes: bytes, filename: str):
    """사람별로 사진을 촬영 순서대로 정렬하기 위한 시각을 구한다. Drive
    업로드 완료 시각(createdTime)은 파일 크기·네트워크 상태에 따라 완료
    순서가 실제 촬영 순서와 달라질 수 있어 쓰지 않는다 - 카메라가 셔터를
    누른 순간 자동으로 남기는 EXIF 촬영시각을 1순위로, 그마저 없으면(예:
    메신저를 거치며 메타데이터가 지워진 사진) 삼성 파일명의 날짜_시각
    패턴을 2순위로, 그것도 아니면 아이폰 IMG_1234류 일련번호를 3순위로
    쓴다. 아이폰 HEIC도 pillow_heif가 등록한 오프너를 통해 JPEG와 동일하게
    EXIF를 읽는다. 변환 전 원본 바이트에서 읽어야 한다 - normalize_image_for_ocr()로
    재인코딩하면 방향 태그가 지워진다. 셋 다 없으면 순서를 알 수 없다는
    뜻이므로 None을 돌려주고, 그 사진은 앞뒤 매칭 대상에서 빠진다(데이터
    자체는 RAW 시트에 그대로 남는다)."""
    try:
        image = Image.open(io.BytesIO(image_bytes))
        exif = image.getexif()
        raw = None
        if exif:
            exif_ifd = exif.get_ifd(_EXIF_IFD_EXIF)
            raw = (
                exif_ifd.get(_EXIF_DATETIME_ORIGINAL)
                or exif_ifd.get(_EXIF_DATETIME_DIGITIZED)
                or exif.get(_EXIF_DATETIME_FALLBACK)
            )
        if raw:
            return datetime.datetime.strptime(raw.strip(), "%Y:%m:%d %H:%M:%S")
    except Exception:
        pass

    m = _FILENAME_TIMESTAMP_PATTERN.search(filename)
    if m:
        try:
            return datetime.datetime(*(int(g) for g in m.groups()))
        except ValueError:
            pass

    m = _FILENAME_IMG_SEQUENCE_PATTERN.search(filename)
    if m:
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=int(m.group(1)))

    return None


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
                    return _extract_text_and_confidence(result, image_bytes)
                if result.get("status") == "failed":
                    raise RuntimeError("Azure OCR 작업 실패")
            raise RuntimeError("Azure OCR 결과 대기 시간 초과")

        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # 지수 백오프

    raise RuntimeError("Azure OCR 재시도 횟수 초과")


# 순수 가격/할인액 숫자 줄("23,980", "-2,000", "- 2,000", "21,980원")만
# 매칭한다 - 이런 줄은 위치와 무관하게 파싱 로직이 카드 전체에서 찾아내므로,
# 뒤로 밀려도 지장이 없다. "100ml당 2398원"처럼 글자가 섞인 줄은 매칭 안
# 되므로 건드리지 않는다.
_PRICE_TOKEN_RE = re.compile(r"^-?\s?[\d,]+\s*원?$")


def _defer_offcolumn_price_tokens(positioned):
    """
    셀링포인트 문구(왼쪽)와 가격 스택(오른쪽)이 나란히 배치된 2단 카드에서는,
    오른쪽 가격 숫자 하나의 Y좌표가 왼쪽 문구의 줄바꿈된 두 줄 사이 높이에 걸려서
    Y좌표만으로 정렬하면 그 문구가 둘로 쪼개진다("...소스," 다음에 "41,980"이
    끼어들고 그 다음에야 "드레싱에 적합"이 나오는 식). 순수 숫자 줄이면서 Y좌표상
    바로 앞/뒤 줄보다 확연히 오른쪽(그 줄의 오른쪽 끝보다 더 오른쪽)에서
    시작하면 - 즉 같은 줄이 아니라 다른 컬럼에 있다는 뜻 - 그 줄을 원래 자리에서
    빼서 뒤로 미룬다. 앞/뒤 줄도 숫자 줄이면(가격이 여러 줄로 쌓여 나오는 정상
    레이아웃) 건드리지 않는다 - 그런 경우는 애초에 문장을 쪼개고 있는 게 아니다.
    """
    def is_price_token(e):
        return bool(_PRICE_TOKEN_RE.match(e["text"].strip()))

    main, deferred = [], []
    for i, e in enumerate(positioned):
        if not is_price_token(e):
            main.append(e)
            continue
        neighbors = [
            n for n in (positioned[i - 1] if i > 0 else None,
                        positioned[i + 1] if i + 1 < len(positioned) else None)
            if n is not None and not is_price_token(n)
        ]
        if neighbors and all(e["left_x"] > n["right_x"] for n in neighbors):
            deferred.append(e)
        else:
            main.append(e)
    return main, deferred


# 제품 뒷면 한글표시사항은 "라벨" 칸(제품명/식품유형/제조사/수입/판매업소/
# 원재료명/내용량/원산지/보관방법 등)과 "값" 칸이 나란히 놓인 표 형태가
# 많다. 이 표는 라벨 칸이 좁고 값 칸과 뚜렷한 간격을 두고 떨어져 있어서,
# 위 2단(좌/우) 재배열 로직이 이걸 "셀링포인트가 좌/우로 나뉜 카드"로 착각해
# "라벨 전부 -> 값 전부" 순서로 통째로 재배열해버리는 사고가 실사진(HUGHSON
# 아몬드/NOEL 하몽/BIFFI 버섯소스)에서 확인됐다. 원래 순서(라벨1,값1,라벨2,
# 값2,...)가 이미 올바른 읽기 순서인 표에 이 재배열을 적용하면 "수입"
# 라벨이 그 값("(주)코스트코 코리아")과 완전히 떨어진 곳으로 밀려나서,
# determine_sourcing()이 둘을 짝지어 읽지 못해 "직수입" 판정을 놓친다.
# 그래서 후보 줄들 중 이런 표시사항 라벨 낱말로 시작하는 줄이 여럿이면
# 이 카드는 표 형태로 보고 재배열 자체를 건너뛴다.
_BACK_LABEL_FIELD_WORDS = (
    "제품명", "식품유형", "제조사", "제조원", "제조업체", "수입원", "수입업소",
    "수입판매업소", "수입", "판매업소", "판매원", "원재료명", "내용량", "원산지",
    "제조국", "소비기한", "유통기한", "보관방법", "조리방법", "반품", "교환장소",
    "포장재질", "영양정보", "품목보고번호",
)


def _looks_like_field_label_table(entries) -> bool:
    label_like = sum(
        1 for e in entries
        if any(e["text"].strip().startswith(w) for w in _BACK_LABEL_FIELD_WORDS)
    )
    return label_like >= 3


def _reorder_two_column_block(entries):
    """셀링포인트(특징) 문구가 카드 하단에 좌/우 2단으로 나란히 배치된
    레이아웃이 실사진에서 확인됐다(예: 왼쪽 "샌드위치, 피자토핑, ... 사용
    가능합니다.", 오른쪽 "라이트와인, 과일과도 어울림. ..." 처럼 서로 다른
    두 문장이 나란히 놓임). y좌표만으로 정렬하면(위 함수들이 처리하는
    "가격 숫자 하나가 끼어드는" 경우와 달리) 두 단의 줄이 위에서부터 통째로
    번갈아 섞여 두 문장이 뒤죽박죽 하나로 합쳐진다("왼쪽줄1, 오른쪽줄1,
    왼쪽줄2, 오른쪽줄2, ..." 순서로 나와서 셀링포인트 파싱이 엉뚱한 문구를
    만들어냄).

    실사진(BelGioioso 모짜렐라)으로 확인해보니, 사진에 배경 물체(옆에 붙어
    있는 같은 상품의 박스 더미 등)가 가격표와 거의 붙어서 같이 찍히면
    _cluster_lines_by_layout()의 간격/색상 분리가 항상 성공하는 건 아니라
    그 배경 텍스트까지 같은 목록에 섞여 들어온다. 이때 "카드 폭"을 사진
    전체(entries 전부)의 min/max로 계산하면, 배경 물체의 훨씬 넓게 퍼진
    텍스트가 폭 기준을 왜곡해서(예: 실제로는 크게 벌어진 2단 간격이 부풀려진
    "전체 폭"의 15%에 못 미치는 것처럼 보임) 진짜 2단 카드조차 인식하지
    못하는 문제가 있었다. 그래서 순수 숫자 가격 줄(_PRICE_TOKEN_RE)을
    경계로 사진을 여러 구간으로 나누고, 각 구간의 폭은 그 구간 자체의
    줄들로만 계산한다 - 실제 카드 내용(코드~단가 안내)과 그 뒤에 이어지는
    배경 물체 텍스트가 보통 가격 줄 하나 이상을 사이에 두고 떨어져 있어서,
    이렇게 구간을 나누면 배경 물체의 폭이 실제 카드 구간의 폭 계산을 더 이상
    왜곡하지 않는다.

    각 구간 안에서는, 폭이 좁아 한쪽에만 있는 줄들만 좌/우로 클러스터링해서
    "왼쪽 단을 위→아래로 다 읽은 뒤 오른쪽 단을 위→아래로" 순서로 재배열한다.
    뚜렷한 2단 구조(그 구간 폭의 15% 이상 벌어진 간격, 양쪽 모두 2줄 이상)가
    안 보이면 그 구간은 원래 순서를 그대로 유지한다 - 대부분의 카드는 이
    함수가 사실상 아무것도 바꾸지 않는 단일 컬럼 레이아웃이다."""
    if len(entries) < 4:
        return entries

    result = []
    i = 0
    n = len(entries)
    while i < n:
        j = i
        while j < n and not _PRICE_TOKEN_RE.match(entries[j]["text"].strip()):
            j += 1
        result.extend(_reorder_segment_if_two_column(entries[i:j]))
        if j < n:
            result.append(entries[j])  # 가격 줄 자체는 그대로 이어붙인다
        i = j + 1
    return result


# 좌/우 재배열 지점에 심어두는 표시. "왼쪽 단을 전부 읽은 뒤 오른쪽 단"으로
# 줄 순서 자체는 바로잡아도, 그 경계 정보가 그대로 사라지면 텍스트만 보는
# 이후 단계(extract_selling_points)는 왼쪽 단의 마지막 불릿과 오른쪽 단의
# 불릿 없는 이어지는 문장을 구분 못 하고 하나로 붙여버린다(실사진 확인:
# "*샌드위치나 토스트용으로 적합"(왼쪽 단 마지막 불릿) 바로 뒤에 "개봉 후
# 부패, 변질 될..."(오른쪽 단, 불릿 없음)이 와서 같은 항목으로 합쳐짐).
# 실제 OCR 텍스트에 나올 리 없는 문자열이라 안전하게 구분자로 쓸 수 있다.
_COLUMN_BREAK_MARKER = "\x00COLUMN_BREAK\x00"


def _reorder_segment_if_two_column(segment):
    """_reorder_two_column_block()이 가격 줄로 나눈 한 구간 안에서, 그
    구간 자체의 폭을 기준으로 2단 구조를 판단해 재배열한다."""
    if len(segment) < 4:
        return segment

    if _looks_like_field_label_table(segment):
        return segment

    min_left = min(e["left_x"] for e in segment)
    max_right = max(e["right_x"] for e in segment)
    width = max_right - min_left
    if width <= 0:
        return segment

    narrow = [e for e in segment if (e["right_x"] - e["left_x"]) < width * 0.6]
    if len(narrow) < 4:
        return segment

    xs = sorted(e["left_x"] for e in narrow)
    gaps = [(xs[i + 1] - xs[i], i) for i in range(len(xs) - 1)]
    biggest_gap, split_i = max(gaps)
    if biggest_gap < width * 0.15:
        return segment  # 뚜렷한 2단 구분이 없음 - 원래 순서 유지
    boundary_x = (xs[split_i] + xs[split_i + 1]) / 2

    left_ids = {id(e) for e in narrow if e["left_x"] < boundary_x}
    right_ids = {id(e) for e in narrow if e["left_x"] >= boundary_x}
    if len(left_ids) < 2 or len(right_ids) < 2:
        return segment

    # 연속으로 이어지는 좌/우 후보만 "왼쪽 전부 -> 오른쪽 전부"로 재배열한다 -
    # 그 사이에 이 구간 폭 전체를 쓰는 줄이 끼면 거기서 새 구간으로 다시
    # 시작해서, 서로 다른 2단 블록이 뒤섞이지 않게 한다.
    result = []
    k = 0
    while k < len(segment):
        e = segment[k]
        if id(e) in left_ids or id(e) in right_ids:
            m = k
            block = []
            while m < len(segment) and (id(segment[m]) in left_ids or id(segment[m]) in right_ids):
                block.append(segment[m])
                m += 1
            left_block = [x for x in block if id(x) in left_ids]
            right_block = [x for x in block if id(x) in right_ids]
            result.extend(left_block)
            if left_block and right_block:
                # 왼쪽 단 마지막 줄과 오른쪽 단 첫 줄 사이에 경계 표시를
                # 심어서, 순서만 바로잡고 경계 정보 자체는 잃어버리는 걸
                # 막는다 (아래 _COLUMN_BREAK_MARKER 설명 참고).
                anchor = left_block[-1]
                result.append({
                    "top_y": anchor["top_y"], "bottom_y": anchor["bottom_y"],
                    "left_x": anchor["left_x"], "right_x": anchor["right_x"],
                    "text": _COLUMN_BREAK_MARKER,
                })
            result.extend(right_block)
            k = m
        else:
            result.append(e)
            k += 1
    return result


# ---- 사진 한 장에 여러 물체(다른 상품 박스, 배경의 다른 가격표 등)가 같이
# 찍혔을 때, 진짜 대상 상품카드가 아닌 것들을 걸러내기 위한 레이아웃 분석 ----
# 같은 상품카드 안의 줄들은 세로 간격이 좁고 배경색도 일정하다. 다른 물체로
# 넘어가면 보통 간격이 크게 벌어지거나(다른 진열 위치) 배경색이 확 달라진다
# (다른 색 포장 박스). 이 두 신호로 줄들을 물리적 덩어리(클러스터)로 나누고,
# 그 중 가장 큰(=사진에서 가장 크게, 가까이 찍힌 - 즉 실제 촬영 대상일 가능성이
# 가장 높은) 덩어리만 남긴다.
#
# 다만 오탐(false split)을 특히 조심해야 한다: 카드 안에 작은 색깔 강조
# 라벨(예: 빨간 "할인" 표시)이 있어도 그 앞뒤 줄과의 세로 간격 자체는 카드
# 안의 다른 줄 간격과 비슷하다(같은 카드 안에 촘촘히 배치되어 있으므로). 그래서
# "색만 다르다"는 절대 단독으로 분리 근거로 안 쓴다.
#
# 반대 방향의 오탐도 실사진에서 확인됐다: 안토넬라/홀그레인처럼, 같은 가격표
# 안에서도 본문(제품명/셀링문구)과 아래쪽 바코드+가격 구역 사이에 디자인상
# 여백이 큰 카드가 많다 - 이 여백이 카드 밖 다른 물체와의 간격만큼 커질 수
# 있어서, "간격이 아주 크면 색과 무관하게 분리한다"는 예전 규칙이 같은 카드를
# 둘로 쪼개버렸다. 그 뒤 가격이 있는 덩어리를 우선하는 로직과 맞물려 본문
# 전체(제품명 등)가 통째로 버려지는 심각한 회귀로 이어졌다. 그래서 간격은
# 색이 실제로 다를 때만 분리 근거로 쓴다 - 색이 같으면(같은 흰 바탕 가격표
# 안이라는 뜻) 간격이 아무리 커도 절대 분리하지 않는다.
#
# "색이 실제로 다르다"만으로는 아직 부족했다 - 같은 흰 가격표라도 태그
# 홀더의 그림자나 조명 반사(글레어) 때문에 본문 쪽과 가격 쪽의 밝기가 크게
# 차이나는 경우가 실사진 대부분에서 확인됐다(예: (153,159,163) vs
# (219,223,225), 유클리드 거리 110 - 위 60 기준을 훌쩍 넘는데도 둘 다
# 채도는 0.06/0.03으로 사실상 무채색). 그림자/글레어는 R/G/B 세 채널을
# 거의 같은 비율로 밝게 또는 어둡게 만들 뿐이라 색 자체(채도)는 그대로
# 무채색에 가깝게 남는다 - 실제로 다른 색 포장/라벨(카드 밖 다른 물체)만
# 뚜렷한 채도 차이를 만든다. 그래서 유클리드 거리가 크더라도 두 색 다
# 무채색이면(채도가 낮으면) "밝기 차이일 뿐"으로 보고 분리하지 않는다.
_CLUSTER_GAP_RATIO = 1.2      # 이 배수 이상 벌어지면서 색도 다르면 분리
_CLUSTER_COLOR_DISTANCE = 60  # RGB 유클리드 거리 기준
_CLUSTER_MIN_SATURATION = 0.15  # 이 미만이면 무채색(흰/회/검)으로 간주
# 무채색 두 개라도 밝기 차이가 이 값을 넘으면(예: 거의 검정 vs 거의 흰색)
# 조명 차이로 보지 않는다 - 실사진(진열대 위 다른 상품의 검은 스펙 안내판
# 배경 vs 그 아래 흰 코스트코 태그)에서 확인된 실제 밝기차보다 한참 위,
# 같은 흰 태그 안의 그림자/글레어로 확인된 밝기차(최대 약 90)보다는 한참
# 아래로 잡아서 두 경우를 가른다.
_CLUSTER_MAX_SHADOW_BRIGHTNESS_GAP = 130


def _orient_image_to_match(image, expected_width, expected_height):
    """휴대폰 사진은 카메라 센서가 담은 원본 픽셀은 가로로 저장하고, "실제로는
    90도 돌려서 봐야 한다"는 EXIF 방향 정보만 따로 붙여두는 경우가 많다(세로로
    찍은 사진인데 파일 자체 크기는 4000x2252처럼 가로로 저장됨). PIL이 파일을
    열 때는 이 EXIF 방향을 자동으로 반영하지 않으므로, Azure가 bounding box
    좌표의 기준으로 삼은 페이지 크기(readResults[].width/height)와 실제로
    안 맞을 수 있다 - 이 상태로 배경색을 샘플링하면 완전히 엉뚱한 영역을
    짚게 된다. 어느 쪽이 맞는지 추측하지 않고, Azure가 알려준 크기와 실제로
    맞아떨어지는 회전을 찾아서 맞춘다."""
    if not expected_width or not expected_height:
        return image
    if image.size == (expected_width, expected_height):
        return image
    for rotation in (Image.ROTATE_90, Image.ROTATE_180, Image.ROTATE_270):
        rotated = image.transpose(rotation)
        if rotated.size == (expected_width, expected_height):
            return rotated
    return image  # 그래도 안 맞으면 원본 그대로 (배경색 판단은 부정확할 수 있음)


def _sample_background_color(image, left_x, top_y, right_x, bottom_y):
    """바운딩박스 영역을 1x1로 축소해서 평균색을 낸다. 글자 획은 가늘어서
    영역 내 픽셀 비중이 적기 때문에, 평균을 내면 자연스럽게 배경색 쪽으로
    많이 치우친다. 실패하면(영역이 이미지 밖이거나 크기가 0 등) None을
    돌려주고, 호출부는 이 줄을 색 판단에서 그냥 제외한다."""
    try:
        box = (
            max(0, int(left_x)), max(0, int(top_y)),
            min(image.width, int(right_x)), min(image.height, int(bottom_y)),
        )
        if box[2] <= box[0] or box[3] <= box[1]:
            return None
        crop = image.crop(box)
        return crop.resize((1, 1)).convert("RGB").getpixel((0, 0))
    except Exception:
        return None


def _color_distance(c1, c2):
    return sum((a - b) ** 2 for a, b in zip(c1, c2)) ** 0.5


def _saturation(c):
    """0(완전 무채색: 흰/회/검)~1(뚜렷한 색) 범위로, RGB 세 채널이 서로
    얼마나 벌어져 있는지를 본다. 그림자/글레어는 세 채널을 거의 같은 비율로
    밝게/어둡게 만들 뿐이라 채도는 낮게 유지되고, 실제로 다른 색 물체만
    채도가 뚜렷하게 올라간다."""
    mx, mn = max(c), min(c)
    return (mx - mn) / mx if mx else 0.0


def _brightness(c):
    return sum(c) / 3


def _is_different_background(c1, c2):
    """유클리드 거리만으로는 흰 가격표 위의 밝기 차이(그림자/글레어)와 실제로
    다른 색 물체를 구분하지 못한다 - 둘 다 채도가 낮으면(무채색이면) 거리가
    아무리 커도 조명 차이일 뿐 다른 물체가 아니라고 본다.

    다만 무채색이라는 것만으로 전부 조명 차이로 넘기면 안 된다 - 진열대 위
    다른 상품(예: 조명 스펙을 검은 바탕에 흰 글씨로 안내하는 판)이 바로
    위/옆에 찍히면, 그 검은 배경과 이 카드의 흰 배경도 둘 다 "무채색"이지만
    실제로는 완전히 다른 두 물체다(실사진에서 확인됨 - TAPO 카메라 태그
    바로 위 다른 상품의 검은 스펙 안내판). 이런 경우는 밝기 차이 자체가
    그림자/글레어로 설명 가능한 범위를 훨씬 넘어서므로(거의 검정 vs 거의
    흰색), 무채색이어도 밝기 차이가 너무 크면 다른 물체로 본다."""
    achromatic = (
        _saturation(c1) < _CLUSTER_MIN_SATURATION
        and _saturation(c2) < _CLUSTER_MIN_SATURATION
    )
    if achromatic and abs(_brightness(c1) - _brightness(c2)) <= _CLUSTER_MAX_SHADOW_BRIGHTNESS_GAP:
        return False
    return _color_distance(c1, c2) >= _CLUSTER_COLOR_DISTANCE


def _cluster_lines_by_layout(positioned):
    """세로 간격(+ 보조적으로 배경색)을 보고 줄들을 물리적 덩어리로 나눈다."""
    if not positioned:
        return []
    clusters = [[positioned[0]]]
    for prev, cur in zip(positioned, positioned[1:]):
        prev_height = max(1, prev["bottom_y"] - prev["top_y"])
        cur_height = max(1, cur["bottom_y"] - cur["top_y"])
        avg_height = (prev_height + cur_height) / 2
        gap = cur["top_y"] - prev["bottom_y"]
        gap_ratio = gap / avg_height

        should_break = False
        if gap_ratio >= _CLUSTER_GAP_RATIO and prev.get("color") and cur.get("color"):
            should_break = _is_different_background(prev["color"], cur["color"])

        if should_break:
            clusters.append([cur])
        else:
            clusters[-1].append(cur)
    return clusters


def _cluster_area(cluster):
    return sum((e["right_x"] - e["left_x"]) * (e["bottom_y"] - e["top_y"]) for e in cluster)


def _cluster_has_price(cluster):
    """진짜 가격표 덩어리는 항상 "23,980"류 콤마 가격 줄을 포함한다. 옆/뒤에
    같이 찍힌 다른 상품의 포장 문구(브랜드명, 성분표 등)는 글자 크기가 커서
    실제 가격표보다 픽셀 면적이 더 큰 경우가 실사진에서 확인됐다 - 면적만
    보면 이런 배경 포장이 이겨버린다. 가격 줄이 있는 덩어리를 우선하면 이
    문제를 피할 수 있다."""
    return any(PRICE_LINE_PATTERN.match(e["text"].strip()) for e in cluster)


def _extract_text_and_confidence(result, image_bytes=None):
    """
    Azure가 반환하는 줄 순서는 항상 사진의 실제 위→아래 순서와 일치하지는
    않는다 (특히 바코드/상품코드 구역, 제품명 구역, 가격 구역처럼 서로 떨어진
    영역이 있는 사진에서 순서가 뒤섞이는 경우가 있다). 우리 파싱 로직은 "N번째
    줄" 식으로 순서에 의존하므로, 각 줄의 bounding box 중 가장 작은 y좌표(윗쪽
    끝)를 기준으로 재정렬해서 실제 시각적 순서에 가깝게 맞춘다.
    """
    entries = []
    confidences = []
    page_width = page_height = None
    for page in result.get("analyzeResult", {}).get("readResults", []):
        if page_width is None:
            page_width, page_height = page.get("width"), page.get("height")
        for line in page.get("lines", []):
            text_val = line.get("text", "")
            bbox = line.get("boundingBox") or []
            if len(bbox) >= 8:
                xs, ys = bbox[0::2], bbox[1::2]
                entries.append({
                    "top_y": min(ys), "bottom_y": max(ys),
                    "left_x": min(xs), "right_x": max(xs), "text": text_val,
                })
            else:
                entries.append({"top_y": None, "text": text_val})
            for word in line.get("words", []):
                conf = word.get("confidence")
                if conf is not None:
                    confidences.append(conf)

    positioned = sorted((e for e in entries if e["top_y"] is not None), key=lambda e: e["top_y"])
    unpositioned = [e for e in entries if e["top_y"] is None]

    # 사진에 다른 물체(옆/뒤 진열대의 다른 상품, 다른 가격표)가 같이 찍혀서
    # 그 텍스트까지 섞여 들어오는 걸 막기 위해, 실제 이미지 픽셀에서 배경색을
    # 뽑아 세로 간격과 함께 레이아웃을 분석한다. 이미지를 못 열거나 실패해도
    # (다음 실행에서 이미지 형식이 바뀌는 등) 원래 동작으로 안전하게 넘어간다 -
    # 이 분석은 어디까지나 정확도를 높이는 보조 수단이라, 실패했다고 파이프라인
    # 전체가 죽으면 안 된다.
    if image_bytes and positioned:
        try:
            image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            image = _orient_image_to_match(image, page_width, page_height)
            for e in positioned:
                e["color"] = _sample_background_color(
                    image, e["left_x"], e["top_y"], e["right_x"], e["bottom_y"]
                )
            clusters = _cluster_lines_by_layout(positioned)
            if len(clusters) > 1:
                priced_clusters = [c for c in clusters if _cluster_has_price(c)]
                positioned = max(priced_clusters or clusters, key=_cluster_area)
        except Exception as e:
            print(f"  경고: 레이아웃(배경색/간격) 분석 실패, 필터링 없이 진행: {e}")

    positioned = _reorder_two_column_block(positioned)
    main_lines, deferred_lines = _defer_offcolumn_price_tokens(positioned)
    full_text = "\n".join(e["text"] for e in main_lines + deferred_lines + unpositioned)
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
# 숫자 부분은 "1,540g"처럼 천단위 콤마가 있는 경우와 "2500g"처럼 콤마 없이
# 이어붙은 경우를 둘 다 받아야 한다. `\d{1,3}(?:,\d{3})+`(콤마 형식 전용) 하나만
# 쓰면 "2500"처럼 콤마 없는 4자리 숫자는 통째로 매칭 실패하므로, 콤마 형식과
# 일반 연속 숫자를 |로 나눠 우선 콤마 형식을 시도하고 안 되면 일반 숫자로
# 받는다.
# "30CM X 43.4M"(랩/호일류 롤 제품)처럼 폭을 cm로 표기하는 경우가 실사진에서
# 확인됐다 - cm도 단위 목록에 추가한다.
WEIGHT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\s*(g|ml|kg|cm|l|m)(?![a-wyz])",
    re.IGNORECASE,
)
# 콤마 천단위 구분자를 Azure가 마침표로 잘못 읽는 경우가 실사진에서 확인됐다
# ("9,790"이 "9.790"으로) - 한국 판매가엔 소수점이 없으므로, 이 위치(3자리씩
# 묶는 자리)에 나오는 마침표는 사실상 항상 콤마의 오독이다. 그래서 콤마와
# 마침표를 둘 다 구분자로 받는다.
PRICE_LINE_PATTERN = re.compile(r"^(\d{1,3}(?:[,.]\d{3})+)\s*\S{0,2}$")
# 할인액 앞의 "-"도 Azure가 en dash(–)/em dash(—)/마이너스 기호(−)로 잘못
# 읽을 수 있어(모양이 거의 같음) 같이 받는다 - 안 받으면 할인액 줄이
# 셀링포인트 불릿으로 잘못 채택될 위험이 있다. 천단위 구분자도 가격 줄과
# 마찬가지로 마침표로 잘못 읽힐 수 있어 같이 받는다.
DISCOUNT_LINE_PATTERN = re.compile(r"^[-–—−]\s?[\d,.]+\s*원?$")


def _find_discount_amount(lines):
    for line in lines:
        m = DISCOUNT_LINE_PATTERN.match(line)
        if m:
            return int(re.sub(r"[^\d]", "", line))
    return None


# "값이 제일 큰 게 정가"라는 규칙만으로는, 배경에 겹친 다른 가격표나 프로모션
# 문구("100ml당 2398원"의 "2398" 등)를 OCR이 콤마 포함 형태로 잘못 읽어들여
# 실제 정가보다 큰 숫자를 만들어내는 경우를 못 걸러낸다(예: 실제 카드엔 없는
# "41,980"이 23,980/21,980과 나란히 인식됨). 할인액 줄이 있으면, 할인은 항상
# "정가 - 할인액 = 할인가" 산수가 정확히 맞아야 하므로 그 관계를 만족하는
# 조합을 우선 찾는다 - 환각으로 생긴 숫자는 어떤 후보와도 할인액만큼 정확히
# 차이나지 않으므로 이 검증에서 자연스럽게 걸러진다. 검증되는 조합이 없으면
# (할인이 없거나 산수가 안 맞으면) 기존처럼 최댓값을 정가로 본다.
def _price_to_int(p: str) -> int:
    # PRICE_LINE_PATTERN이 콤마와 마침표를 둘 다 천단위 구분자로 받으므로,
    # 숫자로 바꿀 땐 어느 쪽이었든 그냥 다 지운다. 값을 다시 쓸 때는 항상
    # f"{n:,}"로 콤마 형식으로 통일해서, 마침표로 잘못 읽힌 값이 그대로
    # 시트에 남지 않게 한다.
    return int(re.sub(r"[^\d]", "", p))


def _select_regular_price(prices, discount):
    if discount:
        values = [_price_to_int(p) for p in prices]
        for a in values:
            for b in values:
                if a != b and a - discount == b:
                    return f"{a:,}원"
    return f"{max(_price_to_int(p) for p in prices):,}원"


# 중량이 독립된 줄이 아니라 "오리온 오그래놀라 시나몬츄러스 440g* 3개"처럼
# 제품명 끝에 그대로 붙어 나오는 경우가 많다. 중량 단위 뒤에 "* 3개", "x2개",
# "포"처럼 짧은 수량 표시만 더 있고 그걸로 줄이 끝나면, 그 지점부터를 전부
# 중량 쪽으로 떼어낸다. "1L(퓨어)"처럼 단위 뒤에 짧은 괄호 설명만 붙는 경우도
# 마찬가지로 허용한다.
#
# "츠바키 ... 180g*2+리필 150g+펌프"처럼, 중량 조각 사이/뒤에 "리필"/"펌프"
# 같은 짧은 한글 설명이 끼어드는 콤보도 실사진에서 확인됐다 - 단순 접속기호
# (+,x,×)만 허용하던 기존 이음새로는 이 한글 단어에서 막혀 버렸다. 이음새에
# 곱셈 배수(*2 등)와 짧은 한글 단어(4자 이하)를 같이 허용해서, 중량 조각들
# 사이(뒤에서 볼 때는 앞으로 병합, 앞에서 볼 때는 뒤로 확장) 모두에 쓴다.
_SPEC_JOIN_RE = r"(?:[*xX×]\s*\d+)?[\s+xX×,]*[가-힣]{0,4}[\s+xX×,]*"

_TRAILING_QUANTITY_RE = re.compile(
    r"^[\sx×X*]*\d*\s*(개|포|입|병|팩|ea|EA|Ea)?\.?\s*(\([^)]{1,10}\))?"
    r"(?:" + _SPEC_JOIN_RE + r")*$"
)

# "100g당 899원"처럼 "단가"라는 글자 없이 "N단위당 M원" 형태로만 찍히는 경우도
# 있다 (트레이더스뿐 아니라 코스트코 화면 캡처류에서도 나온다). 두 파서 모두
# 이 패턴을 단가 보조 신호로 쓴다. 무게 단위(g/ml/kg/l/m)뿐 아니라 "1개당
# 2,570원"처럼 개수 단위가 오는 경우도 실사진 다수(트레이더스 상품 대부분)에서
# 확인됐다 - "개"가 g/ml/kg/l/m 목록에 없어서 "당" 앞에서 매칭 자체가
# 끊겨 통째로 실패하고 있었다. 이미 다른 개수 규격 판별에서 쓰는 단위
# 목록(정/적/캡슐/포/병/팩/롤/입/매/개)을 그대로 같이 받는다.
UNIT_PRICE_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?\s*(?:g|ml|kg|l|m|정|적|캡슐|포|병|팩|롤|입|매|개)?)\s*당\s*([\d,]+)\s*원",
    re.IGNORECASE,
)

# "392원/개", "233원/100 ml"처럼 "단가"라는 글자도, "~당~원" 문구도 없이
# 그냥 "금액원/단위" 형태로 한 줄에 통째로 찍히는 세제류 카드도 있다. 같은
# 정보를 "1개당"/"1회당"처럼 두 단위로 중복 표기해서 이런 줄이 두 개 나오는
# 경우도 실사진에서 확인됐다("392원/개"와 "392원/회") - 카드에 먼저(=더
# 위쪽에) 인쇄된 줄을 대표값으로 쓴다.
_STANDALONE_UNIT_PRICE_RE = re.compile(r"^([\d,]+)\s*원\s*/\s*(.+)$")


def _find_standalone_unit_price(lines: list) -> str:
    for line in lines:
        m = _STANDALONE_UNIT_PRICE_RE.match(line)
        if m:
            unit = m.group(2).replace(" ", "")
            return f"{m.group(1)}원/{unit}"
    return ""


# 건강기능식품류는 무게(g/ml/kg 등) 대신 "80포", "180정", "1,000MG × 180정"처럼
# 개수만으로 규격을 나타내는 경우가 많다 - WEIGHT_PATTERN이 다루는 단위가 아니라
# 지금까지는 그냥 제품명에 붙은 채로 남아있었다. "정"을 OCR이 "적"으로 잘못
# 읽는 경우가 실제로 있어서("112적") 같이 받아준다. "입"/"매"(개당 낱개 수를
# 세는 아주 흔한 단위, "3입"/"12입"/"6입", "145매")도 마찬가지라 같이 받는다.
# "30캡슐× 3EA"처럼 개수 단위 뒤에 "× 3EA"류 배수 표기가 한 번 더 붙는
# 경우도 있어("캡슐개수 × 낱개묶음수"), 있으면 그 배수까지 통째로 포함한다.
# "160매 / 대형"처럼 개수 단위 뒤에 "/ 대형"류 크기 설명이 한 번 더 붙는
# 경우도 실사진에서 확인됐다(지퍼백 등 크기가 여러 종류인 제품) - 있으면
# 그 크기 설명까지 규격에 포함해서 통째로 뗀다.
# "청소용브러쉬세트 4종"처럼 낱개 수가 아니라 "종류 수"로 구성을 세는
# 세트 상품도 있다 - "종"도 개수 단위에 같이 포함한다. "62개입"처럼 "낱개
# 수 + 입"이 붙어 다니는 표기("OO개입")도 있어("아로퓸 캡슐세제 62개입")
# 하나의 단위로 같이 받는다.
_TRAILING_COUNT_RE = re.compile(
    r"((?:[\d,]+\s*(?:MG|mg|G|g)\s*[×xX]\s*)?\d{1,4}\s*(?:정|적|캡슐|포|병|팩|롤|입|매|종|개입)"
    r"(?:\s*[×xX*]\s*\d+\s*(?:EA|ea|Ea|개)?)?"
    r"(?:\s*/\s*(?:대형|중형|소형|특대형))?)\s*$"
)

# "쉬크 하이드로5 스킨 프로텍트 기*2+날*14", "질레트 프로글라이드 파워
# (면도기1+날8)"처럼, 무게 단위도 개수 단위도 아니라 "부품명+숫자"를 "+"로
# 이어 붙인 구성품 목록으로 규격을 나타내는 카드도 실사진에서 확인됐다(면도기
# 종류에 많음 - 손잡이/면도날처럼 세트 구성이 다양해서). 앞의 두 방식(무게/
# 개수 단위)으로 못 걸러지므로 별도 패턴이 필요하다. 부품명 하나짜리는(예:
# 제품명 끝의 우연한 "숫자") 오탐 위험이 있어 최소 두 조각(부품+숫자가 "+"로
# 두 번 이상) 이어진 경우만 인정한다.
_COMPONENT_COMBO_RE = re.compile(
    r"(?:^|\s)([가-힣]{1,4}\*?\d{1,3}(?:\s*\+\s*[가-힣]{1,4}\*?\d{1,3})+)\s*$"
)


def _is_spec_combo(text: str) -> bool:
    """중량/개수/구성 콤보 규격 중 하나에라도 해당하는지 확인한다. 괄호로
    통째로 감싸인 규격 표기를 뗄 때, 안의 내용이 어떤 종류의 규격이든 다
    인식하기 위해 세 가지를 한 번에 확인한다."""
    text = text.strip()
    return bool(
        WEIGHT_PATTERN.search(text)
        or _TRAILING_COUNT_RE.fullmatch(text)
        or _COMPONENT_COMBO_RE.fullmatch(text)
    )


def _split_trailing_weight(name: str) -> tuple:
    """제품명 문자열 끝에 중량이 붙어있으면 (중량 뗀 제품명, 중량)을 돌려주고,
    없으면 (원래 이름, "")을 그대로 돌려준다."""
    # "에어프라이어 4L+1.5L"처럼 규격 전체가 괄호로 감싸인 채 나오는 경우가
    # 아니라, "(48g * 12입)"처럼 규격 표기 전체가 괄호 하나로 감싸인 채 붙는
    # 경우도 있다. 괄호 안이 어떤 종류든 규격으로 인식되면 괄호 전체를 중량
    # 자리로 떼어낸다 - 괄호(와 안의 콤보 구조)는 버리고 내용만 남긴다.
    # (괄호 안에 규격이 없는 "1L(퓨어)" 같은 경우는 여기 해당 없이 아래 일반
    # 로직으로 넘어간다.)
    paren_match = re.search(r"\(([^()]*)\)\s*$", name)
    if paren_match and _is_spec_combo(paren_match.group(1)):
        return name[:paren_match.start()].strip(), paren_match.group(1).strip()

    matches = list(WEIGHT_PATTERN.finditer(name))
    if not matches:
        return name, ""
    last = matches[-1]
    rest = name[last.end():]
    if not _TRAILING_QUANTITY_RE.match(rest):
        return name, ""
    start = last.start()
    # "에어프라이어 4L+1.5L"처럼 콤보 규격이 "+" 같은 짧은 연결자(+ 곱셈 배수,
    # + 짧은 한글 설명)로 이어진 경우, 맨 뒤 조각("1.5L")만 떼면 앞 조각
    # ("4L")을 잃어버린다. 바로 앞 중량 조각과의 사이가 이음새뿐이면 그
    # 조각까지 같이 묶어서 중량으로 인정한다.
    for prev in reversed(matches[:-1]):
        gap = name[prev.end():start]
        if re.fullmatch(_SPEC_JOIN_RE, gap):
            start = prev.start()
        else:
            break
    return name[:start].strip(), name[start:].strip()


def _split_trailing_count(name: str) -> tuple:
    """제품명 문자열 끝에 개수 기반 규격이 붙어있으면 (규격 뗀 제품명, 규격)을
    돌려주고, 없으면 (원래 이름, "")을 그대로 돌려준다."""
    m = _TRAILING_COUNT_RE.search(name)
    if not m:
        return name, ""
    return name[:m.start()].strip(), m.group(1).strip()


def _split_trailing_component_combo(name: str) -> tuple:
    """제품명 문자열 끝에 "부품명+숫자"를 "+"로 이어 붙인 구성품 목록(무게도
    개수 단위도 아닌 규격)이 붙어있으면 (규격 뗀 제품명, 규격)을 돌려주고,
    없으면 (원래 이름, "")을 그대로 돌려준다."""
    m = _COMPONENT_COMBO_RE.search(name)
    if not m:
        return name, ""
    return name[:m.start(1)].strip(), m.group(1).strip()


# "덴티스테 플러스 화이트 치약 세트" 같은 세트 상품은 제품명 자체엔 규격이
# 전혀 안 붙고, 대신 셀링포인트 줄에 "- 구성: 치약 160g*2+60g*1+20g*2"처럼
# 별도로 규격이 나온다 - 위 세 방법은 전부 제품명 문자열 끝에서만 찾으므로
# 이 경우를 못 잡는다. "구성:" 문구가 있는 줄을 따로 찾아서 그 안에서
# 중량 패턴이 시작하는 지점부터 끝까지를 규격으로 본다("치약" 같은 품목명
# 접두어는 중량이 없는 부분이라 자연히 잘려나간다).
_COMPOSITION_LABEL_RE = re.compile(r"구성\s*[:：]\s*(.+)$")


def _find_composition_spec(lines: list) -> str:
    for line in lines:
        m = _COMPOSITION_LABEL_RE.search(line)
        if not m:
            continue
        content = m.group(1).strip()
        spec_match = WEIGHT_PATTERN.search(content)
        if spec_match:
            return content[spec_match.start():].strip()
    return ""


# "신세계포인트", "Global Product", "적립", "행사기간"처럼 셀링포인트 문구가
# 아니라 코스트코/트레이더스가 라벨 양식 자체에 고정으로 찍는 정형 문구들.
# 상품마다 달라지는 게 아니라 양쪽 매장이 재사용하는 고정 문구라 목록이
# 무한정 늘어나진 않는다 - 새로운 정형 문구를 마주치면 여기에 추가한다.
_SELLING_POINT_EXCLUDE_SUBSTRINGS = (
    "신세계포인트", "Global Product", "적립", "카드할인", "행사기간",
    "회원가", "비회원가",
)


def _is_selling_point_noise(line: str) -> bool:
    """셀링포인트 문장의 이어지는 줄로 착각하면 안 되는, 다른 종류의 줄인지
    판단한다 (가격/단가/코드/영문제품명/정형 프로모션 문구)."""
    if PRICE_LINE_PATTERN.match(line) or DISCOUNT_LINE_PATTERN.match(line):
        return True
    if UNIT_PRICE_PATTERN.search(line):
        return True
    if re.fullmatch(r"\d{4,14}", line):  # 상품코드/바코드
        return True
    if re.fullmatch(r"[A-Z0-9 .,'&×\-]{4,}", line) and re.search(r"[A-Z]{2,}", line):
        return True  # 영문 제품명류
    if "단가" in line:  # "단가 / 개", OCR이 "/"를 "ㅣ"로 잘못 읽은 "단가 ㅣ 개" 포함
        return True
    return any(sub in line for sub in _SELLING_POINT_EXCLUDE_SUBSTRINGS)


# 의류 사이즈표("66CM : 26")처럼, 카드 안의 다른(관련 없는) 컬럼에서 끼어든 게
# 명백한 줄은 지금 조립 중인 항목을 거기서 끝내면 안 된다 - 두 컬럼짜리
# 카드에서 셀링포인트 문구 중간에 옆 컬럼의 사이즈표 한 줄이 Y좌표상 끼어드는
# 경우가 실사진에서 확인됐다("* 시원하고 건조가 빠른" 다음에 사이즈표 몇 줄이
# 끼고 나서야 "쿨맥스(COOLMAX) 원단 사용"이 이어짐). 이런 줄은 항목을 안
# 끝내고 그냥 건너뛰어야, 그 뒤에 이어지는 진짜 내용을 놓치지 않는다 - 반면
# 코드/영문명/단가처럼 "여기서부터는 확실히 다른 절"임을 뜻하는 노이즈는
# 지금처럼 항목을 끝내는 게 맞다.
_SIZE_TABLE_LINE_RE = re.compile(r"^\d{2,3}\s*cm\s*[:.]?\s*\d{1,3}$", re.IGNORECASE)


def _is_skippable_interleaved_noise(line: str) -> bool:
    return bool(_SIZE_TABLE_LINE_RE.match(line))


def _is_origin_label_line(line: str) -> bool:
    """상품카드(뒷면이 아니라 앞면) 자체에 "원산지 : 영국"처럼 원산지 표기가
    셀링포인트 문구 자리에 별도 줄로 나오는 경우가 실사진에서 확인됐다. 이
    값은 이제 _parse_fields_from_lines()가 병입원산지로 따로 뽑으므로,
    셀링포인트 추출에는 안 섞이게 건너뛴다(끝내는 게 아니라 건너뛰는 이유:
    이 줄 바로 다음에 진짜 셀링 문구가 이어지는 카드가 실사진에서 확인됨)."""
    return bool(_ORIGIN_LABEL_PATTERN.search(line))


# 대부분은 "-"/"•"/"·"로 시작하지만, 카드에 따라 "▶"(섹션 제목으로도 쓰이지만
# "-" 불릿이 아예 없는 카드에서는 그 자체가 유일한 불릿 표시인 경우도 있다)나
# "*"만 쓰는 경우도 실사진에서 확인됐다(에어프라이어, 청바지 카드 등). 다만
# "▶"/"*"는 에버콜라겐/덴프스처럼 "-" 불릿 앞에 붙는 섹션 제목으로도 흔히
# 쓰이므로, 무조건 같이 인식하면 그 제목 줄까지 불릿으로 오인해서 실제
# 내용과 뒤섞인다. 그래서 우선순위를 둔다: "-•·"만으로 뭔가 찾아지면 그걸로
# 끝내고, 하나도 못 찾을 때만 "▶"를 추가해서 다시 시도하고, 그래도 없으면
# "*"까지 추가해서 마지막으로 시도한다.
# 인쇄된 하이픈을 Azure가 en dash(–)/em dash(—)/마이너스 기호(−)로 잘못
# 읽는 경우도 있다 - 사람 눈엔 다 "-"로 보이는 모양이 거의 같은 글자들이라
# 첫 등급부터 같이 받는다(예전 등급에 없어서 이런 사진은 셀링포인트가 통째로
# 안 뽑히는 문제가 있었다).
_BULLET_MARKER_TIERS = ("-–—−•·", "-–—−•·▶", "-–—−•·▶*")


def _extract_selling_points_with_markers(lines, markers):
    bullet_re = re.compile(rf"^[{re.escape(markers)}]\s*(.+)$")
    points = []
    current = None
    for line in lines:
        m = bullet_re.match(line)
        # "-5,000 원"처럼 할인액도 대시로 시작하는데, "원"이라는 글자 자체가
        # 한글이라서 "뒤에 한글/영문이 있으면 진짜 항목"이라는 원래 조건을
        # 통과해버려 가짜 셀링포인트로 채택되는 경우가 실사진에서 확인됐다
        # (NEPA/BRIGGS/바이탈뷰티 카드 등). DISCOUNT_LINE_PATTERN에 걸리는
        # 줄은 "원"이 붙어있어도 항목 시작으로 인정하지 않는다.
        if m and DISCOUNT_LINE_PATTERN.match(line):
            m = None
        if m and re.search(r"[가-힣A-Za-z]", m.group(1)):
            if current:
                points.append(current.strip())
            current = m.group(1).strip()
            continue
        # "▶"가 이번 등급에서 불릿 문자로 안 쓰이면("-" 불릿이 이미 찾아진
        # 경우), "▶제품 특징" / "▶섭취량 및 섭취방법"처럼 카드 안의 서로 다른
        # 절 제목으로 쓰인다. 첫 번째 절 제목은(아직 아무 불릿도 못 찾은
        # 상태이므로) 그냥 지나치지만, 이미 불릿을 하나라도 찾은 뒤에 또
        # 다른 "▶" 제목이 나오면 - 그건 "섭취방법"처럼 셀링포인트가 아닌
        # 다른 절로 넘어갔다는 뜻이므로 거기서 추출을 멈춘다.
        if "▶" not in markers and line.startswith("▶") and (current is not None or points):
            break
        if current is None:
            continue
        if _is_skippable_interleaved_noise(line) or _is_origin_label_line(line):
            continue
        if _is_selling_point_noise(line):
            points.append(current.strip())
            current = None
            continue
        current += " " + line
    if current:
        points.append(current.strip())
    return points


def extract_selling_points(text: str, product_code: str = "") -> str:
    """
    "- 신선하고 품질이 좋은 ... 압착하여" 다음 줄에 "만든 오일"처럼, 대시로
    시작하는 셀링포인트 문구가 화면 폭 때문에 줄바꿈되어 여러 줄로 나뉘어
    나온다. 대시/불릿 문자로 시작하는 줄을 새 항목의 시작으로 보고, 다음
    대시가 나오기 전까지 이어지는 줄들을 계속 붙인다 - 단, 가격/코드/영문명/
    정형 프로모션 문구처럼 셀링포인트가 아닌 게 뻔한 줄이 나오면 그 항목은
    거기서 끝난다. "- 2,000"처럼 할인액도 대시로 시작하므로, 대시 뒤에
    글자(한글/영문)가 하나도 없으면 애초에 항목 시작으로 인정하지 않는다.
    실사진으로 계속 검증하며 다듬어야 하는 초기 버전이다.

    product_code(파싱된 상품코드)가 주어지면 그 코드가 적힌 줄 이후부터만
    스캔한다. 사진에 다른 상품이 배경으로 같이 찍히면, 그 상품 포장의 "•"
    불릿 문구가 실제 카드보다 먼저 나오면서 잘못 채택되는 경우가 실사진에서
    확인됐다 - "개봉후 부패·변질될 우려가 있으니 냉장..."/"부정 불량식품
    신고는 국번없이 1399" 같은 문구는 코스트코 라벨 대부분에 똑같이 들어가는
    표준 문구라 배경 상품에도 거의 항상 있어서 특히 잘 걸린다. 코드를
    못 찾았거나(뒷면 사진 등) product_code가 안 주어지면 전체 텍스트를
    그대로 스캔한다(기존 동작과 동일).
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    if product_code:
        for i, line in enumerate(lines):
            if _normalize_bare_code(line, allow_five_digit=True) == product_code:
                lines = lines[i + 1:]
                break

    # _COLUMN_BREAK_MARKER가 있으면 그 지점에서 줄 목록을 나눠 각 구간을
    # 독립적으로 처리한다 - 왼쪽 단의 마지막 항목과 오른쪽 단의 첫 내용이
    # 서로 다른 구간에 속한다는 걸 알아야, 둘을 하나의 항목으로 잘못
    # 이어붙이지 않는다. 마커가 없는 사진(대다수)은 구간이 하나뿐이라
    # 기존 동작과 동일하다.
    segments = [[]]
    for line in lines:
        if line == _COLUMN_BREAK_MARKER:
            segments.append([])
        else:
            segments[-1].append(line)

    all_points = []
    for seg_idx, seg_lines in enumerate(segments):
        seg_points = []
        for markers in _BULLET_MARKER_TIERS:
            seg_points = _extract_selling_points_with_markers(seg_lines, markers)
            if seg_points:
                break
        if not seg_points:
            fallback = (
                _find_unmarked_description(seg_lines) if seg_idx == 0
                else _collect_plain_description(seg_lines)
            )
            if fallback:
                seg_points = [fallback]
        all_points.extend(seg_points)

    if not all_points:
        return ""
    return "- " + all_points[0] + "".join(f"\n - {p}" for p in all_points[1:])


# 베이커리류 카드는 셀링포인트에 "-"/"▶" 같은 불릿 표시가 아예 없이, 영문
# 제품명 바로 다음에 화면 폭 때문에 여러 줄로 줄바꿈된 순수 설명 문장 하나만
# 있는 경우가 실사진에서 확인됐다(신라명과 탕종식빵, 삼립 토마토피자브레드
# 등). 불릿 기반 추출(위 세 등급)이 전부 아무것도 못 찾았을 때만 쓰는 마지막
# 폴백으로, 영문 제품명 줄(대문자 2단어 이상 - english_idx와 같은 조건) 바로
# 다음부터 노이즈(가격/단가/코드/정형문구 등)가 나오거나 한글이 없는 줄이
# 나오기 전까지 이어지는 줄들을 통째로 한 문장으로 합친다.
def _find_unmarked_description(lines: list) -> str:
    # "THE LAUGHING COW"처럼 브랜드명이 코드 바로 다음 줄에 대문자 2단어
    # 이상으로 찍히는 경우가 있어서(_parse_fields_from_lines의 english_idx
    # 판별과 같은 함정), 한글 줄을 하나도 못 봤으면 대문자 줄을 진짜 영문
    # 제품명으로 인정하지 않는다 - 그러지 않으면 브랜드명 줄을 english_idx로
    # 잘못 채택해서, 그 바로 다음에 오는 진짜 영문 제품명 줄(예: "BELCUBE
    # PLAIN 250GX2")에서 한글이 없다는 이유로 스캔이 너무 일찍 끝나버리고,
    # 정작 그 뒤에 이어지는 진짜 셀링 문구는 하나도 못 줍는다.
    english_idx = None
    seen_korean_line = False
    for i, line in enumerate(lines):
        if re.search(r"[가-힣]", line):
            seen_korean_line = True
        if not seen_korean_line:
            continue
        if (
            re.fullmatch(r"[A-Z0-9 .,'&×\-]{4,}", line)
            and re.search(r"[A-Z]{2,}", line)
            and len(line.split()) >= 2
        ):
            english_idx = i
            break
    if english_idx is None:
        return ""

    desc_lines = []
    for line in lines[english_idx + 1:]:
        if not re.search(r"[가-힣]", line):
            break
        # "단가 / 10G"처럼 단가 라벨만 있는 줄과, 그 라벨과 짝을 이루는 순수
        # 가격 숫자 줄("196원")은 그 자체로 셀링포인트 내용이 아니지만, 이런
        # 줄 하나 때문에 문장 수집을 완전히 끝내버리면 안 된다 - 2단
        # 레이아웃 카드에서 이 줄 뒤에 또 다른(오른쪽 단) 셀링 문구가
        # 이어지는 경우가 실사진(벨지오이오조 모짜렐라)에서 확인됐다.
        # "100g당 1,211원"처럼 실제 숫자 값이 있는 단가 표기는 예외 - 그건
        # 진짜 값이 있는 줄이라 노이즈로 처리해 문장을 끊는 게 맞다.
        if _is_origin_label_line(line):
            continue
        if "단가" in line and not UNIT_PRICE_PATTERN.search(line):
            continue
        if _PRICE_TOKEN_RE.match(line.strip()):
            continue
        if _is_skippable_interleaved_noise(line) or _is_selling_point_noise(line):
            break
        desc_lines.append(line)
    return " ".join(desc_lines).strip()


def _collect_plain_description(lines: list) -> str:
    """_COLUMN_BREAK_MARKER로 나뉜, 첫 구간이 아닌 구간(2단 카드의 오른쪽
    단 등)에는 이미 영문 제품명이 앞 구간에서 소비돼서 없는 경우가 많다.
    영문 제품명 줄을 찾는 게 목적이 아니라 구간 자체가 이미 "왼쪽 단과는
    분리된 하나의 덩어리"라는 게 마커로 보장되므로, _find_unmarked_description과
    같은 노이즈 판정만 그대로 쓰고 구간 맨 앞부터 바로 수집한다."""
    desc_lines = []
    for line in lines:
        if not re.search(r"[가-힣]", line):
            break
        if _is_origin_label_line(line):
            continue
        if "단가" in line and not UNIT_PRICE_PATTERN.search(line):
            continue
        if _PRICE_TOKEN_RE.match(line.strip()):
            continue
        if _is_skippable_interleaved_noise(line) or _is_selling_point_noise(line):
            break
        desc_lines.append(line)
    return " ".join(desc_lines).strip()


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


def _normalize_bare_code(line: str, allow_five_digit: bool = False) -> str:
    """OCR이 코드 숫자 중간에 공백을 잘못 끼워넣는 경우가 실제로 있다
    ("196 987" - 진짜 코드는 "196987"). 이러면 원래 fullmatch(r"\\d{4,8}")가
    실패해서 코드 자체를 못 찾고, 그 뒤로 코드가 없는 것처럼 처리되어 배경
    잡음까지 제품명에 다 딸려 들어간다. 공백을 뺀 값이 6~8자리 숫자면 코드로
    인정하고 그 공백 없는 값을 돌려준다 - 공백이 둘 이상이면(진짜 코드가 아닐
    가능성이 높은 문장류) 코드로 인정하지 않는다. 코드가 아니면 빈 문자열.
    (하한을 6자리로 둔 이유: 실제 운영 데이터의 모든 코스트코 상품코드는
    6~7자리였고, 유일한 5자리 "코드"는 사진에 같이 찍힌 스펙 플래카드의
    "50 000"(진짜 상품카드가 아닌 배경 물체)가 잘못 코드로 인식된
    경우였다 - 5자리 이하는 애초에 진짜 상품코드일 가능성이 없다.)
    청과류는 진짜 상품코드가 5자리인 경우가 실사진에서 확인됐다("30669"
    델몬떼 바나나 등). allow_five_digit=True를 넘기면 5자리도 인정하는데,
    호출부에서 "카드의 맨 첫 줄"일 때만 이 플래그를 켠다 - 진짜 상품코드는
    항상 카드 맨 위에 오고, 위에서 설명한 배경 잡음 오탐 사례는 첫 줄이
    아니었으므로 이렇게 위치를 제한하면 6자리 하한을 둔 원래 취지를 그대로
    지키면서 5자리 코드만 추가로 구제할 수 있다."""
    if line.count(" ") > 1:
        return ""
    compact = line.replace(" ", "")
    min_digits = 5 if allow_five_digit else 6
    return compact if re.fullmatch(rf"\d{{{min_digits},8}}", compact) else ""


# 사람들이 Drive 폴더에 상품카드(가격표) 사진뿐 아니라 제품 뒷면(영양정보/
# 원재료명/제조사 표기 등) 사진도 같이 올린다 - 폴더가 같아서 구조적으로는
# 구분할 수 없고, 내용을 봐야 한다. 실사진 두 장(도브 뷰티 크림바 뒷면 vs
# 상품카드) 비교로 확인한 두 조건을 그대로 쓴다:
#   1) 가격(콤마 형식 판매가 줄)이 아예 없다 - 상품카드엔 반드시 있다.
#   2) 제조/판매/수입하는 기업 표기가 있다 - 뒷면엔 법적 표기로 항상 있고,
#      상품카드엔 나오지 않는다. 다만 이 표기의 용어는 적용 법령(화장품법/
#      식품위생법/생활화학제품법/위생용품 관리법 등)마다 제각각이라(화장품책임
#      판매업자, 식품제조업자, 제조원/판매원, 생활화학제품의 제조자/판매자/
#      수입자, 위생용품의 제조업소/수입업소 등) 개별 단어를 나열하는 대신
#      (제조/판매/수입/유통/공급) + (원/자/처/업자/업체/업소/회사/사) 조합을
#      통째로 잡는다.
# 두 조건이 "그리고"(AND)로 다 맞아야 뒷면으로 판단한다 - Azure가 상품카드의
# 가격 줄만 어쩌다 놓쳐도(이번 세션에서 실제로 있었던 문제), 그 카드엔
# 기업 표기 자체가 없으므로 조건 2에서 걸러져 오분류를 막아준다.
_BACK_LABEL_MANUFACTURER_PATTERN = re.compile(
    r"(?:제조|판매|수입|유통|공급)\s*(?:원|자|처|업자|업체|업소|회사|사)"
    # 수입 완제품 중엔 뒷면이 한글 표기 없이 영문 라벨만 있는 경우도 있다
    # (예: 영국산 클로티드크림 - "Manufacturer:"/"Imported By:"만 있고 한글
    # 법정 표기가 아예 없음). 한글 라벨과 같은 역할을 하는 영문 표기
    # ("Manufactured/Imported/Distributed by", "Manufacturer"/"Importer"/
    # "Distributor")도 같이 인정한다.
    r"|\b(?:manufactured|imported|distributed|packed|packaged)\s+by\b"
    r"|\b(?:manufacturer|importer|distributor)\b",
    re.IGNORECASE,
)


def is_back_label(lines: list) -> bool:
    has_price_line = any(PRICE_LINE_PATTERN.match(l) for l in lines)
    # 뒷면 사진 중엔 실제로 사진에 찍힌 범위가 좁아서(라벨 아래쪽이 프레임
    # 밖으로 잘리는 등) 제조/판매 기업 표기 자체가 통째로 안 찍히는 경우가
    # 있다 - 그런 사진은 조건 2를 못 만족해 앞면으로 오분류된다. "전성분:"
    # (화장품 전 성분표)은 뒷면에서만 나오고 상품카드엔 절대 안 나오는
    # 또 다른 법정 고정 문구라, 기업 표기가 안 잡혀도 이걸로 보조 판정한다.
    has_manufacturer_info = any(
        _BACK_LABEL_MANUFACTURER_PATTERN.search(line) or "전성분" in line
        for line in lines
    )
    return not has_price_line and has_manufacturer_info


def match_card_back_pairs(entries: list) -> dict:
    """상품카드와 그 뒷면 사진을 짝지어 {파일ID: [매칭된 파일ID, ...]}로
    돌려준다. entries의 각 항목은 {"file_id", "uploader", "capture_time",
    "is_back"}. 업로더가 같은(=같은 사람이 찍은) 사진들만 서로 비교하고,
    그 안에서 촬영시각(capture_time, extract_capture_time() 결과) 순으로
    정렬한 뒤 카드 바로 다음에 연달아 나오는 뒷면들을 그 카드에 붙인다 -
    뒷면을 여러 장(정면+측면 등) 찍을 수 있어 "다음 카드가 나오기 전까지"를
    한 묶음으로 본다. 업로더를 모르거나(owners 필드 없음) 촬영시각을
    모르는(EXIF도 파일명도 없음) 사진은 순서를 신뢰할 수 없으므로 애초에
    비교 대상에서 뺀다 - 그 사진 자체는 RAW 시트에 매칭 없이 그대로
    남을 뿐 데이터가 사라지지는 않는다."""
    by_uploader = {}
    for e in entries:
        if not e["uploader"] or e["capture_time"] is None:
            continue
        by_uploader.setdefault(e["uploader"], []).append(e)

    result = {}
    for group in by_uploader.values():
        group.sort(key=lambda e: e["capture_time"])
        pending_card = None
        for e in group:
            if not e["is_back"]:
                pending_card = e
            elif pending_card is not None:
                result.setdefault(pending_card["file_id"], []).append(e["file_id"])
                result.setdefault(e["file_id"], []).append(pending_card["file_id"])
    return result


# ---------------- 뒷면: 소싱형태 / 병입원산지 판별 ----------------
# 소싱형태는 상품카드 제품명과 뒷면 텍스트를 함께 봐야 판단할 수 있다:
#   1. 상품카드 제품명에 PB 브랜드 문구가 있으면 무조건 "PB"
#   2. (아니면) 뒷면의 수입원/수입자/수입업체/수입판매업소가 그 사진을 찍은
#      매장의 계열사(코스트코 <-> 코스트코, 트레이더스(이마트) <-> 이마트)와
#      일치하면 "직수입". 트레이더스 사진에 수입업체가 코스트코 계열이면
#      (또는 그 반대) 그 매장 입장에서는 직수입이 아니므로 여기 해당 안 됨.
#   3. (아니면) 제조사(류)와 수입사(류)가 뒷면에 둘 다 적혀있으면 "벤더구매"
#   4. 위 세 경우에 다 해당하지 않으면 소싱형태는 비우고 병입원산지는 "국내"
#      (수입 경로 자체가 안 적혀있다는 뜻이므로 국내 생산으로 간주)
_PB_NAME_PATTERN = re.compile(
    r"[Tt]\s*[Ss]tandard|티\s*스탠다드|[Kk]irkland|커클랜드", re.IGNORECASE
)

_ENTITY_SUFFIX = r"(?:원|자|처|업자|업체|업소|회사|사)"
# 실사진에서 확인된 "수입" 라벨 표기가 다양하다: "수입업소"(가장 흔함, "판매"
# 없이 바로 업소), "수입판매원"/"수입 판매원"(수입과 접미사 사이에 "판매"가
# 낀 경우 - P&G/LG생활건강 라벨에 흔함), "수입 및 판매업소", "수입판매책임업자",
# 그리고 코스트코 자체 라벨에 흔한 접미사 없는 "수입"(바로 뒤에 "(주)..."나
# ":"가 옴) 등. "수입" 바로 뒤에 접미사가 오는 경우만 인정하면 이런 변형들을
# 다 놓친다. "및"/가운뎃점/"판매"/"책임" 같은 흔한 연결어가 중간에 끼어도
# 인정하되, "공식 수입 업체인" 같은 마케팅 문장(진짜 라벨이 아님)까지 걸리지
# 않도록 연결어 목록은 실제 라벨에서 확인된 것들로만 한정한다.
#
# 접미사 없는 "수입"까지 받아주면서 생긴 새 함정: "반품 및 교환장소: 구입처
# 및 수입판매업소"처럼 식품 표시 고시 문구에 관용적으로 들어가는 문장에도
# "수입판매업소"라는 글자가 그대로 나온다 - 이건 라벨이 아니라 그 갈피에 낀
# 서술어라서, 진짜 "수입" 라벨(대개 그 문장보다 앞쪽에 있음)보다 이 가짜
# 매치가 먼저 잡히면 안 된다. "및" 바로 뒤에 오는 "수입"은 이런 서술 문장의
# 일부일 뿐 필드 라벨이 아니므로 부정형 후방탐색으로 제외한다(실제 라벨
# 변형 중 "수입" 앞에 "및"이 붙는 경우는 없음 - "수입 및 판매업소"처럼 "및"은
# 항상 "수입" 뒤에 온다).
_IMPORTER_LABEL_PATTERN = re.compile(
    rf"(?<!및)(?<!및\s)수입\s*(?:"
    rf"(?:및\s*)?(?:[·・]\s*)?(?:판매\s*)?(?:책임\s*)?(?:{_ENTITY_SUFFIX}|판매업소)"
    rf"|(?=[:：(（])"
    rf")"
    # 화장품류(샴푸/로션/바디워시 등) 뒷면은 "수입"이라는 글자 자체가 아예
    # 없이, 화장품법상 국내 유통 책임 주체를 뜻하는 "(화장품)책임판매업자"
    # 라벨만 있는 경우가 실사진(팬틴/츠바키/헤드앤숄더/비판톨 등)에서 다수
    # 확인됐다. 이 라벨은 사실상 식품 라벨의 "수입원"과 같은 역할(외국
    # 제조사와 국내 유통을 이어주는 주체)을 하므로, 뒷면에 제조원까지 같이
    # 있으면 determine_sourcing()의 "제조사+수입사 둘 다 있으면 벤더구매"
    # 판정에 그대로 활용한다.
    rf"|(?:화장품\s*)?책임\s*판매\s*업자"
    # 한글 라벨이 아예 없이 영문 라벨만 있는 뒷면(수입 완제품에 흔함)을 위한
    # 영문 표기 - "Imported By:"/"Importer:"/"Distributed by"/"Distributor".
    rf"|\bimported\s+by\b|\bimporter\b|\bdistributed\s+by\b|\bdistributor\b",
    re.IGNORECASE,
)
_MANUFACTURER_LABEL_PATTERN = re.compile(
    rf"(?:화장품)?제조\s*{_ENTITY_SUFFIX}"
    rf"|\bmanufactured\s+by\b|\bmanufacturer\b",
    re.IGNORECASE,
)
# 라벨 뒤에 붙은 값을 어디까지 읽을지 정하는 경계 - 다음 항목 라벨(제조/판매/
# 수입/유통/공급 계열, 원산지/제조국)이 나오거나, 줄바꿈/불릿/대괄호가 나오면
# 거기서 값을 끊는다. 같은 줄에 라벨 여러 개가 붙어 나오는 경우(뒷면 라벨은
# 항목이 다닥다닥 붙어 있는 경우가 많다)를 대비한 것.
_FIELD_BOUNDARY_PATTERN = re.compile(
    r"[\[\n•▪]|" + _BACK_LABEL_MANUFACTURER_PATTERN.pattern
    + r"|원산지|제조국|place\s+of\s+origin|country\s+of\s+origin",
    re.IGNORECASE,
)

# "제조국"만 찾으면 "제조국명"(더 흔한 표기)의 "제조국"까지만 매치되고 바로
# 뒤의 "명" 한 글자가 국가명 자리에 캡처되는 오탐이 실제로 있었다("제조국명:
# 베트남" -> "명"). 알파벳 순서상 "제조국명"을 "제조국"보다 먼저 두어(둘 다
# 매치 가능한 위치에서 더 긴 쪽이 우선 시도되도록) 이 라벨도 통째로 먼저
# 소비하게 한다.
_ORIGIN_LABEL_PATTERN = re.compile(
    r"(?:원산지|제조국명|제조국)\s*[\]:：]*\s*([가-힣]{1,10})"
    # 한글 라벨 없이 영문 라벨만 있는 뒷면(수입 완제품에 흔함)을 위한 영문
    # 표기 - "Place of origin:"/"Country of origin:". 값은 한글이 아니라
    # 영문(국가명)이라 별도 그룹으로 잡고, extract_origin()에서
    # _COUNTRY_EN_TO_KO로 한글화한다.
    r"|(?:place\s+of\s+origin|country\s+of\s+origin)\s*[\]:：]*\s*([A-Za-z][A-Za-z .]{1,30})",
    re.IGNORECASE,
)
_MADE_IN_PATTERN = re.compile(r"made\s+in\s+([A-Za-z]+)", re.IGNORECASE)

_COUNTRY_NAMES_KO = (
    "대한민국", "한국", "중국", "일본", "미국", "베트남", "태국", "인도네시아",
    "말레이시아", "필리핀", "인도", "대만", "이탈리아", "프랑스", "독일", "스페인",
    "영국", "스위스", "네덜란드", "벨기에", "폴란드", "캐나다", "멕시코", "브라질",
    "호주", "뉴질랜드", "튀르키예", "스웨덴", "노르웨이", "덴마크", "핀란드",
    "칠레", "아르헨티나", "러시아",
    # 실사진에서 확인된, 기존 목록에 없던 원산지들 - 그리스(비판톨 더마
    # 바디로션), 콜롬비아(아이보리 젠틀바 오리지널향) 등. 유럽/중남미 쪽으로
    # 흔히 나올만한 나라를 몇 개 더 보강했다.
    "그리스", "콜롬비아", "포르투갈", "오스트리아", "아일랜드", "체코",
    "헝가리", "이스라엘", "이집트", "싱가포르", "남아프리카공화국", "페루",
    "에콰도르", "크로아티아",
)
_COUNTRY_EN_TO_KO = {
    "ITALY": "이탈리아", "CHINA": "중국", "USA": "미국", "AMERICA": "미국",
    "VIETNAM": "베트남", "THAILAND": "태국", "FRANCE": "프랑스", "GERMANY": "독일",
    "SPAIN": "스페인", "JAPAN": "일본", "KOREA": "대한민국", "INDONESIA": "인도네시아",
    "MALAYSIA": "말레이시아", "PHILIPPINES": "필리핀", "TAIWAN": "대만",
    "SWITZERLAND": "스위스", "NETHERLANDS": "네덜란드", "BELGIUM": "벨기에",
    "POLAND": "폴란드", "CANADA": "캐나다", "MEXICO": "멕시코", "BRAZIL": "브라질",
    "AUSTRALIA": "호주", "TURKEY": "튀르키예", "SWEDEN": "스웨덴", "NORWAY": "노르웨이",
    "DENMARK": "덴마크", "FINLAND": "핀란드", "INDIA": "인도", "UK": "영국",
    "ENGLAND": "영국", "GREECE": "그리스", "COLOMBIA": "콜롬비아",
    "PORTUGAL": "포르투갈", "AUSTRIA": "오스트리아", "IRELAND": "아일랜드",
    "ISRAEL": "이스라엘", "EGYPT": "이집트", "SINGAPORE": "싱가포르",
    "PERU": "페루", "ECUADOR": "에콰도르", "CROATIA": "크로아티아",
    # 한글 라벨 없이 영문 라벨만 있는 뒷면에서 나라 이름이 축약형("UK") 대신
    # 정식 명칭 그대로 적히는 경우("United Kingdom" 등)를 위한 보강.
    "UNITED KINGDOM": "영국", "UNITED STATES": "미국",
    "UNITED STATES OF AMERICA": "미국", "NEW ZEALAND": "뉴질랜드",
    "SOUTH KOREA": "대한민국", "REPUBLIC OF KOREA": "대한민국",
}

_COSTCO_CHAIN_KEYWORDS = ("코스트코",)
_EMART_CHAIN_KEYWORDS = ("이마트", "트레이더스")
# 사진이 어느 매장(retailer 파라미터, "costco"/"traders")에서 왔는지를
# 실제 유통 계열사로 매핑한다 - 트레이더스는 이마트 계열이라 "직수입" 판정 시
# 뒷면 수입업체가 "이마트"로 적혀있어야 그 매장 입장에서 직수입인 것이다.
_RETAILER_CHAIN = {"costco": "costco", "traders": "emart"}


def _chain_of(name: str):
    if any(k in name for k in _COSTCO_CHAIN_KEYWORDS):
        return "costco"
    if any(k in name for k in _EMART_CHAIN_KEYWORDS):
        return "emart"
    return None


def _extract_after_label(text: str, label_pattern: "re.Pattern"):
    """text 안에서 label_pattern이 처음 매치되는 지점 바로 뒤의 값을
    잘라 돌려준다. 라벨 자체가 없으면 None(=그 항목이 아예 안 적혀있음),
    라벨은 있는데 값이 비어있으면 빈 문자열을 돌려준다."""
    m = label_pattern.search(text)
    if not m:
        return None
    rest = re.sub(r"^\s*[\]:：]*\s*", "", text[m.end():])
    stop = _FIELD_BOUNDARY_PATTERN.search(rest)
    value = rest[:stop.start()] if stop else rest[:80]
    return value.strip(" .,:;·-")


def parse_sourcing_fields(text: str) -> dict:
    importer = _extract_after_label(text, _IMPORTER_LABEL_PATTERN)
    manufacturer = _extract_after_label(text, _MANUFACTURER_LABEL_PATTERN)
    return {
        "importer_value": importer or "",
        "has_importer": importer is not None,
        "has_manufacturer": manufacturer is not None,
    }


def _country_in(text: str) -> str:
    """text 안에서 알려진 국가명(한글 또는 영문)을 찾아 한글로 돌려준다.
    여러 개 있으면 가장 뒤에 나오는 것을 우선한다 - 주소는 보통 끝에
    국가명을 적으므로("...신시내티, 오하이오주, 미국"), 앞쪽에 우연히 섞인
    지명/사명보다 뒤쪽 값이 실제 원산지일 가능성이 높다. 하나도 없으면
    빈 문자열.

    "미국산"/"캐나다산"처럼 국가명 바로 뒤에 "산"이 붙은 표기는 원재료
    하나하나의 개별 원산지 표시(식품표시 관행상 "원재료명(OO산)" 형태로
    나열됨)일 뿐, 상품 전체의 병입원산지가 아니다. 실사진(그래놀라 뒷면 -
    "롤드오트(캐나다산), 볶음해바라기씨(미국산)"이 있는데 정작 제조원은
    국내(충북 제천)였던 사례)으로 확인된 오탐이라 이런 표기는 후보에서
    제외한다."""
    candidates = []
    for c in _COUNTRY_NAMES_KO:
        pos = text.rfind(c)
        if pos != -1 and text[pos + len(c):pos + len(c) + 1] != "산":
            candidates.append((pos, c))
    for en, ko in _COUNTRY_EN_TO_KO.items():
        for em in re.finditer(rf"\b{en}\b", text, re.IGNORECASE):
            candidates.append((em.start(), ko))
    if not candidates:
        return ""
    candidates.sort()
    return candidates[-1][1]


def extract_origin(text: str) -> str:
    """뒷면 텍스트에서 병입원산지(제조국)를 뽑는다. 우선순위: ① "원산지:"/
    "제조국:"/"Place of origin:" 같은 명시적 라벨(값이 한글 또는 영문) -> ②
    제조자/제조원 라벨 바로 뒤 주소에서 국가명을 찾음(라벨은 있는데 값이 영문
    회사명+주소로만 끝나는 경우가 실사진에서 많이 확인됨 - "Colgate Sanxiao
    co., Ltd... China.", "Johnson & Johnson (Thailand) Limited...BANGKOK,
    THAILAND") -> ③ "Made in X" -> ④ 그 외엔 텍스트 전체에서 국가명을 찾는다.
    아무 것도 못 찾으면 빈 문자열(수기 확인 필요)을 돌려준다."""
    m = _ORIGIN_LABEL_PATTERN.search(text)
    if m:
        # 그룹1은 한글 라벨("원산지:"/"제조국:")의 값, 그룹2는 영문 라벨
        # ("Place of origin:")의 값이다 - 한 번에 매치되는 쪽은 항상 하나뿐.
        value = m.group(1) or m.group(2)
        if value in ("대한민국", "한국"):
            return "국내"
        if m.group(2):
            # 영문 값은 알려진 국가명 사전으로 한글화한다 - 모르는 영문
            # 국가명이면(사전에 없는 나라) 값을 신뢰하지 않고 아래 다른
            # 방식으로 계속 찾는다(한글 값과 달리 임의의 영문 문자열을
            # 그대로 "원산지"로 쓰면 회사명 등이 섞여 들어올 위험이 크다).
            mapped = _COUNTRY_EN_TO_KO.get(value.strip().upper())
            if mapped:
                return "국내" if mapped == "대한민국" else mapped
        # 실제 국가명은 항상 2글자 이상이다 - 1글자만 캡처됐으면(줄바꿈 등으로
        # 값이 중간에 잘린 경우, 예: "[원산지] 태"만 잡히고 "국"이 다음
        # 줄로 넘어간 경우) 이 라벨 매치를 신뢰하지 않고 아래 다른 방식으로
        # 계속 찾는다. _COUNTRY_NAMES_KO 목록에 없는 나라(예: 콜롬비아)도
        # 실제 라벨 값이면 그대로 믿는다 - 목록은 라벨이 아예 없을 때 쓰는
        # 폴백 전용이라, 여기서 목록에 없다고 버리면 라벨이 명확히 있는
        # 값을 오히려 못 믿게 되는 역효과가 난다.
        elif len(value) >= 2:
            return value

    # "원산지"/"제조국" 라벨 자체가 없거나 값이 한글이 아니어서(예: "[국외
    # 제조업소 및 원산지] Colgate Sanxiao co., Ltd... China.") 위에서 못
    # 찾았으면, 제조자/제조원 라벨 바로 뒤 주소(보통 다음 한두 줄)에서
    # 국가명을 찾는다. 다른 라벨(수입자/판매자 등)이 이어지면 거기서 끊어서
    # 엉뚱한 뒤쪽 항목의 국가명을 잘못 줍지 않게 한다.
    mfr_match = _MANUFACTURER_LABEL_PATTERN.search(text)
    if mfr_match:
        window = text[mfr_match.end():mfr_match.end() + 200]
        boundary = re.search(r"[\[•▪]|" + _BACK_LABEL_MANUFACTURER_PATTERN.pattern, window)
        if boundary:
            window = window[:boundary.start()]
        found = _country_in(window)
        if found:
            return "국내" if found in ("대한민국", "한국") else found

    m = _MADE_IN_PATTERN.search(text)
    if m:
        mapped = _COUNTRY_EN_TO_KO.get(m.group(1).upper())
        if mapped:
            return "국내" if mapped == "대한민국" else mapped

    found = _country_in(text)
    if found:
        return "국내" if found in ("대한민국", "한국") else found

    return ""


def determine_sourcing(product_name: str, retailer: str, back_text: str, front_text: str = "") -> tuple:
    """(소싱형태, 병입원산지)를 함께 돌려준다. product_name은 상품카드에서
    읽은 제품명(뒷면이 아님), back_text는 매칭된 뒷면 사진(들)의 원문텍스트,
    front_text는 상품카드 자체의 원문텍스트("직수입" 인증 문구 판별용)."""
    origin = extract_origin(back_text)

    if _PB_NAME_PATTERN.search(product_name or ""):
        return "PB", origin

    # 코스트코 자체 가격표에 "-직수입 오리지널 제품"처럼 "직수입" 문구가
    # 불릿으로 직접 찍혀 있는 경우가 실사진(도리토스 나초칩)에서 확인됐다 -
    # 이건 뒷면의 제조사/수입사 라벨을 해석해서 추론하는 게 아니라 코스트코가
    # 스스로 매긴 소싱 인증이라, 뒷면 사진 매칭 여부와 무관하게 그대로
    # 신뢰한다(뒷면이 아예 없는 카드도 이 값만으로 채워질 수 있다 -
    # sync_back_sourcing 참고).
    if "직수입" in (front_text or ""):
        return "직수입", origin

    fields = parse_sourcing_fields(back_text)
    importer_chain = _chain_of(fields["importer_value"])
    if importer_chain and importer_chain == _RETAILER_CHAIN.get(retailer):
        return "직수입", origin
    if fields["has_manufacturer"] and fields["has_importer"]:
        return "벤더구매", origin

    # 수입 경로 자체가 안 적혀있으면 소싱형태는 비운다. 병입원산지는 그래도
    # "원산지:"/"제조국:"/"Made in" 같은 명시적 표기가 있으면 그 값을 우선
    # 쓰고("국내"로 덮어써 원산지 표기와 모순되는 값을 만들지 않는다),
    # 그런 표기가 전혀 없을 때만 국내산으로 간주해 "국내"를 채운다.
    return "", (origin or "국내")


# 코드 다음에 오는 진짜 브랜드명 줄("ARLA", "RICOLA" 등)과, 배경의 다른
# 상품 포장에서 새어 들어온 뜻 없는 대문자 낱말 파편("HEE", "CON" 등)을
# 구분할 만한 확실한 기준은 없지만, 후자는 실사진에서 항상 2글자~8글자의
# 짧은 단독 대문자 낱말이었다 - _parse_fields_from_lines()에서 이 낱말이
# 연달아 여러 줄 나오는지로 노이즈 여부를 판단한다.
_SHORT_CAPS_WORD_RE = re.compile(r"^[A-Z]{2,8}$")


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

    실사진에서 확인된 함정: 진짜 카드 뒤쪽(제품 포장 자체의 장식 문구 등)에
    우연히 숫자 4~8자리처럼 보이는 잡음이 하나 더 있으면, 그 잡음이 "다음
    코드"로 오인되어 진짜 카드의 구간을 정가 줄 도달 전에 끊어버린다. 실제
    카드는 항상 "코드 -> ... -> 단가 -> 정가" 순서이므로, 구간 안에 "단가"
    문구가 이미 나왔는데 아직 정가(콤마 가격 줄)를 못 찾았다면 다음 코드
    후보 하나쯤은 진짜 코드가 아니라 잡음일 가능성이 높다고 보고 구간을
    그 다음 경계까지 넓힌다 - 그래야 뒤에 이어지는 진짜 정가를 놓치지 않는다.
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    code_indices = [
        i for i, l in enumerate(lines)
        if _normalize_bare_code(l, allow_five_digit=(i == 0))
    ]

    if len(code_indices) <= 1:
        return _parse_fields_from_lines(lines)

    best_fields, best_score = None, None
    for idx, code_i in enumerate(code_indices):
        end_idx = idx + 1
        end = code_indices[end_idx] if end_idx < len(code_indices) else len(lines)
        while (
            end_idx < len(code_indices)
            and any("단가" in lines[k] for k in range(code_i, end))
            and not any(PRICE_LINE_PATTERN.match(lines[k]) for k in range(code_i, end))
        ):
            end_idx += 1
            end = code_indices[end_idx] if end_idx < len(code_indices) else len(lines)
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
        normalized_code = _normalize_bare_code(line, allow_five_digit=(i == 0))
        if normalized_code:
            result["상품코드"] = normalized_code
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
            weight_value = line
            # "170G원산지영국"처럼 원산지가 중량 줄 끝에 그대로 붙어 나오는
            # 경우가 실사진에서 확인됐다 - 원산지는 아래에서 따로 뽑으므로,
            # 중량 값엔 그 앞부분("170G")만 남긴다.
            origin_in_weight = _ORIGIN_LABEL_PATTERN.search(line)
            if origin_in_weight:
                weight_value = line[:origin_in_weight.start()].strip()
            result["중량"] = weight_value
            weight_idx = i
            break

    # 뒷면 사진이 매칭되지 않아도, 상품카드 자체에 "원산지:"/"원산지OO"가 직접
    # 찍혀 있는 경우가 실사진에서 확인됐다("170G원산지영국"처럼 중량 줄
    # 끝에 붙거나, "원산지 : 영국"처럼 셀링포인트 자리에 별도 줄로 나옴).
    # 이럴 땐 뒷면 매칭을 기다릴 필요 없이 이 값을 바로 병입원산지로 채택한다
    # (determine_sourcing()의 뒷면 기반 병입원산지 추정과 별개 경로 - 이미
    # 채워진 칸은 sync_back_sourcing()이 덮어쓰지 않으므로 서로 충돌하지
    # 않는다). extract_selling_points()도 이 줄을 셀링포인트로 잘못 삼키지
    # 않도록 별도로 건너뛴다(_is_origin_label_line 참고).
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        m = _ORIGIN_LABEL_PATTERN.search(line)
        if not m:
            continue
        raw_value = (m.group(1) or m.group(2) or "").strip()
        mapped = _COUNTRY_EN_TO_KO.get(raw_value.upper()) if m.group(2) else raw_value
        if mapped and len(mapped) >= 2:
            result["병입원산지"] = "국내" if mapped in ("대한민국", "한국") else mapped
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

    # "할인행사(2026/06/29 ~2026/07/12)"처럼 할인 기간을 알리는 줄이 영문명보다
    # 먼저 나오는 경우가 있다. 보통은 영문명이 먼저 경계를 잡아주지만, OCR이
    # 영문명을 온전히 못 읽으면("GQ LAB CHOLESTEROL"의 "GQ LAB"이 통째로
    # 누락되어 "CHOLESTEROL" 한 단어만 남는 등, 영문명 조건의 "2단어 이상"을
    # 못 채움) english_idx가 안 잡혀서 이 할인 기간 문구까지 한글 제품명에
    # 그대로 딸려 들어간다. 그래서 영문명과 무관하게 독립적인 경계로 둔다.
    discount_event_idx = None
    for i, line in enumerate(lines):
        if code_idx is not None and i <= code_idx:
            continue
        if "할인행사" in line or "행사기간" in line or re.search(r"\d{4}/\d{2}/\d{2}", line):
            discount_event_idx = i
            break

    english_idx = None
    stray_caps_idx = None  # 영문명 조건은 다 못 채우지만, 여전히 한글 제품명의
    # 일부는 아닐 가능성이 높은 대문자 단독 단어("CHOLESTEROL" 등)의 위치.
    # 영문명으로 채택하진 않지만 한글 제품명 경계로는 쓴다 - 아래 참고.
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
            re.fullmatch(r"[A-Z0-9 .,'&×\-]{4,}", line)
            and re.search(r"[A-Z]{2,}", line)
            and len(line.split()) >= 2
        ):
            english_idx = i
            result["제품명(영어)"] = line
            break
        # "GQ LAB CHOLESTEROL"의 "GQ LAB"이 OCR에서 통째로 누락되고
        # "CHOLESTEROL" 한 단어만 남는 경우처럼, 영문명 전체를 못 읽었지만
        # 그 잔여 단어 자체는 확실히 영문명 조각이다. 영문명으로 채택할 만큼
        # 확실하진 않아 result["제품명(영어)"]엔 안 넣지만, 한글 제품명이 이
        # 단어까지 삼켜버리는 건 막아야 하므로 경계 후보로는 기록해둔다.
        if stray_caps_idx is None and re.fullmatch(r"[A-Z]{4,}", line):
            stray_caps_idx = i

    # 한글 제품명은 상품코드 다음 줄부터(코드를 아예 못 찾았으면 맨 처음부터 -
    # 예: 바코드 구역이 프레임에 없는 화면 캡처), 중량/영문명/단가/가격 중 가장
    # 먼저 나오는 줄 전까지로 본다 (넷 다 없으면 끝까지). 식품은 보통 중량이
    # 경계가 되고, 비식품은 중량이 없으니 영문명이 바로 경계가 된다.
    # 이 구간에는 "RICOLA"처럼 한글이 아닌 브랜드명 줄이 섞여 있을 수 있는데,
    # 실제로는 한글 제품명의 일부이므로("RICOLA 레몬민트 허브캔디") 한글 포함
    # 여부로 거르지 않고 구간 안의 모든 줄을 그대로 합친다.
    boundary_candidates = [
        i for i in (
            weight_idx, english_idx, danga_idx, price_boundary_idx,
            discount_event_idx, stray_caps_idx,
        )
        if i is not None
    ]
    start = code_idx + 1 if code_idx is not None else 0
    end = min(boundary_candidates) if boundary_candidates else len(lines)
    korean_lines = lines[start:end]

    # 무게/영문명 경계보다 먼저 "원산지 : 노르웨이산"/"돈육원산지 : 미국"처럼
    # 원산지 표기가 한글 제품명 구간 안쪽에 끼는 카드가 있다(수산/청과/축산
    # 실사진에서 확인됨 - 식품/비식품은 보통 원산지 표기가 이 구간보다 뒤,
    # 셀링포인트 자리에 나와서 지금까지는 안 걸렸다). 이 값은 위에서 이미
    # 병입원산지로 따로 뽑았으므로, 제품명에 그대로 안 섞이게 건너뛴다.
    korean_lines = [l for l in korean_lines if not _is_origin_label_line(l)]

    # 사진에 다른 상품(배경 진열대의 다른 포장 박스 등)이 같이 찍히면, 그
    # 포장에 적힌 작은 글자 파편이 "HEE"/"CON"처럼 뜻 없는 짧은 대문자 낱말
    # 줄로 OCR되어 코드 바로 다음, 진짜 브랜드명 줄(예: "ARLA") 앞에 끼어드는
    # 경우가 실사진에서 확인됐다("HEE"/"CON"/"ARLA" 세 줄이 연달아 나오고
    # 그 뒤에야 한글 제품명이 시작됨 - 진짜 브랜드는 한글 바로 앞의 "ARLA"
    # 뿐이고 앞 두 줄은 배경 소음). 위 클러스터링(_cluster_lines_by_layout)이
    # 대부분의 배경 텍스트를 걸러내지만, 이렇게 실제 카드와 아주 가까운 위치에
    # 있는 파편 몇 줄은 걸러지지 않고 새어 들어올 수 있다. 진짜 브랜드명 줄은
    # 이 자리에 보통 하나만 오므로("RICOLA 레몬민트 허브캔디"의 "RICOLA"처럼),
    # 한글 제품명이 시작되기 전에 이런 짧은 대문자 낱말 줄이 2개 이상 연달아
    # 나오면 노이즈로 보고 한글 바로 앞의 마지막 한 줄만 브랜드명으로 남긴다.
    # 그런 줄이 하나뿐이면(가장 흔한 정상 케이스) 원래대로 손대지 않는다.
    leading_caps_run = []
    for line in korean_lines:
        if _SHORT_CAPS_WORD_RE.match(line):
            leading_caps_run.append(line)
        else:
            break
    if len(leading_caps_run) >= 2:
        korean_lines = korean_lines[len(leading_caps_run) - 1:]

    joined_name = " ".join(korean_lines).strip()
    # 실사진에서 Azure가 카드의 제품명 구역을 통째로 못 읽고 "Global
    # Product"/가격/바코드 같은 아래쪽 정형 문구만 반환하는 경우가 확인됐다.
    # 이 구간에 한글이 하나도 없으면 진짜 제품명을 못 읽은 것이므로, 엉뚱한
    # 정형 문구를 제품명으로 채택하지 않고 빈 채로 남긴다.
    if re.search(r"[가-힣]", joined_name):
        result["제품명(한국어)"] = joined_name

    # "단가 / 10G"처럼 기준 단위가 함께 찍혀있으므로, 가격만 뽑으면 몇 g당 가격인지
    # 알 수 없다. "217원" 대신 "217원/10g" 형태로 단위까지 같이 기록한다
    # (기준 단위는 상품마다 다르므로 사진에서 그대로 읽어와야 정확하다).
    # 단위가 "10g"처럼 숫자+무게단위가 아니라 "장"/"개"처럼 개수 단위 하나만
    # 나오는 경우도 있다("단가 / 장", "단가 / 개") - "/" 뒤에 오는 토큰을
    # 그대로 단위로 쓰면 둘 다 커버된다.
    danga_price = ""
    danga_price_line_idx = None  # 가격 탐색에서 이 줄은 다시 쓰지 않도록 인덱스로 기억
    # 축산 카드는 "KG당단가"라는 고정 문구(실제 값 없음)가 진짜 단가 구간
    # ("단가 / 100G" + 가격)보다 먼저 나온다. danga_idx(위, "단가"가 처음
    # 나오는 위치 - 한글 제품명 경계로는 이 위치가 맞아서 그대로 둔다)를
    # 그대로 쓰면 "KG당단가" 줄부터 3줄 안에서 가격을 찾다가 실패한다.
    # "단가"가 포함된 줄이 여러 개면 순서대로 시도해서 실제로 가격이 있는
    # 구간을 찾는다 - "단가"가 한 번만 나오는 기존 카드는 첫 시도에서 바로
    # 끝나므로(아래 for문이 한 번만 돌고 break) 결과가 예전과 동일하다.
    for candidate_idx in (i for i, line in enumerate(lines) if "단가" in line):
        # OCR이 "/" 구분자를 모양이 비슷한 다른 문자로 잘못 읽는 경우가 실제로
        # 있다 - 세로줄 모양의 "ㅣ"(한글 자모, "단가 ㅣ 개")와 "!"(느낌표,
        # "단가 ! 장") 둘 다 확인됐다. 셋 다 받아준다.
        unit_match = re.search(r"단가\s*[/ㅣ!]\s*(\S+)", lines[candidate_idx])
        unit = unit_match.group(1) if unit_match else ""
        for offset, line in enumerate(lines[candidate_idx:candidate_idx + 3]):
            m = re.search(r"[\d,]{2,}\s*원", line)
            if m:
                danga_price = m.group(0)
                danga_price_line_idx = candidate_idx + offset
                break
        if danga_price:
            result["단가"] = f"{danga_price}/{unit}" if unit else danga_price
            break
    if not result["단가"]:
        # "단가"라는 글자 자체가 없어도 "100g당 1,211원"처럼 단위당 가격이
        # 그대로 찍혀있는 경우가 있다.
        unit_price_match = UNIT_PRICE_PATTERN.search("\n".join(lines))
        if unit_price_match:
            unit = unit_price_match.group(1).replace(" ", "")
            result["단가"] = f"{unit_price_match.group(2)}원/{unit}"
    if not result["단가"]:
        # "단가"라는 글자도 "~당~원" 문구도 없이 "392원/개"처럼 통째로 한 줄에
        # 찍히는 경우도 있다.
        result["단가"] = _find_standalone_unit_price(lines)

    # 가격은 "12,990원"처럼 천단위 콤마가 있는 큰 금액이다. Azure OCR이 "원" 글자를
    # 가끔 다른 문자(예: "z")로 잘못 읽는 경우가 있어("7,990z"), "원" 글자 자체보다
    # 콤마 포함 숫자 형태를 우선 신뢰한다. 단가는 보통 콤마 없는 2~3자리 숫자라서
    # ("217원", "89원") 이 방식으로도 서로 헷갈리지 않는다.
    # 할인이 있는 카드는 정가/할인가 둘 다 콤마 형식 숫자로 찍히는데, 할인은 항상
    # 가격을 낮추므로 정가가 항상 더 큰 값이다 - 그래서 "몇 번째 줄에 있냐"가
    # 아니라 "값이 더 크냐"로 정가를 고른다. 예전에는 "할인 표시 줄 앞쪽에서만
    # 찾는다"는 위치 기반 규칙을 썼는데, 할인가 숫자는 보통 더 크게 강조
    # 인쇄되어 있어서(바운딩박스가 커서) OCR 줄 순서 자체가 할인 표시 문구보다
    # 앞으로 밀려버리면 이 규칙이 무력화된다 - 값 기준으로 판단하면 줄 순서가
    # 어떻게 뒤섞여도 영향받지 않는다. 단가로 이미 쓴 줄(danga_price_line_idx)은
    # 후보에서 제외한다(같은 값이 단가/가격 두 칸에 중복으로 들어가는 것 방지).
    def find_price(candidate_lines, exclude_idx=None):
        prices = []
        for i, line in enumerate(candidate_lines):
            if i == exclude_idx:
                continue
            m = PRICE_LINE_PATTERN.match(line)
            if m:
                prices.append(m.group(1))
        if not prices:
            return ""
        return _select_regular_price(prices, _find_discount_amount(candidate_lines))

    result["가격"] = find_price(lines, exclude_idx=danga_price_line_idx)

    if not result["가격"]:
        # 콤마 없는 가격도 드물게 있을 수 있으니, "원" 글자가 정상적으로 인식된
        # 나머지 금액 중에서 값이 제일 큰 것을 찾는 것으로 대비한다. "100g당
        # 899원"처럼 "단가" 키워드 없이 단위당가격만 찍힌 경우, 그 안의
        # "899원" 조각까지 후보로 잡히면 진짜 가격을 못 찾았을 때 이 단가
        # 조각을 엉뚱하게 채택해버리므로 먼저 지운다.
        text_without_unit_price = UNIT_PRICE_PATTERN.sub("", "\n".join(lines))
        all_prices = [m.group(0) for m in re.finditer(r"[\d,.]{2,}\s*원", text_without_unit_price)]
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
    # 무게 단위가 없는 건강기능식품류("...80포", "...180정")도 마찬가지로 보정한다.
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_count = _split_trailing_count(result["제품명(한국어)"])
        if trailing_count:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_count
    # 무게도 개수 단위도 아니라 "부품명+숫자" 구성품 목록으로 규격을 나타내는
    # 카드("기*2+날*14", "면도기1+날8")도 마찬가지로 보정한다.
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_combo = _split_trailing_component_combo(result["제품명(한국어)"])
        if trailing_combo:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_combo
    # 세트 상품은 제품명 자체엔 규격이 안 붙고 "- 구성: 치약 160g*2+60g*1+20g*2"
    # 처럼 별도 셀링포인트 줄에 규격이 나오기도 한다. 위 세 방법 다 제품명
    # 문자열 끝에서만 찾으므로 이 경우를 못 잡는다 - 마지막으로 "구성:" 문구가
    # 있는 줄을 뒤진다.
    if not result["중량"]:
        result["중량"] = _find_composition_spec(lines)

    return result


# ---------------- 트레이더스 가격표 파싱 ----------------
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

    # 맨 첫 줄을 무조건 제품명으로 신뢰하면 안 된다 - "TRADERS"(진열대 로고),
    # "MONTI"(옆 상품 박스), "E-MART"(매장 로고)처럼 사진 위쪽에 찍힌 배경
    # 텍스트가 실제 상품카드보다 먼저 인식되는 경우가 있다. 진짜 제품명 줄은
    # 브랜드가 영어로 앞에 붙어도("RICOLA 레몬민트 허브캔디") 항상 한글이 같이
    # 있으므로, 한글이 하나도 없는 순수 영어 줄(로고류)은 건너뛰고 한글이
    # 포함된 첫 줄을 제품명으로 삼는다. 실사진에서 Azure가 카드 위쪽(제품명
    # 구역)을 통째로 못 읽고 "Global Product"/가격/바코드 같은 아래쪽 정형
    # 문구만 반환하는 경우가 확인됐다 - 이럴 땐 한글 줄이 단 하나도 없으므로,
    # 예전처럼 0번째 줄을 억지로 제품명으로 쓰면 "Global Product"나 "11,400"
    # 같은 걸 제품명으로 잘못 채택하게 된다. 한글 줄이 하나도 없으면 제품명은
    # 그냥 빈 채로 남긴다(실제로 못 읽은 것이므로).
    # 첫 한글 줄이라고 다 제품명은 아니다 - "√ 탁월한 세척력*"/"- 스머커스
    # 딸기, 오렌지, 블루베리맛..." 같은 불릿 셀링포인트나 "신세계포인트" 같은
    # 정형 프로모션 문구가 진짜 제품명 줄보다 먼저 나오는 경우가 실사진에서
    # 확인됐다(트레이더스는 카드 위쪽에 셀링/프로모션 문구가 먼저 오는 레이아웃이
    # 있음). 불릿 문자로 시작하거나 알려진 프로모션 문구인 한글 줄은 건너뛰고,
    # 그 다음 한글 줄을 제품명으로 삼는다.
    # 배경의 다른 상품 가격표에서 새어 들어온 문장 조각("...보상 받을 수
    # 있습니다.", "...깨끗이 씻어내 줍니다." 같은 소비자분쟁해결기준/사용법
    # 문구의 뒷부분만 찍힘)이 한글을 포함하고 불릿도 아니라서 위 두 조건을
    # 통과해 제품명으로 잘못 채택되는 경우가 실사진(네이처 그래놀라)에서
    # 확인됐다. 진짜 제품명은 명사(구)라 이런 종결어미로 끝나는 일이 없으므로,
    # "-습니다"/"-니다"로 끝나는 줄은 문장 조각으로 보고 제외한다.
    _SENTENCE_ENDING_RE = re.compile(r"(?:습니다|니다)\.?$")

    def _is_traders_name_candidate(line: str) -> bool:
        if not re.search(r"[가-힣]", line):
            return False
        if line[:1] in "-–—−•·▶*√":
            return False
        if _SENTENCE_ENDING_RE.search(line):
            return False
        return not any(sub in line for sub in _SELLING_POINT_EXCLUDE_SUBSTRINGS)

    korean_name_idx = next((i for i, l in enumerate(lines) if _is_traders_name_candidate(l)), None)
    if korean_name_idx is not None:
        result["제품명(한국어)"] = lines[korean_name_idx]

    idx = (korean_name_idx + 1) if korean_name_idx is not None else 0
    if (
        idx < len(lines)
        and re.fullmatch(r"[A-Z0-9 .,'&×\-]{4,}", lines[idx])
        and re.search(r"[A-Z]{2,}", lines[idx])
    ):
        result["제품명(영어)"] = lines[idx]
        idx += 1

    danga_match = UNIT_PRICE_PATTERN.search(text)
    if danga_match:
        unit = danga_match.group(1).replace(" ", "")
        result["단가"] = f"{danga_match.group(2)}원/{unit}"
    if not result["단가"]:
        result["단가"] = _find_standalone_unit_price(lines)

    code_idx = None
    for i, line in enumerate(lines):
        if TRADERS_CODE_PATTERN.fullmatch(line):
            result["상품코드"] = line
            code_idx = i
            break

    # 가격: 콤마 형식(예: "15,380")의 독립된 줄을 카드 전체에서 찾는다. 큰 판매가
    # 숫자는 폰트가 커서 바운딩박스 top-Y가 다른 줄보다 위로 잡히는 경우가 많아,
    # 인쇄상으로는 아래 있어도 OCR 줄 순서에서는 훨씬 먼저 나올 수 있다 - 특정
    # 위치(코드 다음, 할인 표시 앞 등)를 기준으로 찾으면 이런 카드에서 계속
    # 깨진다. 할인이 있는 카드는 정가/할인가 둘 다 콤마 형식으로 찍히는데, 할인은
    # 항상 가격을 낮추므로 정가가 항상 더 큰 값이다 - 그래서 "몇 번째 줄에
    # 있냐"가 아니라 "값이 더 크냐"로 정가를 고른다. 이러면 줄 순서가 어떻게
    # 뒤섞여도(할인가가 할인 표시 문구보다 먼저 인식되어도) 영향받지 않는다.
    # 단가는 "10g당 154원"처럼 콤마 없는 숫자라 PRICE_LINE_PATTERN(콤마 필수)에
    # 애초에 안 걸리므로 카드 전체에서 찾아도 단가와 헷갈릴 일은 없다.
    def find_price(candidate_lines):
        prices = [m.group(1) for m in (PRICE_LINE_PATTERN.match(l) for l in candidate_lines) if m]
        if not prices:
            return ""
        return _select_regular_price(prices, _find_discount_amount(candidate_lines))

    result["가격"] = find_price(lines)

    if not result["가격"]:
        # "100g당 428원"처럼 단위당가격 표현 안에 있는 "428원"까지 콤마 없는
        # 가격 후보로 잡히면, 진짜 판매가를 못 찾았을 때 이 단가 조각을
        # 엉뚱하게 상품 가격으로 잘못 채택해버린다(실사진에서 확인됨 - 가격표
        # 뒷부분을 Azure가 통째로 못 읽은 카드에서 "100g당 428원"의 428원이
        # 가격 칸에 들어감). 단위당가격 문구는 먼저 지우고 나서 찾는다.
        text_without_unit_price = UNIT_PRICE_PATTERN.sub("", text)
        all_prices = [m.group(0) for m in re.finditer(r"[\d,.]{2,}\s*원", text_without_unit_price)]
        if all_prices:
            result["가격"] = max(all_prices, key=lambda p: int(re.sub(r"[^\d]", "", p) or "0"))

    # 트레이더스 파서는 중량을 따로 찾는 로직이 없다 - 제품명 끝에 붙어 나온
    # 경우("...1.5kg")만이라도 분리해서 채운다.
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_weight = _split_trailing_weight(result["제품명(한국어)"])
        if trailing_weight:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_weight
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_count = _split_trailing_count(result["제품명(한국어)"])
        if trailing_count:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_count
    if not result["중량"] and result["제품명(한국어)"]:
        cleaned_name, trailing_combo = _split_trailing_component_combo(result["제품명(한국어)"])
        if trailing_combo:
            result["제품명(한국어)"] = cleaned_name
            result["중량"] = trailing_combo
    if not result["중량"]:
        result["중량"] = _find_composition_spec(lines)

    # 뒷면 사진 매칭 없이도 카드 자체에 "원산지:"가 찍혀 있으면 바로
    # 병입원산지로 채택한다 - _parse_fields_from_lines()(코스트코)와 같은
    # 근거(README 7절 참고).
    m = _ORIGIN_LABEL_PATTERN.search(text)
    if m:
        raw_value = (m.group(1) or m.group(2) or "").strip()
        mapped = _COUNTRY_EN_TO_KO.get(raw_value.upper()) if m.group(2) else raw_value
        if mapped and len(mapped) >= 2:
            result["병입원산지"] = "국내" if mapped in ("대한민국", "한국") else mapped

    return result


# ---------------- 제품군정리 시트 ----------------
# 제품군 분류는 두 단계다: 1순위는 아래 match_category_from_catalog() - 자사
# 카탈로그(MC_CATALOG_SPREADSHEET_ID)에 실제로 있는 상품과 겹치는지 확인하는,
# 훨씬 정확하고 유지보수가 필요 없는 방식. 카탈로그에 없거나 카탈로그 설정
# 자체가 안 돼있을 때만 2순위로 아래 CATEGORY_KEYWORDS(수동 목록)를 쓴다.
# CATEGORY_KEYWORDS: 제품명(한국어)에 이 키(왼쪽)가 들어있으면 매핑된
# 제품군(오른쪽)으로 분류한다. 대부분은 키와 제품군 이름이 똑같지만
# ("올리브유"->"올리브유"), "하리보 골드베렌"처럼 제품명에 제품군을 나타내는
# 글자가 아예 없는 경우를 위해 브랜드/제품 종류 이름 -> 실제 제품군으로
# 매핑해두기도 한다("골드베렌"->"구미젤리") - 다만 이런 브랜드 항목들은
# 대부분 카탈로그에 이미 있어서(예: 하리보 실제로 "젤리,푸딩"으로 카탈로그가
# 잡아줌) 카탈로그가 설정돼있으면 거의 안 쓰일 것이다. 아직 마주치지 못한
# 제품군은 자동으로 UNCATEGORIZED_LABEL로 들어가서 데이터가 유실되진 않지만,
# 실제로 찍히는 상품 종류를 봐가며 이 목록을 계속 채워나가야 분류율이
# 올라간다. 길이가 긴 키부터 먼저 시도해야 "유기농 엑스트라버진 올리브유"가
# "올리브유"보다 먼저 매칭되어 더 구체적인 이름으로 분류된다.
CATEGORY_KEYWORDS = {
    "선크림": "선크림", "폴로티": "폴로티", "스트레치바지": "스트레치바지",
    "반팔티": "반팔티", "콜라겐": "콜라겐", "비타민": "비타민",
    "허브캔디": "허브캔디", "방수팩": "방수팩", "선글라스": "선글라스",
    "팝콘치킨": "팝콘치킨", "레티놀": "레티놀",
    "올리브유": "올리브유", "올리브오일": "올리브유",
    "아보카도오일": "아보카도오일", "아보카도 오일": "아보카도오일",
    "마요네즈": "마요네즈", "마요네스": "마요네즈",
    "머스타드": "머스타드", "감자칩": "감자칩", "감자튀김": "감자튀김",
    "슈스트링": "감자튀김",
    "치킨너겟": "치킨너겟", "가라아게": "치킨너겟",
    "땅콩버터": "땅콩버터", "피넛버터": "땅콩버터",
    "오레오": "초콜릿 샌드위치 쿠키",
    # 제품명만으로는 제품군을 유추할 수 없는 브랜드/제품명 -> 제품군 매핑.
    # 새 브랜드를 마주칠 때마다 여기 한 줄씩 추가해나간다.
    "하리보": "구미젤리", "골드베렌": "구미젤리", "스타믹스": "구미젤리",
    "소바바치킨": "냉동치킨",
}
UNCATEGORIZED_LABEL = "미분류"

# 제품군 블록 하나는 "제목 행" + 아래 11개 항목 행으로 구성된다. 상품은 이
# 항목들을 세로로 채운 열 하나로 표현되고(카드형), 같은 제품군의 상품들이
# 옆으로(B, C, D...) 나란히 쌓인다. OCR 상품카드에는 이 중 상품명/규격·단량/
# 판매가/단위단가/병입원산지(카드 자체에 원산지가 찍혀 있을 때만)/셀링만
# 있으므로 CATEGORY_FIELD_MAP에 있는 행만 채우고 나머지(사진/소싱형태/
# 매출(연)/매총율/산도)는 빈 칸으로 남긴다 - 수기로 채우거나 다른 소스에서
# 나중에 채워 넣을 몫이다. 상품코드/파일ID/원문텍스트는 여기 안 넣는다 -
# 이 시트는 사람이 보기 좋은 정리본이고, 그 추적용 정보는 원본(RAW) 시트에
# 이미 행마다 남아있다.
CATEGORY_ROW_LABELS = [
    "사진", "상품명", "소싱형태", "매출(연)", "규격/단량", "판매가",
    "단위단가", "매총율", "산도", "병입원산지", "셀링",
]
CATEGORY_FIELD_MAP = {
    "상품명": "제품명(한국어)",
    "규격/단량": "중량",
    "판매가": "가격",
    "단위단가": "단가",
    # 뒷면 사진이 매칭돼야만 채워지던 값과 달리, 카드 자체에 원산지가 직접
    # 찍혀 있을 때만 _parse_fields_from_lines()가 채운다 - 못 찾으면 빈
    # 문자열이라 이 칸은 그대로 비워두고, sync_back_sourcing()이 나중에
    # 뒷면 매칭으로 채울 수 있게 남겨둔다(이미 채워진 칸은 덮어쓰지 않으므로
    # 서로 충돌하지 않는다).
    "병입원산지": "병입원산지",
    "셀링": "셀링포인트",
}


# ---- 자사 카탈로그(MC) 기반 제품군 매칭 ----
# 카탈로그 상품명과 OCR 상품명은 표기가 다 다르므로(예: 카탈로그의
# "OO브랜드 아보카도 오일 1L"과 OCR의 "피에트로코리첼리 아보카도오일 1L(퓨어)")
# 완전일치가 아니라 부분 문자열 겹침으로 찾는다. 단순히 "글자가 좀 겹친다"만으로
# 매칭하면 "아보카도오일"과 "냉동 아보카도(원물)"처럼 전혀 다른 상품이
# "아보카도"라는 흔한 단어 하나로 잘못 엮일 수 있다. 그래서:
#   1. 가장 긴 겹치는 부분 문자열부터 확인한다(길수록 구체적이라 더 믿을 만함).
#   2. 그 부분 문자열을 포함하는 카탈로그 항목들의 제품군(MC)이 한쪽으로
#      확실히 쏠려 있을 때만(과반 이상, 최소 2건 이상 근거) 그 제품군을 쓴다.
#      여러 제품군에 흩어져 있으면(비특이적인 단어라는 뜻) 억지로 맞추지 않고
#      포기한다 - 틀리게 분류되는 것보다 미분류로 남는 게 낫다.
_CATEGORY_INDEX_MIN_LEN = 4
_CATEGORY_INDEX_MAX_LEN = 12
_CATEGORY_MATCH_MIN_SUPPORT = 2  # 근거로 삼는 카탈로그 항목이 최소 이만큼은 있어야 함
_CATEGORY_MATCH_MIN_CONFIDENCE = 0.6  # 그 중 최다 제품군의 비중이 이 이상이어야 함

_category_index = None  # build_category_index()로 한 번 만들어서 재사용한다


def _compact(s: str) -> str:
    return re.sub(r"\s+", "", s or "")


def build_category_index(catalog_rows) -> dict:
    """catalog_rows: (제품군(MC), 상품명) 튜플의 목록. {길이: {부분문자열: {제품군: 등장횟수}}}
    형태로 색인해서, match_category_from_catalog()가 빠르게 조회할 수 있게 한다."""
    index = {length: {} for length in range(_CATEGORY_INDEX_MIN_LEN, _CATEGORY_INDEX_MAX_LEN + 1)}
    for mc, name in catalog_rows:
        if not mc or not name:
            continue
        compact = _compact(name)
        for length in range(_CATEGORY_INDEX_MIN_LEN, min(_CATEGORY_INDEX_MAX_LEN, len(compact)) + 1):
            bucket_by_len = index[length]
            for i in range(len(compact) - length + 1):
                sub = compact[i:i + length]
                bucket = bucket_by_len.setdefault(sub, {})
                bucket[mc] = bucket.get(mc, 0) + 1
    return index


def match_category_from_catalog(product_name: str, index) -> str:
    if not index:
        return ""
    compact = _compact(product_name)
    for length in range(min(_CATEGORY_INDEX_MAX_LEN, len(compact)), _CATEGORY_INDEX_MIN_LEN - 1, -1):
        bucket = index.get(length)
        if not bucket:
            continue
        votes = {}
        for i in range(len(compact) - length + 1):
            hit = bucket.get(compact[i:i + length])
            if not hit:
                continue
            for mc, count in hit.items():
                votes[mc] = votes.get(mc, 0) + count
        if not votes:
            continue
        total = sum(votes.values())
        if total < _CATEGORY_MATCH_MIN_SUPPORT:
            continue
        top_mc, top_count = max(votes.items(), key=lambda kv: kv[1])
        if top_count / total >= _CATEGORY_MATCH_MIN_CONFIDENCE:
            return top_mc
    return ""


def load_category_index(creds):
    """MC_CATALOG_SPREADSHEET_ID가 설정돼있으면 그 비공개 시트를 읽어서
    카탈로그 매칭 색인을 만든다. 설정 안 돼있으면(또는 읽기 실패하면) 조용히
    건너뛰고 CATEGORY_KEYWORDS 수동 목록만으로 분류한다 - 이 색인은 어디까지나
    분류 정확도를 높이는 보조 수단이라, 없어도 파이프라인 자체는 정상 동작해야
    한다."""
    global _category_index
    if not MC_CATALOG_SPREADSHEET_ID:
        print("MC_CATALOG_SPREADSHEET_ID 미설정 - 자사 카탈로그 매칭 없이 CATEGORY_KEYWORDS만 사용합니다.")
        return
    try:
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(MC_CATALOG_SPREADSHEET_ID).worksheet(MC_CATALOG_SHEET_NAME)
        values = sheet.get_all_values()
    except Exception as e:
        print(f"경고: 자사 카탈로그 시트를 읽지 못해 카탈로그 매칭 없이 진행합니다: {e}")
        return
    # 헤더(MCH1, MC, 상품명) 제외, MC/상품명 둘 다 있는 행만 사용한다.
    rows = [
        (row[1].strip(), row[2].strip())
        for row in values[1:]
        if len(row) >= 3 and row[1] and row[2]
    ]
    _category_index = build_category_index(rows)
    print(f"자사 카탈로그 {len(rows)}건으로 제품군 분류 색인을 만들었습니다.")


def detect_category(product_name: str) -> str:
    catalog_match = match_category_from_catalog(product_name, _category_index)
    if catalog_match:
        return catalog_match
    for kw in sorted(CATEGORY_KEYWORDS, key=len, reverse=True):
        if kw in product_name:
            return CATEGORY_KEYWORDS[kw]
    return UNCATEGORIZED_LABEL


def _col_letter(col: int) -> str:
    letters = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


# Google Sheets API의 values:batchGet/batchUpdate는 한 번에 보낼 수 있는 범위
# 개수에 제한이 있다 - 실측으로 228개 범위는 항상 400(Bad Request)으로
# 실패하고 150개는 성공함을 확인했다(정리본(트레이더스)_2026-01, 카드 114개
# x 소싱형태/병입원산지 2칸 = 228개 범위 요청 시 매번 실패해서 트레이더스
# 소싱형태/병입원산지가 단 하나도 안 채워지는 문제로 이어졌다). 카드/행이
# 계속 쌓이는 시트(정리본, RAW 앞뒤매칭/정리본위치 등)는 언젠가 이 상한을
# 넘을 수 있으므로, batch_get/batch_update를 쓰는 모든 곳에서 이 크기로
# 나눠 보낸다.
_SHEETS_BATCH_CHUNK_SIZE = 100


def _batch_get_chunked(sheet, ranges: list):
    result = []
    for i in range(0, len(ranges), _SHEETS_BATCH_CHUNK_SIZE):
        result.extend(sheet.batch_get(ranges[i:i + _SHEETS_BATCH_CHUNK_SIZE]))
    return result


def _batch_update_chunked(sheet, updates: list, **kwargs):
    for i in range(0, len(updates), _SHEETS_BATCH_CHUNK_SIZE):
        sheet.batch_update(updates[i:i + _SHEETS_BATCH_CHUNK_SIZE], **kwargs)


# USER_ENTERED로 쓰는 값이 "+"/"-"/"="/"@"로 시작하면 구글 시트가 그 셀을
# 수식으로 해석하려다 실패해서 "#ERROR!"로 뜬다(실사진에서 확인됨 - OCR로
# 뽑은 제품명이 우연히 "+1 정밀 트리머"처럼 "+"로 시작한 경우). 앞에 작은따옴표
# 하나를 붙이면 시트가 "이 뒤는 그냥 텍스트"로 인식해서 수식으로 해석하지
# 않고, 화면에 보일 때도 작은따옴표 자체는 안 보인다(수식 입력줄에서만 보임).
# 시트에 쓰는 모든 값(RAW/정리본/매칭 갱신)에 공통으로 적용한다.
_FORMULA_TRIGGER_CHARS = ("=", "+", "-", "@")


def _sheet_safe(value):
    text = str(value)
    if text.startswith(_FORMULA_TRIGGER_CHARS):
        return "'" + text
    # 앞자리 0이 있는 순수 숫자 문자열("0033200975939" 같은 바코드/상품코드)도
    # USER_ENTERED가 숫자로 해석해버리면 앞자리 0이 통째로 사라진다(실사진
    # 대조로 확인됨 - 트레이더스 상품코드 3건이 "0033200975939" ->
    # "33200975939"로 저장돼 있었음). int로 왕복했을 때 원래 문자열과
    # 달라지면(=앞자리 0을 잃으면) 강제 텍스트로 escape한다.
    if text.isdigit() and len(text) > 1 and str(int(text)) != text:
        return "'" + text
    return value


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
    스레드로 호출한다. 같은 상품코드가 다시 나와도 항상 새 열을 만든다.

    돌려주는 값은 {파일ID: "시트제목!C15"} 형태로, 각 카드가 어느 정리본
    시트의 어느 칸(열+제목행)에 기록됐는지를 나타낸다 - 뒷면 사진이 나중에
    도착했을 때 소싱형태/병입원산지를 그 칸에 소급 반영하려면
    (sync_back_sourcing 참고) 이 위치를 RAW 시트의 "정리본위치" 컬럼에
    남겨둬야 하기 때문이다. 정리본이 촬영월별로 여러 시트로 나뉘어 있으므로
    시트 제목까지 같이 남긴다."""
    if not row_dicts:
        return {}

    values = sheet.get_all_values()
    blocks, next_new_row = _scan_category_blocks(values)
    updates = []
    positions = {}
    max_col_used = 1
    max_row_used = len(values)

    for row_dict in row_dicts:
        category = detect_category(row_dict.get("제품명(한국어)") or "")
        block = blocks.get(category)
        if block is None:
            title_row = next_new_row
            field_rows = {label: title_row + i for i, label in enumerate(CATEGORY_ROW_LABELS, start=1)}
            block = {"title_row": title_row, "field_rows": field_rows, "next_col": 2}
            blocks[category] = block
            next_new_row = title_row + 1 + len(CATEGORY_ROW_LABELS) + 2
            updates.append({"range": f"A{title_row}", "values": [[_sheet_safe(category)]]})
            updates.append({
                "range": f"A{title_row + 1}:A{title_row + len(CATEGORY_ROW_LABELS)}",
                "values": [[label] for label in CATEGORY_ROW_LABELS],
            })
            max_row_used = max(max_row_used, title_row + len(CATEGORY_ROW_LABELS))

        col_letter = _col_letter(block["next_col"])
        for label, source_key in CATEGORY_FIELD_MAP.items():
            value = row_dict.get(source_key) or ""
            row = block["field_rows"][label]
            updates.append({"range": f"{col_letter}{row}", "values": [[_sheet_safe(value)]]})
        max_col_used = max(max_col_used, block["next_col"])
        file_id = row_dict.get("파일ID")
        if file_id:
            # 정리본이 촬영월별로 여러 시트로 나뉘어 있어(아래 run_once 참고),
            # 어느 시트의 칸인지도 같이 남겨야 sync_back_sourcing()이 나중에
            # 정확한 시트를 다시 찾을 수 있다.
            positions[file_id] = f"{sheet.title}!{col_letter}{block['title_row']}"
        block["next_col"] += 1

    if updates:
        # 한 제품군에 상품이 계속 쌓이면(특히 "미분류") 시트 기본 26열을 금방
        # 넘어설 수 있다. 시트 크기를 안 늘리고 그 열에 쓰려고 하면 Sheets API가
        # "grid limits를 벗어났다"며 batch_update 전체를 통째로 거부해서, 이번에
        # 쓰려던 항목뿐 아니라 이미 잘 들어가 있던 다른 항목까지 전부 안 써진
        # 것처럼 실패해버린다(실제로 코스트코 정리본이 이 문제로 계속 비어
        # 있었다). 그래서 필요한 만큼 미리 늘려두고 쓴다.
        if max_col_used > sheet.col_count:
            sheet.resize(cols=max_col_used + 10)
        if max_row_used > sheet.row_count:
            sheet.resize(rows=max_row_used + 20)
        _batch_update_chunked(sheet, updates, value_input_option="USER_ENTERED")

    return positions


# ---------------- 시트 저장 ----------------
sheet_lock = threading.Lock()  # gspread 동시 append 충돌 방지


def build_row_dict(file_id, filename, fields, raw_text, uploader, is_back, capture_time, image_hash, retailer):
    # 상품코드 이후부터만 스캔하는 앵커(배경 상품의 "•" 표준 문구 오채택 방지,
    # extract_selling_points 참고)는 "코드 -> ... -> 셀링" 순서인 코스트코
    # 레이아웃에서만 유효하다. 트레이더스는 반대로 "셀링 -> ... -> 코드"
    # 순서라("누텔라 스프레드" 카드처럼 셀링 문구가 바코드/코드보다 먼저 나옴)
    # 코드로 앵커하면 진짜 셀링 문구가 스캔 범위 밖으로 밀려 못 잡힌다.
    anchor_code = fields.get("상품코드", "") if retailer != "traders" else ""
    row_dict = {
        "파일ID": file_id,
        "파일명": filename,
        "처리일시": time.strftime("%Y-%m-%d %H:%M:%S"),
        "원문텍스트": raw_text,
        # RAW 시트 컬럼(COLUMN_ORDER)엔 없는 값이라 원본 로그에는 안 보이고,
        # 제품군정리(카테고리) 시트의 "셀링" 행에만 쓰인다.
        "셀링포인트": extract_selling_points(raw_text, anchor_code),
        "업로더": uploader,
        "뒷면여부": "뒷면" if is_back else "",
        "촬영시각": capture_time.strftime("%Y-%m-%d %H:%M:%S") if capture_time else "",
        # 같은 날 찍힌 동일 사진의 실수 재업로드를 걸러내는 데 쓰는 사진
        # 내용물 해시. load_same_day_hashes() 참고.
        "사진해시": image_hash,
        # 이 시점엔 아직 시트에 쌓인 다른 사진들과 비교해보기 전이라 매칭을
        # 모른다 - resync_card_back_matches()가 별도로 채운다.
        "매칭파일ID": "",
        # 카드가 제품군정리 시트에 기록될 때(update_category_sheet) 채워진다.
        # 뒷면 사진 자체는 이 값이 끝까지 빈 칸으로 남는다.
        "정리본위치": "",
    }
    row_dict.update(fields)  # 상품코드/제품명(한국어)/가격
    return row_dict


def resync_card_back_matches(sheet):
    """RAW 시트에 지금까지 기록된 모든 사진(이번 실행분은 물론, 과거에 실행
    버튼을 눌렀을 때 처리된 사진까지 전부)을 다시 훑어서 상품카드<->뒷면
    매칭을 재계산하고, 값이 바뀐 칸만 시트에 반영한다.

    이 파이프라인은 정해진 시각에 자동으로 도는 게 아니라 대시보드
    "업데이트" 버튼을 누를 때마다 한 번씩 실행되는 구조라, 카드 사진과
    그 뒷면 사진이 서로 다른 실행 회차에 나뉘어 올라올 수 있다(뒷면을
    깜빡하고 나중에 올리는 등). 이번 실행에서 새로 처리된 사진끼리만
    비교하면 이런 경우를 놓치므로, 매칭은 항상 시트에 쌓인 전체 이력을
    기준으로 다시 계산한다 - 시트 자체가 실행 회차를 넘나드는 유일한
    영속 저장소이기 때문이다. 매번 전체를 다시 계산하지만 실제로
    값이 바뀐 칸만 쓰기 때문에, 이미 정착된 과거 매칭까지 매번 다시
    쓰지는 않는다.

    다만 이 컬럼들(업로더/뒷면여부/촬영시각)이 생기기 전에 이미 처리된
    과거 행은 촬영시각이 비어있어 매칭 대상이 될 수 없다 - 새로 배포된
    이후 처리되는 사진부터 소급 매칭이 적용된다."""
    idx = {name: COLUMN_ORDER.index(name) for name in ("파일ID", "업로더", "뒷면여부", "촬영시각", "매칭파일ID")}
    columns = {name: sheet.col_values(i + 1)[1:] for name, i in idx.items()}  # 헤더 제외
    row_count = len(columns["파일ID"])
    for name in columns:
        # gspread의 col_values()는 그 열의 마지막 값이 있는 행까지만 돌려주므로,
        # 뒤쪽 행에서 특정 컬럼만 비어있으면(흔한 경우) 열마다 길이가 다를 수
        # 있다 - 파일ID 기준 행 수에 맞춰 빈 문자열로 채워 인덱스를 맞춘다.
        if len(columns[name]) < row_count:
            columns[name] += [""] * (row_count - len(columns[name]))

    entries = []
    for i in range(row_count):
        file_id = columns["파일ID"][i]
        if not file_id:
            continue
        capture_time = None
        raw_time = columns["촬영시각"][i]
        if raw_time:
            try:
                capture_time = datetime.datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                capture_time = None
        entries.append({
            "file_id": file_id,
            "uploader": columns["업로더"][i],
            "capture_time": capture_time,
            "is_back": columns["뒷면여부"][i] == "뒷면",
        })

    matches = match_card_back_pairs(entries)
    match_col_letter = _col_letter(idx["매칭파일ID"] + 1)
    updates = []
    for i, file_id in enumerate(columns["파일ID"]):
        if not file_id:
            continue
        new_value = ", ".join(matches.get(file_id, []))
        if new_value and new_value != columns["매칭파일ID"][i]:
            updates.append({"range": f"{match_col_letter}{i + 2}", "values": [[_sheet_safe(new_value)]]})
    if updates:
        _batch_update_chunked(sheet, updates, value_input_option="USER_ENTERED")
    return len(updates)


def _write_category_positions(sheet, positions: dict):
    """update_category_sheet()가 돌려준 {파일ID: "C15"}를 RAW 시트의
    "정리본위치" 컬럼에 되써 넣는다. 이번 실행에서 방금 append한 행이라
    파일ID로 다시 찾아야 몇 번째 행인지 알 수 있다."""
    if not positions:
        return
    file_id_col = COLUMN_ORDER.index("파일ID") + 1
    pos_col_letter = _col_letter(COLUMN_ORDER.index("정리본위치") + 1)
    file_ids = sheet.col_values(file_id_col)[1:]  # 헤더 제외
    updates = []
    for i, file_id in enumerate(file_ids):
        if file_id in positions:
            updates.append({"range": f"{pos_col_letter}{i + 2}", "values": [[positions[file_id]]]})
    if updates:
        _batch_update_chunked(sheet, updates, value_input_option="USER_ENTERED")


def sync_back_sourcing(sheet, retailer: str, resolve_category_sheet) -> int:
    """RAW 시트 전체 이력을 훑어, 상품카드<->뒷면 매칭이 되어 있고 그 카드가
    이미 제품군정리 시트에 칸을 가지고 있는(정리본위치가 채워진) 경우에
    한해 소싱형태/병입원산지를 계산해서 그 칸에 채운다.

    정리본이 촬영월별로 여러 시트로 나뉘어 있어(run_once 참고), "정리본위치"에는
    "시트제목!C15"처럼 어느 정리본 시트인지도 같이 적혀 있다. resolve_category_sheet(title)는
    그 제목의 gspread Worksheet를 열어 돌려주는 함수로, 카드가 속한 달의
    정리본이 이번 실행에서 한 번도 열리지 않았어도(예: 몇 달 전 카드에 뒷면
    사진이 뒤늦게 도착한 경우) 필요할 때마다 그 시트를 연다. 월별 분리
    이전에 이미 기록된 옛 형식(시트제목 없이 "C15"만 있음)은 그 시절엔
    정리본이 리테일러당 하나뿐이었던 시절의 이름으로 대신 찾는다.

    resync_card_back_matches()와 마찬가지로 매번 전체 이력을 다시 계산한다 -
    뒷면이 카드보다 늦게(심지어 다른 실행 회차에) 올라와도 소급 반영되어야
    하기 때문이다. 다만 이미 값이 채워진 칸(과거에 이 함수가 채웠든, 사람이
    수기로 고쳤든)은 절대 덮어쓰지 않는다 - 이 시트는 사람이 직접 편집하는
    정리본이므로, 자동화가 매 실행마다 수기 수정 내용을 지우면 안 된다."""
    idx = {
        name: COLUMN_ORDER.index(name)
        for name in ("파일ID", "업로더", "뒷면여부", "촬영시각", "원문텍스트", "제품명(한국어)", "정리본위치")
    }
    columns = {name: sheet.col_values(i + 1)[1:] for name, i in idx.items()}
    row_count = len(columns["파일ID"])
    for name in columns:
        if len(columns[name]) < row_count:
            columns[name] += [""] * (row_count - len(columns[name]))

    entries = []
    for i in range(row_count):
        file_id = columns["파일ID"][i]
        if not file_id:
            continue
        capture_time = None
        raw_time = columns["촬영시각"][i]
        if raw_time:
            try:
                capture_time = datetime.datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                capture_time = None
        entries.append({
            "file_id": file_id,
            "uploader": columns["업로더"][i],
            "capture_time": capture_time,
            "is_back": columns["뒷면여부"][i] == "뒷면",
            "text": columns["원문텍스트"][i],
            "product_name": columns["제품명(한국어)"][i],
            "position": columns["정리본위치"][i],
        })
    by_id = {e["file_id"]: e for e in entries}
    matches = match_card_back_pairs(entries)

    candidates_by_title = {}  # 정리본 시트 제목 -> [(셀주소, 값), ...]
    for card in entries:
        if card["is_back"] or not card["position"]:
            continue
        back_ids = matches.get(card["file_id"], [])
        back_text = "\n".join(by_id[bid]["text"] for bid in back_ids if bid in by_id)
        front_text = card["text"]
        # 뒷면 매칭이 없어도 카드 자체에 판단 근거(PB 문구, "직수입" 인증
        # 문구)가 있으면 계속 진행한다 - determine_sourcing()이 그 경우를
        # 처리한다(이전에는 뒷면이 없으면 무조건 건너뛰어서, 뒷면 사진이
        # 없는 PB/직수입 카드가 뒷면 없이는 영영 소싱형태를 못 받았다).
        # 뒷면도 없고 카드 자체에도 근거가 없으면 계산해봐야 항상 빈
        # 소싱형태 + "국내"(원산지 정보 전무 시의 기본값)만 나오는데, 이건
        # 실제로는 "모른다"일 뿐 "국내산으로 확인됨"이 아니므로 여기서
        # 건너뛴다.
        if (
            not back_text
            and not _PB_NAME_PATTERN.search(card["product_name"] or "")
            and "직수입" not in front_text
        ):
            continue
        if "!" in card["position"]:
            title, cell_ref = card["position"].split("!", 1)
        else:
            title, cell_ref = _LEGACY_CATEGORY_SHEET_TITLE[retailer], card["position"]
        m = re.match(r"^([A-Z]+)(\d+)$", cell_ref)
        if not m:
            continue
        col, title_row = m.group(1), int(m.group(2))
        sourcing, origin = determine_sourcing(card["product_name"], retailer, back_text, front_text)
        sourcing_row = title_row + CATEGORY_ROW_LABELS.index("소싱형태") + 1
        origin_row = title_row + CATEGORY_ROW_LABELS.index("병입원산지") + 1
        candidates_by_title.setdefault(title, []).append((f"{col}{sourcing_row}", sourcing))
        candidates_by_title.setdefault(title, []).append((f"{col}{origin_row}", origin))

    total_updates = 0
    for title, candidates in candidates_by_title.items():
        candidates = [(cell, value) for cell, value in candidates if value]
        if not candidates:
            continue
        category_sheet = resolve_category_sheet(title)
        current = _batch_get_chunked(category_sheet, [cell for cell, _ in candidates])
        updates = []
        for (cell, value), grid in zip(candidates, current):
            existing = grid[0][0] if grid and grid[0] else ""
            if not existing:
                updates.append({"range": cell, "values": [[value]]})
        if updates:
            _batch_update_chunked(category_sheet, updates, value_input_option="USER_ENTERED")
        total_updates += len(updates)
    return total_updates


def append_rows_to_sheet(sheet, row_dicts):
    rows = [[_sheet_safe(row_dict.get(col, "")) for col in COLUMN_ORDER] for row_dict in row_dicts]
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


def process_one_file(creds, sheets, file_info, archive_folder_id, retailer, source_folder_id, same_day_hashes):
    name, file_id = file_info["name"], file_info["id"]
    drive_service = get_thread_drive_service(creds)
    image_bytes = download_image(drive_service, file_id)
    # 앞뒤 사진 매칭용 신호. normalize_image_for_ocr()는 방향 태그를 지우고
    # 재인코딩하므로 반드시 그 전 원본 바이트에서 촬영시각을 읽어야 한다.
    # owners는 Drive 목록 조회 시점에 이미 받아온 값(list_all_images 참고).
    capture_time = extract_capture_time(image_bytes, name)
    uploader = (file_info.get("owners") or [{}])[0].get("emailAddress", "")
    image_hash = hashlib.sha256(image_bytes).hexdigest()

    # 같은 날 찍힌 완전히 같은 사진이 실수로 두 번 업로드된 경우만 걸러낸다.
    # 촬영시각을 모르는 사진(EXIF도 파일명 패턴도 없음)은 "같은 날"을 판단할
    # 근거가 없으므로 걸러내지 않고 원래대로 처리한다 - 날짜가 다르면(예:
    # 다음 달 재촬영) 해시가 같아도 절대 걸리지 않으니, 시기별 SKU 이력이
    # 끊기는 일은 없다.
    if capture_time:
        dedup_key = (image_hash, capture_time.strftime("%Y-%m-%d"))
        with same_day_hash_lock:
            if dedup_key in same_day_hashes:
                try:
                    archive_file(drive_service, file_id, archive_folder_id, source_folder_id)
                except Exception as e:
                    print(f"  경고: '{name}' 중복 사진 보관 이동 실패: {e}")
                return name, None, False, None, retailer, False, True
            same_day_hashes.add(dedup_key)

    image_bytes = normalize_image_for_ocr(image_bytes)
    text, confidences = ocr_image_azure(image_bytes)
    low_confidence = needs_review(confidences)
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    is_back = is_back_label(lines)

    # 코스트코/트레이더스는 더 이상 텍스트 내용으로 추측하지 않는다 - 어느
    # Drive 폴더(source_folder_id)에서 온 사진인지로 이미 확정돼서 넘어온다.
    # 배경에 다른 가격표가 같이 찍혀도(예: 초점 밖 진열대의 옆 상품) 사진
    # 한 장당 항상 메인 상품 하나만 뽑는다.
    if retailer == "traders":
        fields = parse_traders_fields(text)
        sheet = sheets["traders"]
    else:
        fields = parse_price_fields(text)
        sheet = sheets["costco"]

    # needs_review()는 "인식된 단어들의 신뢰도"만 본다 - Azure가 가격표의
    # 일부(제품명/가격/바코드 등) 영역을 아예 통째로 못 읽고 건너뛴 경우엔,
    # 인식된 나머지 단어들의 신뢰도 자체는 높을 수 있어서 이 경우를 못
    # 잡아낸다. 가격과 제품명(한국어)은 상품카드에 항상 있어야 하는 값이므로,
    # 둘 중 하나라도 비어있으면 그 자체로 검토가 필요하다는 뜻이다. 다만 이건
    # 상품카드에만 해당하는 얘기다 - 제품 뒷면 사진은 애초에 가격이 없는 게
    # 정상이므로 뒷면으로 판별된 사진에는 이 검토 신호를 적용하지 않는다.
    if not is_back and (not fields.get("가격") or not fields.get("제품명(한국어)")):
        low_confidence = True

    row_dict = build_row_dict(file_id, name, fields, text, uploader, is_back, capture_time, image_hash, retailer)
    append_rows_to_sheet(sheet, [row_dict])

    # 시트 기록이 끝난 뒤에 사진을 '처리완료' 폴더로 옮긴다. 이동이 실패해도
    # 시트 기록(=파일ID 기준 중복 처리 방지)은 이미 끝났으므로 데이터 유실은
    # 아니다 - 그 사진만 원래 폴더에 계속 남아있을 뿐이라 실행을 실패로
    # 처리하지 않고 경고만 남긴다.
    try:
        archive_file(drive_service, file_id, archive_folder_id, source_folder_id)
    except Exception as e:
        print(f"  경고: '{name}' 보관 폴더 이동 실패 (시트 기록은 완료됨): {e}")

    return name, fields.get("제품명(한국어)") or "", low_confidence, row_dict, retailer, is_back, False


# ---------------- 메인 실행 ----------------
def run_once():
    require_config()

    creds = get_credentials()
    load_category_index(creds)
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
    # 리테일러별로 따로 두되, 이제 촬영월별로도 나눈다 - "언제 OCR을 돌렸는지"가
    # 아니라 "언제 찍힌 사진인지" 기준으로 그 시기의 코스트코/트레이더스
    # 상품 스냅샷을 보여주기 위함이라, 같은 상품을 다른 달에 다시 촬영해도
    # 중복 제거되지 않고 그 달의 정리본에 새로 쌓인다. 시트는 실제로 필요한
    # 순간(카드가 그 달에 처리되거나, 그 달 카드에 뒷면이 뒤늦게 매칭될 때)에만
    # 열거나 새로 만든다.
    category_sheet_cache = {}  # 시트 제목 -> Worksheet 객체 (매 실행마다 한 번씩만 열기)

    def category_sheet_title(retailer, month_key):
        base_name = (
            MONTHLY_CATEGORY_SHEET_NAME_COSTCO if retailer == "costco"
            else MONTHLY_CATEGORY_SHEET_NAME_TRADERS
        )
        return f"{base_name}_{month_key}"

    def get_category_sheet(title):
        if title not in category_sheet_cache:
            category_sheet_cache[title] = open_or_create_sheet(title)
        return category_sheet_cache[title]

    # 코스트코/트레이더스는 이제 서로 다른 Drive 폴더에서 온다 - 어느 폴더에서
    # 조회했는지가 곧 리테일러이므로, 신규 파일 목록도 시트별로 각자의 폴더에서
    # 각자의 처리완료 목록 기준으로 따로 뽑는다.
    folder_ids = {"costco": DRIVE_FOLDER_ID_COSTCO, "traders": DRIVE_FOLDER_ID_TRADERS}
    new_files = {}
    archive_folder_ids = {}
    same_day_hashes = {}
    for retailer, folder_id in folder_ids.items():
        processed_ids = load_processed_ids(sheets[retailer])
        all_files = list_all_images(drive_service, folder_id)
        new_files[retailer] = [f for f in all_files if f["id"] not in processed_ids]
        archive_folder_ids[retailer] = get_or_create_archive_folder(drive_service, folder_id)
        same_day_hashes[retailer] = load_same_day_hashes(sheets[retailer])

    total_new = sum(len(files) for files in new_files.values())
    if total_new == 0:
        print("처리할 새 이미지가 없습니다.")
        return

    print(f"신규 이미지 {total_new}건 발견"
          f"(코스트코 {len(new_files['costco'])}건, 트레이더스 {len(new_files['traders'])}건). "
          "처리를 시작합니다...")
    print(f"(Azure 무료 티어 속도 제한: 분당 {AZURE_MAX_CALLS_PER_MINUTE}건 -> "
          f"예상 소요 시간 약 {total_new / AZURE_MAX_CALLS_PER_MINUTE:.0f}분)")

    success_count = 0
    failed = []
    duplicate_count = 0
    # 리테일러 -> {촬영월(YYYY-MM): [row_dict, ...]} - 정리본을 월별로 나누기
    # 위한 그룹화(아래 update_category_sheet 호출부 참고).
    processed_row_dicts = {"costco": {}, "traders": {}}

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = {
            executor.submit(
                process_one_file, creds, sheets, f, archive_folder_ids[retailer], retailer,
                folder_ids[retailer], same_day_hashes[retailer],
            ): f
            for retailer, files in new_files.items()
            for f in files
        }
        for future in as_completed(futures):
            f = futures[future]
            try:
                name, product, flag, row_dict, retailer, is_back, is_duplicate = future.result()
                success_count += 1
                if is_duplicate:
                    duplicate_count += 1
                    print(f"  건너뜀: {name} -> 같은 날 찍힌 동일 사진이 이미 등록돼 있어 중복 처리하지 않음")
                    continue
                # 제품 뒷면 사진은 RAW 시트엔 그대로 기록되지만(위에서 이미 끝남),
                # 제품군정리(정리본)에는 상품카드에서 이미 뽑은 내용을 중복으로
                # 넣지 않도록 애초에 이 목록에 넣지 않는다 - 그러면 update_category_sheet()가
                # 이 사진을 위한 새 열 자체를 만들지 않는다.
                if not is_back:
                    # 정리본을 어느 달 시트에 넣을지는 촬영시각 기준이다 - OCR을
                    # 언제 돌렸는지가 아니라 실제로 언제 찍힌 사진인지로 나눠야
                    # "그 시기에 있었던 상품" 스냅샷이 되기 때문이다. 촬영시각을
                    # 알 수 없는 사진(EXIF도 파일명 패턴도 없음)만 예외적으로
                    # 이번 실행 시각(처리월) 기준으로 넣는다.
                    capture_str = row_dict.get("촬영시각") or ""
                    month_key = capture_str[:7] if capture_str else time.strftime("%Y-%m")
                    processed_row_dicts[retailer].setdefault(month_key, []).append(row_dict)
                flag_str = " [검토필요]" if flag else ""
                back_str = " [뒷면]" if is_back else ""
                print(f"  완료: {name} -> {product or '(제품명 인식 실패)'}{flag_str}{back_str}")
            except Exception as e:
                failed.append((f["name"], str(e)))
                print(f"  실패: {f['name']} -> {e}")

    # 제품군 블록에 열 번호를 매기는 작업은 동시에 하면 충돌하므로, 병렬
    # 처리가 다 끝난 뒤 단일 스레드로 한 번에, 그리고 월별 시트 단위로 처리한다.
    for retailer, by_month in processed_row_dicts.items():
        for month_key, row_dicts in by_month.items():
            try:
                sheet_for_month = get_category_sheet(category_sheet_title(retailer, month_key))
                positions = update_category_sheet(sheet_for_month, row_dicts)
                # 방금 만든 칸의 위치를 RAW 시트에 남겨둔다 - 뒷면 사진이 나중에
                # (심지어 다음 실행 회차에) 도착했을 때 그 칸을 다시 찾아 소싱형태/
                # 병입원산지를 채우려면 필요하다 (sync_back_sourcing 참고).
                _write_category_positions(sheets[retailer], positions)
            except Exception as e:
                print(f"경고: {retailer} {month_key} 제품군정리 시트 갱신 실패 (원본 시트 기록은 정상 완료됨): {e}")

    # 상품카드 <-> 뒷면 매칭은 이번 실행분만으로는 계산하지 않는다 - 대시보드
    # "업데이트" 버튼을 누를 때마다 한 번씩 도는 구조라 카드와 뒷면이 서로
    # 다른 실행 회차에 나뉘어 올라올 수 있고, 그러면 이번 실행분끼리만
    # 비교해서는 놓친다. RAW 시트 전체(과거 실행분 포함)를 기준으로 매번
    # 다시 계산해서, 뒤늦게 올라온 사진도 다음 실행 때 소급 매칭되게 한다.
    for retailer, sheet in sheets.items():
        try:
            updated = resync_card_back_matches(sheet)
            if updated:
                print(f"  {retailer}: 앞뒤 매칭 {updated}건 반영(과거 실행분 포함)")
        except Exception as e:
            print(f"경고: {retailer} 앞뒤 매칭 반영 실패 (원본 시트 기록은 정상 완료됨): {e}")
        # 앞뒤 매칭이 갱신된 뒤에 소싱형태/병입원산지를 반영해야, 이번 실행에서
        # 막 매칭된 카드<->뒷면 쌍도 바로 반영된다.
        try:
            filled = sync_back_sourcing(sheet, retailer, get_category_sheet)
            if filled:
                print(f"  {retailer}: 소싱형태/병입원산지 {filled}건 반영")
        except Exception as e:
            print(f"경고: {retailer} 소싱형태/병입원산지 반영 실패 (원본 시트 기록은 정상 완료됨): {e}")

    print(f"\n완료: {success_count - duplicate_count}건 등록, {duplicate_count}건 중복 건너뜀, {len(failed)}건 실패")
    if failed:
        print("실패 목록 (다음 실행 시 자동 재시도됩니다 - 시트에 기록되지 않은 파일ID는 신규로 취급됨):")
        for name, err in failed:
            print(f"  - {name}: {err}")


if __name__ == "__main__":
    run_once()
