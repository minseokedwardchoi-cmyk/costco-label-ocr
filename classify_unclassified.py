"""정리본(제품군정리) 시트의 '미분류' 블록을 Gemini로 분류 제안한다.

카탈로그 매칭 + CATEGORY_KEYWORDS(main.py)로 못 잡은 상품만 대상으로 하고,
원본 정리본 시트는 건드리지 않는다 - 결과는 "AI분류제안_<원본탭이름>"이라는
새 탭에 상품명/제안 제품군/확신도/사유로 적어두기만 한다. 실제로 정리본에
반영할지는 사람이 검토 후 결정한다(제품군 블록을 옮기는 건 되돌리기 번거로운
작업이라 자동으로 하지 않는다).

사용법:
    python classify_unclassified.py --sheet "정리본(코스트코)_2026-07"
    python classify_unclassified.py --sheet "정리본(코스트코)_2026-07" --retailer costco

GEMINI_API_KEY가 설정되어 있지 않으면 아무 것도 하지 않고 안내 메시지만 남기고
종료한다(선택 기능이라 파이프라인 전체를 막으면 안 됨).
"""
import argparse
import json
import sys

import gspread

import gemini_client
import main as ocr  # get_credentials, CATEGORY_KEYWORDS, CATEGORY_ROW_LABELS, UNCATEGORIZED_LABEL 재사용

RESULT_TAB_PREFIX = "AI분류제안_"

SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "index": {"type": "INTEGER"},
            "제품군": {"type": "STRING"},
            "confidence": {"type": "STRING"},
            "기존목록에없음": {"type": "BOOLEAN"},
            "사유": {"type": "STRING"},
        },
        "required": ["index", "제품군", "confidence", "기존목록에없음", "사유"],
    },
}

BATCH_SIZE = 20


def read_block(sheet, block_name: str):
    """정리본 시트에서 이름이 block_name인 블록을 찾아 상품 목록(dict list)으로 돌려준다."""
    values = sheet.get_all_values()
    blocks, _ = ocr._scan_category_blocks(values)
    block = blocks.get(block_name)
    if not block:
        return []

    def row_values(label):
        row_idx = block["field_rows"][label] - 1
        return values[row_idx][1:] if row_idx < len(values) else []

    names = row_values("상품명")
    sourcing = row_values("소싱형태")
    size = row_values("규격/단량")
    price = row_values("판매가")
    origin = row_values("병입원산지")
    selling = row_values("셀링")

    products = []
    for i, name in enumerate(names):
        if not name.strip():
            continue
        products.append({
            "상품명": name,
            "소싱형태": sourcing[i] if i < len(sourcing) else "",
            "규격": size[i] if i < len(size) else "",
            "판매가": price[i] if i < len(price) else "",
            "병입원산지": origin[i] if i < len(origin) else "",
            "셀링": (selling[i] if i < len(selling) else "")[:80],
        })
    return products


def classify_batch(products, known_categories):
    lines = []
    for i, p in enumerate(products):
        lines.append(
            f"{i}. 상품명: {p['상품명']} | 소싱형태: {p['소싱형태']} | 규격: {p['규격']} | "
            f"판매가: {p['판매가']} | 병입원산지: {p['병입원산지']} | 셀링포인트: {p['셀링']}"
        )
    items_text = "\n".join(lines)
    categories_text = ", ".join(known_categories)

    prompt = f"""너는 코스트코/트레이더스 상품을 제품군으로 분류하는 담당자다. 아래는 현재 "미분류"로
남아있는 상품 목록이다.

기존에 이미 쓰이고 있는 제품군 목록(가능하면 이 중에서 고르되, 정말 적합한 게 없으면 새 제품군을 제안해도 됨):
{categories_text}

분류할 상품 목록:
{items_text}

각 상품마다 다음을 JSON으로 답해라:
- index: 위 번호
- 제품군: 제품군 이름 (가능하면 기존 목록에서 선택, 간결하게)
- confidence: high/medium/low
- 기존목록에없음: 기존 목록에 없는 새 제품군을 제안했으면 true
- 사유: 왜 이렇게 분류했는지 한 문장"""

    return gemini_client.call(prompt, response_schema=SCHEMA)


def write_suggestions(sheet_obj, source_tab_name: str, products):
    tab_name = f"{RESULT_TAB_PREFIX}{source_tab_name}"
    existing = [ws.title for ws in sheet_obj.worksheets()]
    if tab_name in existing:
        ws = sheet_obj.worksheet(tab_name)
        ws.clear()
    else:
        ws = sheet_obj.add_worksheet(title=tab_name, rows=len(products) + 10, cols=9)

    header = ["상품명", "소싱형태", "규격", "판매가", "병입원산지", "AI 제안 제품군", "확신도", "기존목록에없음", "분류 사유"]
    rows = [header]
    for p in products:
        rows.append([
            p["상품명"], p["소싱형태"], p["규격"], p["판매가"], p["병입원산지"],
            p.get("ai_category", ""), p.get("ai_confidence", ""),
            "예" if p.get("ai_is_new") else "", p.get("ai_reason", ""),
        ])
    ws.update(values=rows, range_name="A1")
    ws.format("A1:I1", {
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "backgroundColor": {"red": 0.12, "green": 0.31, "blue": 0.47},
    })
    ws.freeze(rows=1)
    return tab_name


def find_latest_tab(sh, prefix):
    tabs = sorted(ws.title for ws in sh.worksheets() if ws.title.startswith(prefix))
    return tabs[-1] if tabs else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet", help='정리본 탭 이름 (예: "정리본(코스트코)_2026-07")')
    parser.add_argument("--retailer", choices=["costco", "traders"],
                         help="--sheet 대신, 해당 리테일러의 가장 최근 정리본 탭을 자동으로 찾아 사용")
    args = parser.parse_args()
    if not args.sheet and not args.retailer:
        print("--sheet 또는 --retailer 중 하나는 반드시 지정해야 합니다.")
        sys.exit(1)

    if not gemini_client.is_configured():
        print("GEMINI_API_KEY가 설정되어 있지 않아 AI 분류 제안을 건너뜁니다.")
        sys.exit(0)
    if not ocr.SPREADSHEET_ID:
        print("SPREADSHEET_ID가 설정되어 있지 않습니다.")
        sys.exit(1)

    creds = ocr.get_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(ocr.SPREADSHEET_ID)

    sheet_name = args.sheet
    if not sheet_name:
        prefix = (ocr.MONTHLY_CATEGORY_SHEET_NAME_COSTCO if args.retailer == "costco"
                  else ocr.MONTHLY_CATEGORY_SHEET_NAME_TRADERS)
        sheet_name = find_latest_tab(sh, prefix)
        if not sheet_name:
            print(f"'{prefix}_YYYY-MM' 형태의 탭을 찾지 못했습니다.")
            sys.exit(0)
        print(f"자동 감지: '{sheet_name}'")
    sheet = sh.worksheet(sheet_name)

    products = read_block(sheet, ocr.UNCATEGORIZED_LABEL)
    if not products:
        print(f"'{sheet_name}' 탭에 미분류 상품이 없습니다. 종료합니다.")
        return
    print(f"미분류 상품 {len(products)}개 발견. Gemini로 분류를 요청합니다...")

    known_categories = sorted(set(ocr.CATEGORY_KEYWORDS.values()))

    for start in range(0, len(products), BATCH_SIZE):
        batch = products[start:start + BATCH_SIZE]
        result = classify_batch(batch, known_categories)
        by_index = {r["index"]: r for r in result}
        for i, p in enumerate(batch):
            r = by_index.get(i, {})
            p["ai_category"] = r.get("제품군", "")
            p["ai_confidence"] = r.get("confidence", "")
            p["ai_is_new"] = r.get("기존목록에없음", False)
            p["ai_reason"] = r.get("사유", "")

    tab_name = write_suggestions(sh, sheet_name, products)
    print(f"'{tab_name}' 탭에 {len(products)}개 분류 제안을 기록했습니다.")
    print("원본 정리본 시트는 변경하지 않았습니다 - 검토 후 필요하면 직접 반영해주세요.")


if __name__ == "__main__":
    main()
