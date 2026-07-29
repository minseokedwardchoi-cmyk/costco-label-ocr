"""정리본(제품군정리) 두 시점(예: 이번 달/저번 달) 탭을 비교해서 SKU 변화를
분석하고, 그 결과를 새 탭에 기록한다. 원본 정리본 탭은 건드리지 않는다.

사용법:
    python monthly_comparison.py --retailer costco \
        --prev "정리본(코스트코)_2026-06" --curr "정리본(코스트코)_2026-07"

두 탭 이름을 안 주면, MONTHLY_CATEGORY_SHEET_NAME_COSTCO/TRADERS 접두사로
시작하는 탭 중 이름순으로 가장 최근 두 개를 자동으로 고른다(탭 이름이
"_YYYY-MM"으로 끝나므로 문자열 정렬이 곧 날짜순 정렬이다).
"""
import argparse
import json
import sys

import gspread

import gemini_client
import main as ocr
import product_matching as pm
from category_grouping import classify as classify_category, top_level as category_top_level

RESULT_TAB_PREFIX = "AI분석_"


def parse_price(v):
    if not v:
        return ""
    s = str(v).replace(",", "").replace("원", "").strip()
    try:
        return float(s)
    except ValueError:
        return ""


def read_all_products(sheet, id_prefix):
    values = sheet.get_all_values()
    blocks, _ = ocr._scan_category_blocks(values)
    products = []
    idx = 0
    for category, block in blocks.items():
        def row_values(label):
            row_idx = block["field_rows"][label] - 1
            return values[row_idx][1:] if row_idx < len(values) else []

        names = row_values("상품명")
        sourcing = row_values("소싱형태")
        size = row_values("규격/단량")
        price = row_values("판매가")
        unit_price = row_values("단위단가")
        origin = row_values("병입원산지")

        for i, name in enumerate(names):
            if not name.strip():
                continue
            idx += 1
            products.append(pm.prepare({
                "id": f"{id_prefix}-{idx}",
                "name": name,
                "category": category,
                "sourcing": sourcing[i] if i < len(sourcing) else "",
                "size": size[i] if i < len(size) else "",
                "price": parse_price(price[i] if i < len(price) else ""),
                "unit_price": unit_price[i] if i < len(unit_price) else "",
                "origin": origin[i] if i < len(origin) else "",
            }))
    return products


def find_latest_two_tabs(sh, prefix):
    tabs = sorted(ws.title for ws in sh.worksheets() if ws.title.startswith(prefix))
    if len(tabs) < 2:
        return None, None
    return tabs[-2], tabs[-1]


def changed_fields(a, b):
    changes = []
    if a["category"] != b["category"]:
        changes.append(f"제품군표기: {a['category'] or '-'} → {b['category'] or '-'}")
    if a["sourcing"] != b["sourcing"]:
        changes.append(f"소싱: {a['sourcing'] or '-'} → {b['sourcing'] or '-'}")
    if a["price"] != "" and b["price"] != "" and a["price"] != b["price"]:
        changes.append(f"가격: {a['price']:,.0f} → {b['price']:,.0f}원")
    return changes


def build_summary(items_prev, items_curr, pairs, used_prev, used_curr):
    removed = [p for p in items_prev if p["id"] not in used_prev]
    added = [p for p in items_curr if p["id"] not in used_curr]

    for p in items_prev + items_curr:
        p["mid"] = classify_category(p["category"])
        p["top"] = category_top_level(p["mid"])

    def count_top(items):
        c = {}
        for it in items:
            c[it["top"]] = c.get(it["top"], 0) + 1
        return c

    def count_mid(items):
        c = {}
        for it in items:
            c[it["mid"]] = c.get(it["mid"], 0) + 1
        return c

    def count_by(items, key_fn):
        c = {}
        for it in items:
            k = key_fn(it) or "(빈값)"
            c[k] = c.get(k, 0) + 1
        return sorted(c.items(), key=lambda x: -x[1])

    def sourcing_count(items, key):
        return sum(1 for p in items if p["sourcing"] == key)

    t_prev, t_curr = count_top(items_prev), count_top(items_curr)
    m_prev, m_curr = count_mid(items_prev), count_mid(items_curr)
    mr, ma = count_mid(removed), count_mid(added)

    price_matched = [x for x in pairs if x["a"]["price"] not in ("", 0) and x["b"]["price"] not in ("", 0)]
    changes = sorted((x["b"]["price"] / x["a"]["price"]) - 1 for x in price_matched)
    up = sum(1 for c in changes if c > 0.001)
    down = sum(1 for c in changes if c < -0.001)
    median = changes[len(changes) // 2] if changes else 0

    data = {
        "총_SKU": {"이전": len(items_prev), "현재": len(items_curr)},
        "제외": len(removed), "신규": len(added),
        "대분류": {
            k: {"이전": t_prev.get(k, 0), "현재": t_curr.get(k, 0)} for k in ("식품", "비식품")
        },
        "중분류_이전_현재_제외_신규": {
            k: {"이전": m_prev.get(k, 0), "현재": m_curr.get(k, 0), "제외": mr.get(k, 0), "신규": ma.get(k, 0)}
            for k in sorted(set(list(m_prev) + list(m_curr)))
        },
        "소싱형태": {
            key: {"이전": sourcing_count(items_prev, key), "현재": sourcing_count(items_curr, key)}
            for key in sorted(set(p["sourcing"] for p in items_prev + items_curr if p["sourcing"]))
        },
        "가격변화": {
            "비교가능_매칭SKU": len(changes), "인상": up, "인하": down,
            "중앙값_퍼센트": round(median * 100, 1),
        },
        "제외_상위_제품군": count_by(removed, lambda p: p["category"])[:10],
        "신규_상위_제품군": count_by(added, lambda p: p["category"])[:10],
    }
    return data, removed, added


def generate_insight(data):
    if not gemini_client.is_configured():
        return "(GEMINI_API_KEY가 설정되어 있지 않아 AI 해석을 생략합니다. 위 표만 참고해주세요.)"
    prompt = f"""너는 리테일 데이터를 분석하는 애널리스트다. 아래는 코스트코/트레이더스 매장 사진 OCR로
수집한 상품 데이터를, 이전 달과 이번 달 두 시점으로 비교한 결과다 (JSON).

중요한 데이터 해석 유의사항 (반드시 반영):
- 이 데이터는 매대 전수조사가 아니라 담당자가 그때그때 촬영한 사진 기록이라, "제외/신규"가 실제
  단종/신규 도입이 아니라 촬영 커버리지 차이를 반영할 수 있다.
- 제품군 분류는 사람이 계속 채워나가는 목록이라, 새로운 제품군이 처음 등장한 경우 그 제품군의
  '제외/신규' 수치는 착시일 수 있다(예전엔 없던 세부 분류가 갑자기 생긴 것일 뿐일 수 있음).
- 주류(와인)는 빈티지에 따라 정상적으로 회전되는 카테고리라 제외/신규를 실제 단종으로 보기 어렵다.

데이터:
{json.dumps(data, ensure_ascii=False, indent=1)}

한국어로, 실제 리테일 애널리스트가 쓸 법한 통찰 있는 문체로 작성해라. 확실한 신호와 착시 가능성이
있는 신호를 구분해서 설명하고, 마지막엔 다음 조사 시 개선하면 좋을 점을 1~2가지 제안해라.
마크다운 기호(*, #, -) 없이 일반 텍스트로, ■ 기호로 소제목을 구분해서 700~1000자 정도로 작성해라."""
    return gemini_client.call(prompt, temperature=0.4)


def write_result_tab(sh, tab_name, prev_name, curr_name, data, insight):
    existing = [ws.title for ws in sh.worksheets()]
    if tab_name in existing:
        ws = sh.worksheet(tab_name)
        ws.clear()
    else:
        ws = sh.add_worksheet(title=tab_name, rows=200, cols=10)

    rows = [
        [f"SKU 변화 분석: {prev_name} → {curr_name}"],
        [],
        ["지표", "이전", "현재", "제외", "신규"],
        ["전체 SKU", data["총_SKU"]["이전"], data["총_SKU"]["현재"], data["제외"], data["신규"]],
    ]
    for k, v in data["대분류"].items():
        rows.append([k, v["이전"], v["현재"], "", ""])
    rows.append([])
    rows.append(["중분류", "이전", "현재", "제외", "신규"])
    for k, v in data["중분류_이전_현재_제외_신규"].items():
        rows.append([k, v["이전"], v["현재"], v["제외"], v["신규"]])
    rows.append([])
    rows.append(["소싱형태", "이전", "현재"])
    for k, v in data["소싱형태"].items():
        rows.append([k, v["이전"], v["현재"]])
    rows.append([])
    rows.append(["가격 비교 가능 SKU", "인상", "인하", "중앙값(%)"])
    pc = data["가격변화"]
    rows.append([pc["비교가능_매칭SKU"], pc["인상"], pc["인하"], pc["중앙값_퍼센트"]])
    rows.append([])
    rows.append(["제외 상위 제품군", "건수"])
    for k, v in data["제외_상위_제품군"]:
        rows.append([k, v])
    rows.append([])
    rows.append(["신규 상위 제품군", "건수"])
    for k, v in data["신규_상위_제품군"]:
        rows.append([k, v])
    rows.append([])
    rows.append(["종합 해석 (Gemini)"])
    rows.append([insight])

    ws.update(values=rows, range_name="A1")
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    return ws


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--retailer", choices=["costco", "traders"], required=True)
    parser.add_argument("--prev", help="이전 달 정리본 탭 이름 (생략하면 자동 감지)")
    parser.add_argument("--curr", help="이번 달 정리본 탭 이름 (생략하면 자동 감지)")
    args = parser.parse_args()

    if not ocr.SPREADSHEET_ID:
        print("SPREADSHEET_ID가 설정되어 있지 않습니다.")
        sys.exit(1)

    creds = ocr.get_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(ocr.SPREADSHEET_ID)

    prefix = (ocr.MONTHLY_CATEGORY_SHEET_NAME_COSTCO if args.retailer == "costco"
              else ocr.MONTHLY_CATEGORY_SHEET_NAME_TRADERS)

    prev_name, curr_name = args.prev, args.curr
    if not prev_name or not curr_name:
        prev_name, curr_name = find_latest_two_tabs(sh, prefix)
        if not prev_name:
            print(f"'{prefix}_YYYY-MM' 형태의 탭이 아직 2개월치 쌓이지 않아 비교할 수 없습니다. "
                  f"다음 달 데이터가 쌓이면 다시 실행해주세요.")
            sys.exit(0)
        print(f"자동 감지: '{prev_name}' vs '{curr_name}'")

    items_prev = read_all_products(sh.worksheet(prev_name), "p")
    items_curr = read_all_products(sh.worksheet(curr_name), "c")
    print(f"이전({prev_name}): {len(items_prev)}개, 현재({curr_name}): {len(items_curr)}개")

    pairs, used_prev, used_curr = pm.match(items_prev, items_curr)
    print(f"매칭 {len(pairs)}쌍, 제외 {len(items_prev)-len(used_prev)}개, 신규 {len(items_curr)-len(used_curr)}개")

    data, removed, added = build_summary(items_prev, items_curr, pairs, used_prev, used_curr)
    print("Gemini로 종합 해석을 생성합니다..." if gemini_client.is_configured() else "GEMINI_API_KEY 없음 - 해석 생략")
    insight = generate_insight(data)

    tab_name = f"{RESULT_TAB_PREFIX}{prev_name}_vs_{curr_name}"[:100]
    ws = write_result_tab(sh, tab_name, prev_name, curr_name, data, insight)
    print(f"'{tab_name}' 탭에 비교 결과를 기록했습니다.")


if __name__ == "__main__":
    main()
