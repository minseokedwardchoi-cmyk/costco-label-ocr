"""
정리본(제품군정리) 시트를 최신 파싱 코드 기준으로 재검토하고, 코드만으로
안전하게 고칠 수 있는 것만 자동으로 고치는 스크립트.

이미 처리된 상품의 원문텍스트(RAW의 "원문텍스트" 컬럼)는 그대로 두고, 그
텍스트를 지금 배포된 main.py 파싱 로직으로 다시 돌려서 두 가지만 한다:

  1. 정리본에서 비어있는 필드(규격/단량, 판매가, 단위단가, 셀링)를, 재파싱
     결과가 있으면 채운다. 이미 값이 있는 칸은 절대 덮어쓰지 않는다 - MD가
     수동으로 손본 값일 수도 있어서, 자동화가 건드릴 수 있는 건 "빈 칸"뿐이다.
  2. RAW의 핵심 필드(상품코드/제품명(한국어)/가격/제품명(영어)/중량/단가)는
     재파싱 결과가 다르면 최신값으로 덮어쓴다. RAW는 원문텍스트에서 파생되는
     레이어라 사람이 손대는 자리가 아니므로, "원문텍스트는 그대로인데 파싱
     코드가 좋아졌다"는 이 경우엔 덮어쓰는 게 맞다.
  3. is_back_label()로 다시 판별했을 때 뒷면으로 바뀐 상품(정리본에 잘못
     들어가 있던 경우)은 정리본에서 제거하고 RAW에 뒷면여부를 표시한다.
  4. 카드 행인데 아직 "정리본위치"가 비어있으면(이 컬럼이 생기기 전에 이미
     정리본에 기록됐던 과거 카드들) 지금 정리본 대조로 위치를 알아낸 김에
     같이 채워 넣는다. 그래야 그 카드의 뒷면 사진이 있을 때
     `main.sync_back_sourcing()`이 소싱형태/병입원산지를 그 칸에 반영할 수
     있다 - 이 스크립트는 그 함수도 이어서 호출해서, 새 사진 없이도
     소싱형태/병입원산지 소급 반영을 버튼 하나로 재실행할 수 있게 한다
     (원래 main.py의 run_once()는 새 사진이 하나도 없으면 이 단계 자체를
     안 돌기 때문에, 과거 데이터에 대해서는 이 스크립트가 유일한 재실행
     경로다).

새 사진을 읽거나(Azure) Drive에 접근하는 부분은 전혀 없다 - 이미 시트에 있는
텍스트만 다시 읽는, 비용이 들지 않는 재검토다. 그래서 재OCR로만 고칠 수 있는
문제(사진 자체가 잘못 읽힌 경우)는 이 스크립트로는 못 고치고, "확인 필요"
목록으로만 보고한다.

이름(상품명) 문자열로 RAW와 정리본을 대조한다. 흔한 드리프트 패턴 하나는
자동으로 보정한다: 이번 재파싱으로 "제품명 끝에 붙어있던 규격"이 새로
분리되면(예: "스카치브라이트 청소용브러쉬세트 4종" -> 제품명 "스카치브라이트
청소용브러쉬세트" + 중량 "4종") RAW의 제품명(한국어)이 정리본에 적힌 옛
이름과 더 이상 정확히 안 맞는다 - "RAW 제품명 + 중량" 조합으로 한 번 더
대조해서 이 경우를 잡고, 매칭되면 정리본의 상품명 칸도 최신 이름으로
동기화한다(상품명 칸은 RAW를 그대로 비추는 자리라 다른 4개 필드와 달리
값이 있어도 갱신한다). 그 외의 이름 불일치(예: 상품코드 오류처럼 이름
자체가 완전히 틀렸던 경우)는 여전히 수동 확인이 필요하다.
"""
import os
import re
import sys

import gspread

import main


DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

RETAILERS = [
    {
        "label": "코스트코",
        "retailer_key": "costco",
        "raw_sheet_name": main.SHEET_NAME,
        # 옛 단일 정리본 이름(월별 분리 이전 데이터)과, 지금 매달 새로 만드는
        # 정리본 탭의 기본 이름은 서로 다르다(main.py의 CATEGORY_SHEET_NAME_*
        # vs MONTHLY_CATEGORY_SHEET_NAME_* 참고) - 둘 다 훑어야 한다.
        "legacy_category_sheet_name": main.CATEGORY_SHEET_NAME_COSTCO,
        "monthly_category_sheet_name": main.MONTHLY_CATEGORY_SHEET_NAME_COSTCO,
        "parse_fn": main.parse_price_fields,
    },
    {
        "label": "트레이더스",
        "retailer_key": "traders",
        "raw_sheet_name": main.TRADERS_SHEET_NAME,
        "legacy_category_sheet_name": main.CATEGORY_SHEET_NAME_TRADERS,
        "monthly_category_sheet_name": main.MONTHLY_CATEGORY_SHEET_NAME_TRADERS,
        "parse_fn": main.parse_traders_fields,
    },
]

RAW_MIRROR_FIELDS = ["상품코드", "제품명(한국어)", "가격", "제품명(영어)", "중량", "단가"]


def _resolve_or_create_category_sheet(spreadsheet, title):
    """sync_back_sourcing()이 "정리본위치"에 적힌 탭을 열 때 쓴다. 정상적인
    경우엔 항상 이미 존재하는 탭이지만, 어떤 이유로든(예: 생성 도중 API
    오류로 RAW에는 위치가 기록됐는데 탭 자체는 못 만들어진 경우) 그 탭이
    없으면 자동화가 죽는 대신 빈 탭을 새로 만들고 경고를 남긴다 - 그 탭에
    있었어야 할 원래 데이터(상품명/가격 등)는 이미 유실된 상태이므로, 이
    경고가 뜨면 해당 카드들을 수동으로 확인해야 한다."""
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"경고: 정리본 탭 '{title}'을 찾을 수 없어 새로 만듭니다 - "
              "이 탭을 참조하던 카드들의 원래 데이터(상품명/가격 등)가 "
              "유실됐을 수 있으니 확인해주세요.")
        # 유실되기 전 탭은 상품이 많이 쌓여 열이 26개보다 훨씬 넓었을 가능성이
        # 높다(정리본위치가 26열 밖을 가리키는 경우가 실제로 있었다) - 되살린
        # 직후 또 grid limits 오류로 죽지 않도록 넉넉하게 만든다.
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=100)


def find_category_sheet_titles(spreadsheet, legacy_name, monthly_base_name):
    """main.py가 정리본을 촬영월별 탭으로 나누면서, 한 리테일러의 정리본이
    더 이상 탭 하나가 아니게 됐다. legacy_name 자체(월별 분리 이전 데이터가
    남아있는 옛 단일 탭, 예: "제품군정리(코스트코)")와, monthly_base_name
    뒤에 촬영월이 붙은 탭(예: "정리본(코스트코)_2026-07")을 전부 찾아
    돌려준다 - 이 스크립트가 존재하는 모든 달의 탭을 훑어야 과거 데이터를
    놓치지 않는다."""
    pattern = re.compile(
        rf"^{re.escape(legacy_name)}$|^{re.escape(monthly_base_name)}_\d{{4}}-\d{{2}}$"
    )
    return sorted(ws.title for ws in spreadsheet.worksheets() if pattern.match(ws.title))


def require_config():
    missing = []
    if not main.SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not main.GOOGLE_SERVICE_ACCOUNT_JSON and not os.path.exists(main.SERVICE_ACCOUNT_FILE):
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON (또는 service_account.json 파일)")
    if missing:
        print("다음 환경변수/파일이 설정되지 않았습니다:")
        for name in missing:
            print(f"  - {name}")
        sys.exit(1)


def review_retailer(spreadsheet, label, retailer_key, raw_sheet_name, category_sheet_titles, parse_fn):
    raw_ws = spreadsheet.worksheet(raw_sheet_name)

    # "정리본위치" 컬럼이 아직 없는 시트(이 기능이 생기기 전부터 있던 RAW
    # 탭)일 수 있으니, 읽기 전에 헤더를 최신 COLUMN_ORDER 기준으로 맞춘다.
    # 기존 데이터 행은 안 밀리게 안전하게 새 컬럼만 끼워 넣는다.
    if not DRY_RUN:
        main.ensure_header(raw_ws)

    raw_values = raw_ws.get_all_values()
    header = raw_values[0]
    idx = {h: i for i, h in enumerate(header)}
    # 같은 이름이 여러 번 나오면 첫 매치를 쓴다 (기존 진단 스크립트와 동일한 규칙).
    # 정리본이 촬영월별로 여러 탭에 나뉘어 있어도, RAW는 여전히 리테일러당
    # 하나뿐이라 이 매칭 풀은 모든 탭에 공통으로 쓴다.
    by_name = {}
    by_combined_name = {}
    for i, row in enumerate(raw_values[1:], start=2):
        name = row[idx["제품명(한국어)"]] if len(row) > idx["제품명(한국어)"] else ""
        if name and name not in by_name:
            by_name[name] = (i, row)
        weight = row[idx["중량"]] if len(row) > idx["중량"] else ""
        if name and weight:
            combined = f"{name} {weight}"
            if combined not in by_combined_name:
                by_combined_name[combined] = (i, row)

    raw_updates = []
    cat_update_count = 0
    backfilled_positions = 0
    filled_items = []          # (category, product_name, [채운 필드])
    renamed_items = []         # (category, old_name, new_name)
    reclassified_items = []    # (category, product_name)
    still_needs_review = []    # (category, product_name, [여전히 빈 필드])
    unmatched_items = []       # (category, product_name) - RAW에서 못 찾음

    # 촬영월별로 나뉜 정리본 탭을 한 장씩 훑는다 - 탭마다 열 번호가 별개라
    # col_letter 계산도 탭 단위로 새로 한다.
    for category_sheet_name in category_sheet_titles:
        cat_ws = spreadsheet.worksheet(category_sheet_name)
        cat_values = cat_ws.get_all_values()
        blocks, _ = main._scan_category_blocks(cat_values)
        cat_updates = []

        for category, block in blocks.items():
            field_rows = block["field_rows"]
            name_row_idx = field_rows["상품명"] - 1
            name_row = cat_values[name_row_idx] if name_row_idx < len(cat_values) else []
            n_cols = len(name_row)

            for col in range(1, n_cols):
                product_name = name_row[col] if col < len(name_row) else ""
                if not product_name:
                    continue

                match = by_name.get(product_name) or by_combined_name.get(product_name)
                if match is None:
                    unmatched_items.append((category, product_name))
                    continue
                row_num, row = match
                raw_text = row[idx["원문텍스트"]] if len(row) > idx["원문텍스트"] else ""
                lines = [l.strip() for l in raw_text.split("\n") if l.strip()]

                col_letter = main._col_letter(col + 1)

                if main.is_back_label(lines):
                    for lk in main.CATEGORY_FIELD_MAP:
                        r = field_rows[lk]
                        current = cat_values[r - 1][col] if col < len(cat_values[r - 1]) else ""
                        if current:
                            cat_updates.append({"range": f"{col_letter}{r}", "values": [[""]]})
                    back_flag_idx = idx.get("뒷면여부")
                    if back_flag_idx is not None and (len(row) <= back_flag_idx or row[back_flag_idx] != "뒷면"):
                        raw_updates.append({
                            "range": f"{main._col_letter(back_flag_idx + 1)}{row_num}",
                            "values": [["뒷면"]],
                        })
                    reclassified_items.append((category, product_name))
                    continue

                current_raw_name = row[idx["제품명(한국어)"]] if len(row) > idx["제품명(한국어)"] else ""
                if current_raw_name and current_raw_name != product_name:
                    name_cell_row = field_rows["상품명"]
                    cat_updates.append({
                        "range": f"{col_letter}{name_cell_row}",
                        "values": [[main._sheet_safe(current_raw_name)]],
                    })
                    renamed_items.append((category, product_name, current_raw_name))

                position_idx = idx.get("정리본위치")
                if position_idx is not None:
                    current_position = row[position_idx] if len(row) > position_idx else ""
                    if not current_position:
                        raw_updates.append({
                            "range": f"{main._col_letter(position_idx + 1)}{row_num}",
                            "values": [[f"{category_sheet_name}!{col_letter}{block['title_row']}"]],
                        })
                        backfilled_positions += 1

                recomputed = parse_fn(raw_text)
                # 코드 이후로 스캔을 앵커하는 건 "코드 -> 셀링" 순서인 코스트코
                # 레이아웃에서만 유효하다 - 트레이더스는 순서가 반대라서
                # (main.build_row_dict 참고) 앵커를 걸면 진짜 셀링 문구를 놓친다.
                anchor_code = recomputed.get("상품코드", "") if retailer_key != "traders" else ""
                recomputed["셀링포인트"] = main.extract_selling_points(raw_text, anchor_code)

                filled_fields = []
                still_missing = []
                for lk, key in main.CATEGORY_FIELD_MAP.items():
                    r = field_rows[lk]
                    current = cat_values[r - 1][col] if col < len(cat_values[r - 1]) else ""
                    if current.strip():
                        continue
                    new_val = str(recomputed.get(key, "")).strip()
                    if new_val:
                        cat_updates.append({"range": f"{col_letter}{r}", "values": [[main._sheet_safe(new_val)]]})
                        filled_fields.append(lk)
                    else:
                        still_missing.append(lk)

                for key in RAW_MIRROR_FIELDS:
                    if key not in idx:
                        continue
                    new_val = str(recomputed.get(key, "")).strip()
                    current = row[idx[key]] if len(row) > idx[key] else ""
                    if new_val and new_val != current:
                        raw_updates.append({
                            "range": f"{main._col_letter(idx[key] + 1)}{row_num}",
                            "values": [[main._sheet_safe(new_val)]],
                        })

                if filled_fields:
                    filled_items.append((category, product_name, filled_fields))
                if still_missing:
                    still_needs_review.append((category, product_name, still_missing))

        if not DRY_RUN and cat_updates:
            cat_ws.batch_update(cat_updates, value_input_option="USER_ENTERED")
        cat_update_count += len(cat_updates)

    sourcing_filled = None  # None = 실행 안 함(DRY RUN), 정수 = 실제로 채운 칸 수
    if not DRY_RUN:
        if raw_updates:
            raw_ws.batch_update(raw_updates, value_input_option="USER_ENTERED")
        # 정리본위치를 방금 다 써넣은 뒤에 호출해야, 이번에 새로 채운 위치의
        # 뒷면도 바로 소급 반영된다. main.py의 run_once()와 똑같은 함수를
        # 그대로 재사용한다 - 새 사진 없이도 전체 이력을 다시 훑는다. 정리본이
        # 촬영월별 탭으로 나뉘어 있으므로, 탭 제목으로 필요한 탭을 그때그때
        # 여는 resolver를 넘긴다.
        sourcing_filled = main.sync_back_sourcing(
            raw_ws, retailer_key, lambda title: _resolve_or_create_category_sheet(spreadsheet, title)
        )

    return {
        "label": label,
        "filled_items": filled_items,
        "renamed_items": renamed_items,
        "reclassified_items": reclassified_items,
        "still_needs_review": still_needs_review,
        "unmatched_items": unmatched_items,
        "raw_update_count": len(raw_updates),
        "cat_update_count": cat_update_count,
        "backfilled_positions": backfilled_positions,
        "sourcing_filled": sourcing_filled,
    }


def format_report(results):
    lines = []
    lines.append("# 정리본 자동 재검토 결과" + (" (DRY RUN - 실제로 쓰지 않음)" if DRY_RUN else ""))
    for r in results:
        lines.append(f"\n## {r['label']}")
        lines.append(
            f"- 정리본 셀 총 {r['cat_update_count']}건, RAW 셀 총 {r['raw_update_count']}건 갱신"
        )
        lines.append(f"- 빈 필드 자동 보정: {len(r['filled_items'])}건")
        for category, name, filled in r["filled_items"]:
            lines.append(f"  - [{category}] {name!r}: {', '.join(filled)} 채움")
        if r["renamed_items"]:
            lines.append(f"- 상품명 동기화(RAW 이름 변경 반영): {len(r['renamed_items'])}건")
            for category, old_name, new_name in r["renamed_items"]:
                lines.append(f"  - [{category}] {old_name!r} -> {new_name!r}")
        lines.append(f"- 뒷면으로 재분류(정리본에서 제거): {len(r['reclassified_items'])}건")
        for category, name in r["reclassified_items"]:
            lines.append(f"  - [{category}] {name!r}")
        lines.append(f"- 재파싱으로도 안 채워짐 (사진 재확인 필요): {len(r['still_needs_review'])}건")
        for category, name, missing in r["still_needs_review"]:
            lines.append(f"  - [{category}] {name!r}: {', '.join(missing)} 없음")
        if r["unmatched_items"]:
            lines.append(f"- RAW에서 이름이 안 맞아 대조 못 함: {len(r['unmatched_items'])}건")
            for category, name in r["unmatched_items"]:
                lines.append(f"  - [{category}] {name!r}")
        if r["backfilled_positions"]:
            lines.append(f"- 정리본위치 새로 채움(과거 카드 소급 태깅): {r['backfilled_positions']}건")
        if r["sourcing_filled"] is None:
            lines.append("- 소싱형태/병입원산지 소급 반영: DRY RUN이라 실행 안 함")
        else:
            lines.append(f"- 소싱형태/병입원산지 소급 반영: {r['sourcing_filled']}칸")
    return "\n".join(lines)


def run_once():
    require_config()
    creds = main.get_credentials()
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(main.SPREADSHEET_ID)

    results = []
    for r in RETAILERS:
        titles = find_category_sheet_titles(
            spreadsheet, r["legacy_category_sheet_name"], r["monthly_category_sheet_name"]
        )
        if not titles:
            print(f"경고: {r['label']} 제품군정리 탭을 찾을 수 없어 건너뜁니다.")
            continue
        results.append(review_retailer(
            spreadsheet,
            r["label"],
            r["retailer_key"],
            r["raw_sheet_name"],
            titles,
            r["parse_fn"],
        ))

    report = format_report(results)
    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(report + "\n")


if __name__ == "__main__":
    run_once()
