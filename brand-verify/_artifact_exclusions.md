# 검증 제외 항목 (크롤러 아티팩트 / 제네릭) — brand_verify.csv에 별도 행 불필요

이 항목들은 `_verify_todo.csv`에 "브랜드"처럼 들어가 있으나 실제 브랜드명이 아니라
크롤러가 상품명 앞부분(포장수량 등)을 잘라낸 아티팩트이거나 상품유형 일반명입니다.
리포트의 브랜드 부분일치 매칭에서는 상품명 안의 실제 브랜드로 이미 매칭되므로,
아래 "실제 커버 브랜드"(brand_verify.csv에 이미 존재)가 이들을 커버합니다.
→ 별도 검증/행 추가 불필요. (junk key를 넣으면 리포트에서 오매칭 위험이 있어 의도적으로 제외)

| todo 아티팩트 | 실제 브랜드(커버) | brand_verify.csv 존재 |
|---|---|---|
| (2 pack)  | 상품명 내 실제 브랜드(예: Texas Pete / Great Value) | texas pete, great value ✓ |
| (12 pack) | Great Value 등 | great value ✓ |
| (3 pack)  | OREO 등 | oreo ✓ |
| (4 pack)  | Hungry Jack / Great Value 등 | hungry jack, great value ✓ |
| Wasabi Peas | Its Delish (해당 유통사 top10 상품) | its delish ✓ |
