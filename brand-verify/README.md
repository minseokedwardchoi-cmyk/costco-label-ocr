# 브랜드 이슈검증 작업 (월마트/샘스클럽/아마존/이온몰)

이 폴더는 costco-label-ocr OCR 코드와 **별개인 브랜드 이슈검증 배치작업**의 작업 상태입니다.
컨테이너 회수 시에도 진행상황이 보존되도록 이 작업 브랜치에 스냅샷으로 커밋해 둡니다.

- `_verify_todo.csv` — 검증 대상 622개 브랜드(입력, count 내림차순)
- `brand_verify.csv` — 검증 결과 누적(출력). **진행상황 = 이 파일 자체**
- `_queue.py` — 남은 대기열 출력(완료+아티팩트 제외)
- `_append.py` — 안전 CSV append(csv.writer, 중복 key 자동 스킵)
- `_artifact_exclusions.md` — 크롤러 아티팩트((N pack)/제네릭) 제외 근거
- `PROMPT_*.md` — 원본 실행지침(파트1~3)

원본 크롤링 xlsx(아마존 15MB/이온 1.5MB)는 파트2·3용이며 용량 문제로 미포함 —
필요 시 원본 zip에서 복원.

## 재개 방법
`python3 _queue.py 20` 으로 다음 대기 브랜드 확인 → 각 브랜드 웹검색(recall/lawsuit/mislabeling)
→ `PROMPT_브랜드검증_실행지침.md` 스키마대로 `_append.py`로 한 건씩 append.
