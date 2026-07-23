import csv, sys, os
# 사용법: 표준입력으로 key|리콜|품질표시|법적평판|5년이내|비고  (조사일 자동)
FN='brand_verify.csv'
today='2026-07-23'
# 기존 key(소문자) 로드
done=set()
with open(FN, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        k=row.get('key','').strip().lower()
        if k: done.add(k)
added=[]; skipped=[]
with open(FN,'a',newline='',encoding='utf-8-sig') as f:
    w=csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    for line in sys.stdin:
        line=line.rstrip('\n')
        if not line.strip(): continue
        parts=line.split('|')
        if len(parts)!=6:
            print("FORMAT ERR:",line); continue
        key,ri,pu,ju,yr,memo=[p.strip() for p in parts]
        kl=key.lower()
        if kl in done:
            skipped.append(key); continue
        w.writerow([key,ri,pu,ju,yr,memo,today])
        done.add(kl); added.append(key)
print("추가:",len(added),added)
if skipped: print("중복스킵:",skipped)
