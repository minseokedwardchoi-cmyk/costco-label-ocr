import csv, re, sys
ART = re.compile(r'^\(\d+\s*pack\)|^wasabi peas$', re.I)
done=set()
with open('brand_verify.csv', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        k=row.get('key','').strip().lower()
        if k: done.add(k)
pend=[]
with open('_verify_todo.csv', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        b=row['brand'].strip(); bl=b.lower()
        if ART.match(bl): continue
        if any(d in bl or bl in d for d in done): continue
        pend.append((b,row['count'],row.get('retailers',''),row.get('sample_products','')))
n=int(sys.argv[1]) if len(sys.argv)>1 else 16
print("남은(아티팩트 제외):",len(pend))
for i,(b,c,r,s) in enumerate(pend[:n],1):
    print(f"{i:>2}.[{c:>2}] {b:<22}| {s[:70]}")
