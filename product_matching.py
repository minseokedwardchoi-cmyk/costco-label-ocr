"""두 시점(예: 이번 달/저번 달)의 상품 목록에서 동일 SKU를 찾아 연결하는 엔진.

상품코드가 없어 상품명만으로 비교해야 하는 상황을 위해 만들어졌다 - 표기 차이
(브랜드 한글/영문 중복, 철자 오타, 규격·괄호 차이)로 서로 다른 문자열이 된
동일 상품을 정규화 + 유사도 비교로 최대한 같은 SKU로 묶는다. 브랜드 별칭·오타
목록은 실제 마주친 사례를 계속 추가해나가는 살아있는 목록이다(아래 두 목록
참고) - 새로운 브랜드의 한글/영문 중복 표기나 OCR 오타를 발견하면 그때그때
추가한다.

이 모듈은 상품명 문자열만 다루고, 시트 읽기/쓰기나 Gemini 호출과는 무관하다.
"""
import math
import re
import unicodedata

MANUAL_CANONICAL_ALIASES = {
    # 실제로 확인된 동일 SKU인데 표준 키가 갈리는 경우 여기에 "표준키 A": "표준키 B"
    # 형태로 추가한다. build_comparison_analysis.mjs(코스트코 2025/2026 엑셀 비교
    # 작업)에서 찾은 항목들은 그 데이터셋에만 해당하는 구체적 상품명이라 여기에는
    # 옮기지 않았다 - 이 목록은 이 파이프라인에서 새로 마주치는 사례로 채워나간다.
}

# 한글 표기와 영문 표기가 같은 브랜드를 가리키는 경우 영문 표준형으로 통일한다.
# 새 브랜드의 "영문 BRAND 한글브랜드" 중복 표기를 발견하면 여기에 한 줄 추가.
BRAND_ALIAS_PAIRS = [
    ("커클랜드시그니처", "KIRKLAND"), ("커클랜드시그니춰", "KIRKLAND"), ("커클랜드시그니쳐", "KIRKLAND"),
    ("센소다인", "SENSODYNE"), ("세타필", "CETAPHIL"), ("피지오겔", "PHYSIOGEL"),
    ("비비고", "BIBIGO"), ("디펜드", "DEPEND"), ("이지듀", "EASYDEW"), ("센카", "SENKA"),
    ("라운드랩", "ROUND LAB"), ("콜게이트", "COLGATE"), ("스팸", "SPAM"), ("아이오페", "IOPE"),
    ("토리든", "TORRIDEN"), ("유리아주", "URIAGE"), ("유리아쥬", "URIAGE"), ("웨이더", "WEIDER"),
    ("테라로사", "TERAROSA"), ("바이오더마", "BIODERMA"), ("일리윤", "ILLIYOON"),
    ("뉴케어", "NUCARE"), ("메이지", "MEIJI"), ("켈로그", "KELLOGG'S"), ("스키틀즈", "SKITTLES"),
    ("트롤리", "TROLLI"), ("포스트", "POST"), ("이디야", "EDIYA"), ("델라미코", "DELAMICO"),
    ("말돈", "MALDON"), ("큐피", "KEWPIE"), ("스타벅스", "STARBUCKS"), ("알티스트", "ALTIST"),
    ("스키피", "SKIPPY"), ("칼레오", "CALEO"), ("듀라셀", "DURACELL"), ("팬틴", "PANTENE"),
    ("헤라", "HERA"), ("오휘", "O HUI"), ("아로마티카", "AROMATICA"), ("킨더", "KINDER"),
    ("킷캣", "KITKAT"), ("잔슨빌", "JOHNSONVILLE"), ("수려한", "SOORYEHAN"),
]

BRAND_DEDUPE_TOKENS = sorted(
    {en.replace(" ", "").replace(".", "").replace("'", "") for _, en in BRAND_ALIAS_PAIRS},
    key=len, reverse=True,
)

# OCR 오타/표기 변형 통일. 실제 상품명에서 확인된 것만 추가한다.
SPELLING_FIX_PAIRS = [
    ("파마지아노", "파르미지아노"), ("그라다나파다노", "그라나파다노"),
]

COUNTRY_PAREN_RE = re.compile(
    r"\([^)]*(?:미국|프랑스|이탈리아|영국|스페인|벨기에|독일|호주|캐나다|일본|중국|네덜란드|그리스|뉴질랜드)[^)]*\)"
)
CODE_PREFIX_RE = re.compile(
    r"^(?:[A-Z]{1,3}\s+)?(?=[A-Z0-9]*\d)[A-Z0-9]{3,8}-(?=[A-Z0-9]*\d)[A-Z0-9]{3,10}\s+"
)
SIZE_MULT_RE = re.compile(
    r"\d+(?:[.,]\d+)?\s*(?:KG|G|MG|L|ML|CM|MM|EA|CT|개입|개|팩|입)?\s*[*X×]\s*\d+(?:\s*(?:EA|CT|개입|개|팩|입))?",
    re.IGNORECASE,
)
SIZE_SINGLE_RE = re.compile(
    r"\d+(?:[.,]\d+)?\s*(?:KG|G|MG|L|ML|CM|MM|EA|CT|개입|개|팩|입)(?=\s|$|[*X×,]|또는)",
    re.IGNORECASE,
)
X_NUM_RE = re.compile(r"\bX\s*\d+\b", re.IGNORECASE | re.ASCII)
NUM_X_RE = re.compile(r"\b\d+\s*X\b", re.IGNORECASE | re.ASCII)
TRAILING_ENG_REPEAT_RE = re.compile(r"\s+[A-Z][A-Z0-9.'\-]*(?:\s+[A-Z][A-Z0-9.'\-]*)+\s*$")
HANGUL_RE = re.compile(r"[가-힣]")
PAREN_RE = re.compile(r"\([^)]*\)")
NON_ALNUM_KO_RE = re.compile(r"[^0-9A-Z가-힣]+")
WORDS_SIZE_RE = re.compile(r"\d+(?:[.,]\d+)?\s*(?:KG|G|MG|L|ML|EA|CT|개입|개|팩|입)\b", re.IGNORECASE | re.ASCII)


def text(v):
    if v is None:
        return ""
    return str(v).strip()


def number(v):
    if isinstance(v, (int, float)):
        return v
    s = re.sub(r"[^\d.\-]", "", text(v))
    return float(s) if s else ""


def normalize_name(name, strip_size=False):
    s = text(name)
    s = unicodedata.normalize("NFKC", s).upper()
    s = re.sub(r"^\*\s*", "", s)
    for ko, en in BRAND_ALIAS_PAIRS:
        s = s.replace(ko, en)
    for wrong, right in SPELLING_FIX_PAIRS:
        s = s.replace(wrong, right)
    s = COUNTRY_PAREN_RE.sub(" ", s)
    s = CODE_PREFIX_RE.sub(" ", s)

    if strip_size:
        s = PAREN_RE.sub(" ", s)
        s = SIZE_MULT_RE.sub(" ", s)
        s = SIZE_SINGLE_RE.sub(" ", s)
        s = X_NUM_RE.sub(" ", s)
        s = NUM_X_RE.sub(" ", s)
        if HANGUL_RE.search(s):
            s = TRAILING_ENG_REPEAT_RE.sub(" ", s)

    compact = NON_ALNUM_KO_RE.sub("", s)
    for token in BRAND_DEDUPE_TOKENS:
        compact = compact.replace(token + token, token)

    if strip_size:
        return MANUAL_CANONICAL_ALIASES.get(compact, compact)
    return compact


def words(name):
    s = text(name)
    s = unicodedata.normalize("NFKC", s).upper()
    s = re.sub(r"^\*\s*", "", s)
    s = PAREN_RE.sub(" ", s)
    s = WORDS_SIZE_RE.sub(" ", s)
    parts = re.split(r"[^0-9A-Z가-힣]+", s)
    return [w for w in parts if len(w) >= 2]


def dice(a, b):
    if not a or not b:
        return 0
    if a == b:
        return 1
    def grams(s):
        out = {}
        for i in range(len(s) - 1):
            g = s[i:i + 2]
            out[g] = out.get(g, 0) + 1
        return out
    ga, gb = grams(a), grams(b)
    overlap = sum(min(c, gb.get(g, 0)) for g, c in ga.items())
    total = sum(ga.values()) + sum(gb.values())
    return (2 * overlap) / total if total else 0


def token_jaccard(a, b):
    sa, sb = set(words(a)), set(words(b))
    if not sa or not sb:
        return 0
    return len(sa & sb) / len(sa | sb)


def price_score(a_price, b_price):
    if not a_price or not b_price:
        return 0.5
    try:
        return max(0, 1 - abs(math.log(b_price / a_price)) / math.log(2))
    except (ValueError, ZeroDivisionError):
        return 0.5


def prepare(product):
    """product dict에 norm/base 표준 키를 추가한다. product는 최소한
    {"name": str, "price": number-or-""} 를 가지고 있어야 한다."""
    product["norm"] = normalize_name(product["name"])
    product["base"] = normalize_name(product["name"], strip_size=True)
    return product


def match(items_a, items_b):
    """items_a/items_b: prepare()를 거친 product dict 리스트, 각 dict는 고유
    "id" 키를 가지고 있어야 한다(예: f"a-{i}"). 동일 SKU로 판단된 (a, b, 신뢰도,
    방법) 튜플의 리스트와, 매칭에 쓰인 a/b id 집합을 돌려준다."""
    used_a, used_b = set(), set()
    pairs = []

    def add_pair(a, b, confidence, method):
        used_a.add(a["id"])
        used_b.add(b["id"])
        pairs.append({"a": a, "b": b, "confidence": confidence, "method": method})

    def pair_unique_by(key):
        map_a, map_b = {}, {}
        for p in items_a:
            if p["id"] in used_a or not p[key]:
                continue
            map_a.setdefault(p[key], []).append(p)
        for p in items_b:
            if p["id"] in used_b or not p[key]:
                continue
            map_b.setdefault(p[key], []).append(p)
        for k, alist in map_a.items():
            blist = map_b.get(k)
            if len(alist) == 1 and blist and len(blist) == 1:
                conf = "높음" if key == "norm" else "중간"
                method = "정규화 상품명 일치" if key == "norm" else "규격 제거 상품명 일치"
                add_pair(alist[0], blist[0], conf, method)

    pair_unique_by("norm")
    pair_unique_by("base")

    # 같은 표준 키가 여러 개인 경우, 상품명·가격 유사도가 가장 가까운 조합부터 1:1 연결.
    groups_a, groups_b = {}, {}
    for p in items_a:
        if p["id"] not in used_a and p["base"]:
            groups_a.setdefault(p["base"], []).append(p)
    for p in items_b:
        if p["id"] not in used_b and p["base"]:
            groups_b.setdefault(p["base"], []).append(p)
    for base, alist in groups_a.items():
        blist = groups_b.get(base)
        if not blist:
            continue
        candidates = []
        for a in alist:
            for b in blist:
                score = 0.75 * dice(a["norm"], b["norm"]) + 0.25 * price_score(a["price"], b["price"])
                candidates.append({"a": a, "b": b, "score": score})
        candidates.sort(key=lambda x: -x["score"])
        for c in candidates:
            if c["a"]["id"] in used_a or c["b"]["id"] in used_b:
                continue
            add_pair(c["a"], c["b"], "높음" if c["score"] >= 0.82 else "중간", "중량·괄호 제거 기준명 일치")

    # 표준 키가 다르면 앞 3단어 블로킹 + 유사도 점수로 후보를 찾는다.
    left_a = [p for p in items_a if p["id"] not in used_a]
    left_b = [p for p in items_b if p["id"] not in used_b]
    block_b = {}
    for b in left_b:
        for k in list(dict.fromkeys(words(b["name"])[:3])):
            block_b.setdefault(k, []).append(b)
    candidates = []
    for a in left_a:
        pool = {}
        for k in words(a["name"])[:3]:
            for b in block_b.get(k, []):
                pool[b["id"]] = b
        for b in pool.values():
            d = dice(a["base"], b["base"])
            j = token_jaccard(a["name"], b["name"])
            prefix_bonus = 0.08 if a["base"][:5] == b["base"][:5] and a["base"][:5] else 0
            score = min(1, 0.72 * d + 0.28 * j + prefix_bonus)
            if score >= 0.70:
                candidates.append({"a": a, "b": b, "score": score})
    candidates.sort(key=lambda x: -x["score"])
    for c in candidates:
        if c["a"]["id"] in used_a or c["b"]["id"] in used_b:
            continue
        conf = "중간" if c["score"] >= 0.86 else "검토 필요"
        add_pair(c["a"], c["b"], conf, f"유사 상품명 {round(c['score']*100)}%")

    # 브랜드 첫 단어 + 숫자 규격 + 가격이 함께 근접하면 마지막으로 연결 (항상 검토 필요).
    left_a = [p for p in items_a if p["id"] not in used_a]
    left_b = [p for p in items_b if p["id"] not in used_b]

    def brand_key(name):
        s = unicodedata.normalize("NFKC", text(name)).upper()
        s = re.sub(r"[^0-9A-Z가-힣]+", " ", s).strip()
        parts = s.split()
        return parts[0] if parts else ""

    def numeric_tokens(name):
        return set(x.replace(",", "") for x in re.findall(r"\d+(?:[.,]\d+)?", text(name)))

    brand_groups_b = {}
    for b in left_b:
        key = brand_key(b["name"])
        if len(key) >= 3:
            brand_groups_b.setdefault(key, []).append(b)
    brand_candidates = []
    for a in left_a:
        key = brand_key(a["name"])
        if len(key) < 3:
            continue
        for b in brand_groups_b.get(key, []):
            base_score = dice(a["base"], b["base"])
            full_score = dice(a["norm"], b["norm"])
            num_a, num_b = numeric_tokens(a["name"]), numeric_tokens(b["name"])
            numeric_score = len(num_a & num_b) / len(num_a | num_b) if (num_a and num_b) else 0
            score = 0.42 * base_score + 0.20 * full_score + 0.20 * numeric_score + 0.18 * price_score(a["price"], b["price"])
            if (base_score >= 0.43 or numeric_score >= 0.5) and score >= 0.52:
                brand_candidates.append({"a": a, "b": b, "score": score})
    brand_candidates.sort(key=lambda x: -x["score"])
    for c in brand_candidates:
        if c["a"]["id"] in used_a or c["b"]["id"] in used_b:
            continue
        add_pair(c["a"], c["b"], "검토 필요", f"브랜드·규격 교차매칭 {round(c['score']*100)}%")

    return pairs, used_a, used_b
