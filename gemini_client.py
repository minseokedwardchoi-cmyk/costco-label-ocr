"""Gemini API 호출 래퍼. 분류 보조와 월간 비교 리포트의 해석 문단 생성에 쓰인다.

GEMINI_API_KEY 환경변수가 없으면 이 모듈을 쓰는 스크립트들은 조용히 건너뛰도록
설계되어 있다(require_key=False로 존재 여부만 확인) - AI 보조 기능이 빠져도
나머지 파이프라인(OCR, 정리본 정리)은 이 모듈과 무관하게 계속 동작해야 한다.
"""
import json
import os
import time
import urllib.error
import urllib.request

MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"


def is_configured() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY"))


def call(prompt: str, response_schema=None, temperature: float = 0.2, max_retries: int = 4):
    """prompt를 Gemini에 보내고 응답을 돌려준다. response_schema를 주면 그 스키마에
    맞는 JSON(파싱된 파이썬 객체)을, 안 주면 순수 텍스트를 돌려준다."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")

    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature},
    }
    if response_schema is not None:
        body["generationConfig"]["responseMimeType"] = "application/json"
        body["generationConfig"]["responseSchema"] = response_schema

    url = f"{ENDPOINT}?key={api_key}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")

    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            text = payload["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(text) if response_schema is not None else text
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            last_err = f"HTTP {e.code}: {body_text[:300]}"
            if e.code in (429, 500, 503):
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(last_err)
        except Exception as e:
            last_err = str(e)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Gemini 호출이 {max_retries}회 재시도 후에도 실패했습니다: {last_err}")
