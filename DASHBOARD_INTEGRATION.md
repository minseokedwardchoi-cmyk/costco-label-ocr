# 대시보드 "업데이트" 버튼 연동 가이드

이 저장소(`costco-label-ocr`)는 Google Drive에 올라온 사진을 Azure OCR로 읽어서
구글 시트에 정리하는 배치 스크립트입니다. 원래는 GitHub Actions 크론으로 15분마다
자동 실행했는데, 대시보드에 "업데이트" 버튼을 만들어서 **그 버튼을 누른 시점에만
실행**하는 방식으로 바꿨습니다. 그래서 크론은 없고, 외부에서 실행을 트리거하는
`workflow_dispatch` 트리거만 남아 있습니다 (`.github/workflows/auto-ocr.yml`).

버튼을 누르면 → 대시보드 백엔드가 GitHub REST API를 호출 → GitHub Actions가
`python main.py`를 실행 → Drive의 새 사진들을 OCR해서 구글 시트에 행을 추가.

## 1. 실행을 트리거하는 방법

**중요: 이 호출은 반드시 서버(백엔드)에서 해야 합니다.** 아래에서 쓰는 토큰은
이 저장소에 대한 쓰기 권한을 가진 비밀값이라, 브라우저/프론트엔드 JS 코드에
넣으면 누구나 개발자도구로 토큰을 훔쳐갈 수 있습니다.

### 1-1. GitHub Personal Access Token 발급

1. GitHub 로그인 → 우측 상단 프로필 → **Settings → Developer settings →
   Personal access tokens → Fine-grained tokens → Generate new token**
2. **Repository access**: "Only select repositories" → `costco-label-ocr` 선택
   (다른 저장소는 건드릴 필요 없으니 이렇게 범위를 최소화하세요)
3. **Permissions → Repository permissions → Actions**: **Read and write**로 설정
   (이거 하나면 충분합니다. 다른 권한은 안 줘도 됨)
4. 만료 기간 설정 후 생성 → 토큰 값 복사 (다시 못 봄, 안전한 곳에 저장)
5. 이 토큰을 대시보드 백엔드의 환경변수/시크릿으로 등록 (예: `GITHUB_PAT`)

### 1-2. 실행 트리거 API 호출

```
POST https://api.github.com/repos/minseokedwardchoi-cmyk/costco-label-ocr/actions/workflows/auto-ocr.yml/dispatches
Authorization: Bearer <GITHUB_PAT>
Accept: application/vnd.github+json
Content-Type: application/json

{"ref": "main"}
```

(선택) 특정 Drive 폴더만 테스트로 처리하고 싶으면 `inputs.costco_folder_id` /
`inputs.traders_folder_id`를 추가하세요 (둘 중 하나만 넣어도 됩니다):

```json
{"ref": "main", "inputs": {"costco_folder_id": "<테스트용 코스트코 Drive 폴더 ID>"}}
```

비워두거나 아예 안 보내면 평소처럼 기본 폴더(`DRIVE_FOLDER_ID_COSTCO` /
`DRIVE_FOLDER_ID_TRADERS` 시크릿)를 사용합니다.

**성공하면 HTTP 204(No Content)**를 돌려줍니다 - 응답 본문은 없습니다.
이 호출은 "실행을 큐에 넣는다"는 뜻이고, **OCR 처리가 끝날 때까지 기다려주지
않습니다** (사진이 많으면 실제 처리에 몇 분~1시간 이상 걸릴 수 있음). 그래서
"업데이트" 버튼을 누르면 "처리를 시작했어요, 잠시 후 시트에서 확인해주세요"
정도로 안내하고, 완료 여부까지 표시하고 싶으면 1-3의 상태 조회 API를 쓰세요.

curl 예시:
```bash
curl -X POST \
  -H "Authorization: Bearer $GITHUB_PAT" \
  -H "Accept: application/vnd.github+json" \
  https://api.github.com/repos/minseokedwardchoi-cmyk/costco-label-ocr/actions/workflows/auto-ocr.yml/dispatches \
  -d '{"ref":"main"}'
```

Node.js(fetch) 예시:
```js
const res = await fetch(
  "https://api.github.com/repos/minseokedwardchoi-cmyk/costco-label-ocr/actions/workflows/auto-ocr.yml/dispatches",
  {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.GITHUB_PAT}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref: "main" }),
  }
);
if (res.status !== 204) {
  throw new Error(`트리거 실패: ${res.status} ${await res.text()}`);
}
```

### 1-3. (선택) 진행 상태 조회

버튼을 누른 뒤 "완료됨" 같은 상태를 보여주고 싶으면, 최근 실행 목록을 폴링하세요:

```
GET https://api.github.com/repos/minseokedwardchoi-cmyk/costco-label-ocr/actions/workflows/auto-ocr.yml/runs?per_page=1
Authorization: Bearer <GITHUB_PAT>
```

응답의 `workflow_runs[0].status`가 `"queued"` → `"in_progress"` → `"completed"`로
바뀌고, 완료되면 `conclusion`이 `"success"` 또는 `"failure"`입니다. 방금 누른
버튼에 대한 실행인지 확실히 구분하려면, 트리거 직전 시각을 기록해뒀다가
`run.created_at`이 그 이후인 것 중 최신 것을 보면 됩니다.

## 2. 동시 실행 방지

버튼을 여러 번 누르거나, 이전 실행이 아직 안 끝났는데 또 누르는 경우를 대비해
워크플로에 `concurrency` 설정이 이미 되어 있습니다 - 이전 실행이 끝날 때까지
다음 실행은 자동으로 대기하고, 같은 파일을 중복 처리하지 않습니다. 대시보드
쪽에서 별도로 막을 필요는 없지만, 사용자 경험상 버튼을 누른 직후 잠깐
비활성화해두는 정도는 권장합니다.

## 3. 새 사진이 없을 때

`main.py`는 Drive 폴더를 조회해서 구글 시트에 이미 기록된 파일ID와 대조하는
방식이라, 새 사진이 없으면 "처리할 새 이미지가 없습니다"라는 로그만 남기고
몇 초 안에 끝납니다. 실패가 아니라 정상 종료입니다.

## 4. 참고 - 필요한 GitHub Secrets

실행 자체에 필요한 값들은 이미 저장소 **Settings → Secrets and variables →
Actions**에 등록되어 있습니다 (`DRIVE_FOLDER_ID_COSTCO`, `DRIVE_FOLDER_ID_TRADERS`,
`SPREADSHEET_ID`, `SHEET_NAME`, `AZURE_VISION_ENDPOINT`, `AZURE_VISION_KEY`,
`GOOGLE_SERVICE_ACCOUNT_JSON`).
대시보드 쪽에서는 이 값들을 알 필요가 없고, 그냥 위 1-2의 트리거만 호출하면
됩니다. 자세한 시스템 구조는 `README.md` 참고.
