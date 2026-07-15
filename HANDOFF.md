# 인수인계 문서 — 여기부터 읽으세요

이 저장소를 새로 맡으신 개발자용 진입점입니다. Claude Code에 이 저장소를 열고
이 파일부터 읽게 하면, 전체 그림과 지금 해야 할 일을 바로 파악할 수 있습니다.

## 이 시스템이 하는 일

```
Google Drive 폴더에 코스트코 매대 가격표 사진 업로드
        │
        ▼
대시보드의 "업데이트" 버튼 클릭  ← 대시보드 개발 = 당신의 몫
        │  (GitHub Actions REST API 호출)
        ▼
GitHub Actions가 이 저장소의 main.py 실행
        │
        ▼
Azure AI Vision OCR로 사진 텍스트 인식
        │
        ▼
상품코드 / 제품명(한국어) / 가격 / 제품명(영어) / 중량 / 단가 파싱
        │
        ▼
Google Sheets에 새 행으로 추가
```

식품/비식품 구분 없이 한 파이프라인으로 처리되고, 완전 무료로 동작하도록
설계되어 있습니다 (Azure Vision F0 무료 티어 + GitHub Actions 무료 사용량).

## 지금 상태: 이미 되어있는 것 vs 앞으로 할 일

### ✅ 이미 다 되어있음 (다시 안 만들어도 됨)

- Azure AI Vision 리소스 (무료 F0 티어) 생성 및 연동 완료
- Google Cloud 프로젝트 + 서비스 계정 + Drive/Sheets API 연동 완료
- 결과가 쌓이는 Google Sheet, 사진 업로드용 Google Drive 폴더 준비 완료
- 위 값들이 전부 이 GitHub 저장소의 **Secrets**에 등록되어 있고, OCR 파이프라인은
  정상 동작 중 (실제 사진으로 검증 완료 — 파싱 정확도 이슈들도 여러 차례 고쳐온 상태)
- GitHub Actions 워크플로(`.github/workflows/auto-ocr.yml`)가 외부에서 트리거
  가능하도록 `workflow_dispatch`로 설정되어 있음

즉 **Azure 계정, Google Cloud 계정을 새로 만들 필요가 없습니다.** 개인 계정이 아니라
이 프로젝트 전용 **공용(공동) 계정**으로 Azure/Google Cloud 리소스를 만들어서 GitHub
Secrets에 등록해뒀습니다 — 특정 개인 소유가 아니므로 담당자가 바뀌어도 리소스를
다시 만들거나 이전할 필요가 없습니다.

### 🔨 당신이 할 일

**대시보드의 "업데이트" 버튼을 누르면 위 파이프라인이 실행되도록 연동하는 것.**

구체적인 절차는 `DASHBOARD_INTEGRATION.md`에 다 정리되어 있습니다:
1. GitHub Personal Access Token(PAT) 발급 (이 저장소 소유자가 발급해서 전달해줘야 함
   — 아래 "필요한 것" 참고)
2. 대시보드 백엔드에서 그 토큰으로 `workflow_dispatch` API를 호출하는 코드 작성
3. (선택) 실행 상태를 대시보드에 표시하고 싶다면 상태 조회 API 연동

Claude Code에 아래처럼 시작하면 됩니다:

> "이 저장소(`costco-label-ocr`)의 `DASHBOARD_INTEGRATION.md`를 읽고, 우리
> 대시보드의 '업데이트' 버튼을 눌렀을 때 이 저장소의 GitHub Actions 워크플로를
> 트리거하도록 백엔드에 구현해줘. 토큰은 [저장소 소유자]가 따로 전달해줄 거야."

## 필요한 것 (계정/권한 체크리스트)

| 항목 | 누가 준비하나 | 비고 |
|---|---|---|
| Azure 계정 | ❌ 필요 없음 | 프로젝트 공용 계정으로 이미 만들어져 있고 Secrets에 키 등록됨 |
| Google Cloud 계정 | ❌ 필요 없음 | 프로젝트 공용 계정으로 이미 만들어져 있고 Secrets에 키 등록됨 |
| 이 GitHub 저장소 접근 | ✅ 필요 없음 | 저장소가 Public이라 링크만 있으면 누구나 열람/클론 가능 |
| GitHub Personal Access Token | ✅ 저장소 소유자가 발급 후 전달 | 대시보드 백엔드가 워크플로를 트리거하는 데만 사용 (Actions 권한만, Azure/Google 접근권한은 없음) |
| 대시보드 자체의 인프라(호스팅 등) | ✅ 당신 담당 | 이 저장소와는 무관 |

토큰 전달 방식은 카톡/이메일 평문 말고 1Password 공유, Bitwarden 공유 볼트 등
안전한 채널을 권장합니다 (자세한 내용 `DASHBOARD_INTEGRATION.md` 1-1절 참고).

## 문서 지도

- **이 파일 (`HANDOFF.md`)**: 전체 그림 + 지금 뭘 해야 하는지
- **`README.md`**: 시스템이 어떻게 동작하는지, 시트 컬럼 구조, Azure/Google Cloud를
  처음부터 설정하는 방법(이미 되어있지만 참고용/재구축용으로 남겨둠), 정확도 관련
  설계 결정들
- **`DASHBOARD_INTEGRATION.md`**: 대시보드 "업데이트" 버튼 연동 방법 (당신이 지금
  할 일의 실제 스펙)
- **`main.py`**: OCR/파싱 로직 본체. 주석에 각 파싱 규칙이 왜 그렇게 짜여있는지
  (실제로 겪었던 오인식 사례 기반) 설명이 달려있습니다.

## 계정 소유 구조 — 공용 계정 방식

Azure/Google Cloud 리소스는 저장소 소유자 개인 계정이 아니라, 이 프로젝트 전용으로
새로 만든 **공용 Gmail 계정**(및 그 계정으로 만든 Microsoft 계정) 아래에 있습니다.
그래서 담당자가 바뀌어도 "개인 계정 소유권 이전" 같은 절차가 필요 없고, 그냥
공용 계정 비밀번호(1Password/Bitwarden 등 공유 볼트에 보관)를 다음 담당자와
공유하면 됩니다.

- 리소스(Azure AI Vision, Google Cloud 프로젝트/서비스 계정, Google Sheet, Drive
  폴더)는 전부 이 공용 계정 아래에 있습니다.
- 지금 당신에게 필요한 건 GitHub PAT(워크플로 트리거용)뿐이고, 공용 계정
  비밀번호 자체는 몰라도 대시보드 연동 작업에는 지장이 없습니다.
- 만약 Azure/Google 콘솔에 직접 로그인해서 리소스를 들여다봐야 하는 상황이면
  (예: 사용량 확인, 리소스 재설정) 저장소 소유자에게 공용 계정 비밀번호 공유를
  요청하면 됩니다 — 개인 계정 이전 절차 없이 바로 로그인 가능합니다.
