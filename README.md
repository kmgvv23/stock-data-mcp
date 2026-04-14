# stock-data-mcp

> KRX·DART·한국은행 ECOS 데이터를 AI 에이전트에 연결하는 한국 금융 데이터 MCP 서버

KRX(한국거래소), Open DART(금융감독원), 한국은행 ECOS 세 가지 공식 API를 통합한 한국 금융 데이터 MCP 서버입니다.
Claude 등 AI 에이전트가 실시간 주식 시세, 기업 공시, 거시경제 지표에 접근할 수 있게 해줍니다.

## 제공 기능 (총 52개 툴)

| 데이터 소스 | 주요 기능 |
|---|---|
| **KRX** (19개) | 종목 시세/거래량, 시가총액, ETF, 투자자 동향, 공매도 |
| **Open DART** (19개) | 재무제표, 기업공시, 주주·임원 정보, 배당 |
| **한국은행 ECOS** (14개) | 기준금리, 환율, CPI, GDP, M2 통화량 |

## 필요한 API 키

| 환경변수 | 발급처 |
|---|---|
| `KRX_API_KEY` | https://openapi.krx.co.kr |
| `DART_API_KEY` | https://opendart.fss.or.kr |
| `ECOS_API_KEY` | https://ecos.bok.or.kr/api |

`.env.example`을 복사해 `.env`로 저장 후 키를 입력하세요.

## 설치 및 실행

```bash
# 패키지 설치
uv sync

# stdio 모드 (Claude Desktop 등 로컬 클라이언트)
uv run stock-mcp

# HTTP 모드 (원격 클라이언트)
uv run stock-mcp --http
```

## Claude Desktop 연결 설정

`claude_desktop_config.json`에 추가:

```json
{
  "mcpServers": {
    "stock-data-mcp": {
      "command": "uv",
      "args": ["run", "stock-mcp"],
      "env": {
        "KRX_API_KEY": "your_key",
        "DART_API_KEY": "your_key",
        "ECOS_API_KEY": "your_key"
      }
    }
  }
}
```

## 배포 (Railway)

Railway에 배포되어 있습니다. Dockerfile도 포함되어 있어 모든 컨테이너 환경에서 실행 가능합니다.

## MCP 지원 LLM 연결 (ChatGPT 등)

MCP를 지원하는 AI 서비스에 아래 URL 하나만 입력하면 바로 연결됩니다.

```
https://stock-data-mcp-production.up.railway.app/mcp
```

**ChatGPT 연결 방법:**
1. ChatGPT → 설정 → 연결된 앱 → **MCP 서버 추가**
2. 위 URL 입력
3. 연결 완료 후 한국 주식·공시·거시경제 데이터 바로 조회 가능

## License

MIT
