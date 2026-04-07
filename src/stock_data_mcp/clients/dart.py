"""
Open DART API client (금융감독원 전자공시시스템).
Base URL: https://opendart.fss.or.kr/api
Auth: crtfc_key query parameter
"""

import io
import zipfile

import httpx

from stock_data_mcp.config import get_settings

_BASE_URL = "https://opendart.fss.or.kr/api"


class DARTClient:
    def __init__(self) -> None:
        self._api_key = get_settings().dart_api_key
        self._client = httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0)

    def _auth(self) -> dict:
        return {"crtfc_key": self._api_key}

    # ── 기업 코드 ─────────────────────────────────────────────────────────

    async def get_corp_codes(
        self,
        query: str = "",
        stock_code: str = "",
        listed_only: bool = True,
    ) -> list[dict]:
        """기업 코드 목록 (ZIP → XML 파싱).

        Args:
            query: 회사명 검색어 (부분 일치, 예: '삼성')
            stock_code: 거래소 종목코드 (정확 일치, 예: '005930')
            listed_only: True이면 상장법인만 반환 (기본값)
        """
        import xml.etree.ElementTree as ET

        resp = await self._client.get("/corpCode.xml", params=self._auth())
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_bytes = zf.read("CORPCODE.xml")

        root = ET.fromstring(xml_bytes)
        results = []
        for item in root.findall("list"):
            sc = item.findtext("stock_code", "").strip()
            name = item.findtext("corp_name", "")

            if listed_only and not sc:
                continue
            if stock_code and sc != stock_code:
                continue
            if query and query not in name:
                continue

            results.append({
                "corp_code": item.findtext("corp_code", ""),
                "corp_name": name,
                "stock_code": sc,
                "modify_date": item.findtext("modify_date", ""),
            })

        return results

    # ── 기업 개황 ─────────────────────────────────────────────────────────

    async def get_company_info(self, corp_code: str) -> dict:
        """기업 개황 (회사명, 대표자, 업종, 주소, 홈페이지 등)."""
        resp = await self._client.get(
            "/company.json", params={**self._auth(), "corp_code": corp_code}
        )
        resp.raise_for_status()
        return resp.json()

    # ── 공시 ──────────────────────────────────────────────────────────────

    async def search_disclosures(
        self,
        corp_code: str = "",
        start_date: str = "",
        end_date: str = "",
        disclosure_type: str = "",
        page_no: int = 1,
        page_count: int = 40,
    ) -> dict:
        """공시 목록 검색.
        disclosure_type: A=정기공시, B=주요사항보고, C=발행공시, D=지분공시,
                         E=기타공시, F=외감법인, G=펀드공시, H=자산유동화
        """
        params = {
            **self._auth(),
            "corp_code": corp_code,
            "bgn_de": start_date,
            "end_de": end_date,
            "pblntf_ty": disclosure_type,
            "page_no": page_no,
            "page_count": page_count,
        }
        resp = await self._client.get("/list.json", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_disclosure_document(self, rcept_no: str) -> dict:
        """공시 원문 ZIP(document.xml)을 다운로드해 파일 목록을 반환합니다."""
        resp = await self._client.get(
            "/document.xml", params={**self._auth(), "rcept_no": rcept_no}
        )
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            file_list = zf.namelist()

        return {
            "status": "000",
            "rcept_no": rcept_no,
            "files": file_list,
        }

    # ── 재무제표 ──────────────────────────────────────────────────────────

    async def get_financial_statements(
        self,
        corp_code: str,
        business_year: str,
        report_code: str,
        fs_type: str = "CFS",
    ) -> dict:
        """전체 재무제표 (BS, IS, CF).
        report_code: 11013=1분기, 11012=반기, 11014=3분기, 11011=사업보고서
        fs_type: CFS=연결, OFS=별도
        """
        resp = await self._client.get(
            "/fnlttSinglAcntAll.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
                "fs_div": fs_type,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        # status 013 = 조회된 데이터 없음 (은행/증권/보험 등 금융업 회사에서 발생)
        if data.get("status") == "013":
            disclosures = await self.search_disclosures(
                corp_code=corp_code,
                start_date=f"{business_year}0101",
                end_date=f"{business_year}1231",
                disclosure_type="A",  # 정기공시
                page_count=5,
            )
            return {
                "status": "013",
                "message": (
                    "재무제표 데이터를 조회할 수 없습니다. "
                    "은행·증권·보험 등 금융업 회사는 DART 표준 재무제표 API(XBRL)에서 "
                    "조회가 불가합니다. 아래 공시 문서를 직접 확인하세요."
                ),
                "suggestion": (
                    "dart_get_disclosure_document(rcept_no)를 사용해 "
                    "실제 공시 원문 문서 목록을 조회한 뒤 재무제표 파일을 확인하세요."
                ),
                "related_disclosures": disclosures.get("list", []),
            }

        return data

    async def get_financial_statements_multi(
        self,
        corp_codes: str,
        business_year: str,
        report_code: str,
        fs_type: str = "CFS",
    ) -> dict:
        """다중 기업 재무제표 (최대 100개 corp_code 콤마 구분)."""
        resp = await self._client.get(
            "/fnlttMultiAcnt.json",
            params={
                **self._auth(),
                "corp_code": corp_codes,
                "bsns_year": business_year,
                "reprt_code": report_code,
                "fs_div": fs_type,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def get_xbrl_taxonomy(self, report_code: str) -> dict:
        """XBRL 택소노미 재무항목 목록."""
        resp = await self._client.get(
            "/xbrlTaxonomy.json",
            params={**self._auth(), "sj_div": report_code},
        )
        resp.raise_for_status()
        return resp.json()

    # ── 주요 재무 항목 (정기보고서) ───────────────────────────────────────

    async def get_major_accounts(
        self, corp_code: str, business_year: str, report_code: str
    ) -> dict:
        """주요 재무 항목 (매출액, 영업이익, 당기순이익, 자산, 부채 등)."""
        resp = await self._client.get(
            "/fnlttSinglAcnt.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "013":
            disclosures = await self.search_disclosures(
                corp_code=corp_code,
                start_date=f"{business_year}0101",
                end_date=f"{business_year}1231",
                disclosure_type="A",
                page_count=5,
            )
            return {
                "status": "013",
                "message": (
                    "주요 재무 항목을 조회할 수 없습니다. "
                    "은행·증권·보험 등 금융업 회사는 DART 표준 재무제표 API에서 "
                    "조회가 불가합니다. 아래 공시 문서를 직접 확인하세요."
                ),
                "suggestion": (
                    "dart_get_disclosure_document(rcept_no)를 사용해 "
                    "실제 공시 원문 문서 목록을 조회한 뒤 재무제표 파일을 확인하세요."
                ),
                "related_disclosures": disclosures.get("list", []),
            }

        return data

    # ── 배당 ──────────────────────────────────────────────────────────────

    async def get_dividend_info(
        self, corp_code: str, business_year: str, report_code: str
    ) -> dict:
        """배당 관련 사항 (배당수익률, 주당배당금 등)."""
        resp = await self._client.get(
            "/alotMatter.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
            },
        )
        resp.raise_for_status()
        return resp.json()

    # ── 지분 공시 ─────────────────────────────────────────────────────────

    async def get_major_shareholders(
        self, corp_code: str, business_year: str, report_code: str
    ) -> dict:
        """최대주주 및 특수관계인 주식 소유 현황."""
        resp = await self._client.get(
            "/hyslrSttus.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def get_executive_shareholding(
        self, corp_code: str, business_year: str, report_code: str
    ) -> dict:
        """임원 및 주요주주 소유 주식 현황."""
        resp = await self._client.get(
            "/elecsAnexp.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
            },
        )
        resp.raise_for_status()
        return resp.json()

    # ── 지분율 5% 이상 대량 보유 ─────────────────────────────────────────

    async def get_large_shareholders(self, corp_code: str) -> dict:
        """5% 이상 대량보유 상황 보고."""
        resp = await self._client.get(
            "/majorstock.json", params={**self._auth(), "corp_code": corp_code}
        )
        resp.raise_for_status()
        return resp.json()

    # ── 임원 정보 ─────────────────────────────────────────────────────────

    async def get_executives(
        self, corp_code: str, business_year: str, report_code: str
    ) -> dict:
        """임원 현황 (직책, 성명, 주요 경력)."""
        resp = await self._client.get(
            "/exctvSttus.json",
            params={
                **self._auth(),
                "corp_code": corp_code,
                "bsns_year": business_year,
                "reprt_code": report_code,
            },
        )
        resp.raise_for_status()
        return resp.json()

    # ── 공시 원문 재무제표 (금융업 포함) ──────────────────────────────────

    async def get_financial_statements_from_document(
        self,
        corp_code: str,
        business_year: str,
        report_code: str = "11011",
    ) -> dict:
        """공시 원문 ZIP을 다운로드해 HTML에서 재무제표 테이블을 파싱합니다.
        증권사·은행·보험사 등 표준 API로 조회 불가한 금융업 회사에 사용하세요.
        report_code: 11011=사업보고서, 11012=반기, 11013=1분기, 11014=3분기
        """
        from bs4 import BeautifulSoup

        # 보고서 코드 → 검색 키워드 매핑
        report_keyword_map = {
            "11011": "사업보고서",
            "11012": "반기보고서",
            "11013": "분기보고서",
            "11014": "분기보고서",
        }
        keyword = report_keyword_map.get(report_code, "사업보고서")

        # ── 1. 해당 연도 공시 검색 ────────────────────────────────────────
        next_year = str(int(business_year) + 1)
        disclosures = await self.search_disclosures(
            corp_code=corp_code,
            start_date=f"{business_year}0101",
            end_date=f"{next_year}0630",
            disclosure_type="A",
            page_count=10,
        )

        rcept_no = None
        report_nm = None
        for disc in disclosures.get("list", []):
            nm = disc.get("report_nm", "")
            if keyword in nm and "[첨부정정]" not in nm and "[정정]" not in nm:
                rcept_no = disc["rcept_no"]
                report_nm = nm
                break

        if not rcept_no:
            return {
                "status": "error",
                "message": f"{business_year}년 {keyword}를 찾을 수 없습니다.",
                "disclosures": disclosures.get("list", []),
            }

        # ── 2. document.xml 다운로드 (실제 ZIP 바이너리) ─────────────────
        resp = await self._client.get(
            "/document.xml",
            params={**self._auth(), "rcept_no": rcept_no},
        )
        resp.raise_for_status()

        # ── 3. ZIP에서 파일 파싱 (HTML/XML 모두 지원) ───────────────────
        import warnings
        from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

        fin_keywords = {
            "재무상태표", "손익계산서", "포괄손익계산서",
            "현금흐름표", "자본변동표",
            "영업수익", "영업비용", "당기순이익",
            "자산총계", "부채총계", "자본총계",
        }

        results: list[dict] = []

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # DART 공시는 .xml 또는 .html 형태로 제공됨
            doc_files = sorted(
                [f for f in zf.namelist()
                 if f.lower().endswith((".html", ".htm", ".xml"))],
            )

            for fname in doc_files:
                raw = zf.read(fname)
                for enc in ("utf-8", "euc-kr", "cp949"):
                    try:
                        content = raw.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    continue

                # XMLParsedAsHTMLWarning 숨기기 (의도적 사용)
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                    soup = BeautifulSoup(content, "html.parser")

                page_text = soup.get_text()
                if not any(kw in page_text for kw in fin_keywords):
                    continue

                for table in soup.find_all("table"):
                    table_text = table.get_text()
                    if not any(kw in table_text for kw in fin_keywords):
                        continue

                    rows = []
                    for tr in table.find_all("tr"):
                        cells = [
                            td.get_text(" ", strip=True)
                            for td in tr.find_all(["td", "th"])
                        ]
                        if cells and any(c for c in cells):
                            rows.append(cells)

                    if len(rows) >= 3:
                        title = ""
                        preceding = table.find_previous(
                            ["h1", "h2", "h3", "h4", "p", "div", "title"],
                            string=lambda t: t and any(kw in t for kw in fin_keywords),
                        )
                        if preceding:
                            title = preceding.get_text(strip=True)

                        results.append({
                            "file": fname,
                            "title": title,
                            "rows": rows,
                        })

        if not results:
            return {
                "status": "error",
                "message": "재무제표 테이블을 찾을 수 없습니다.",
                "rcept_no": rcept_no,
                "report_nm": report_nm,
            }

        return {
            "status": "000",
            "corp_code": corp_code,
            "business_year": business_year,
            "report_nm": report_nm,
            "rcept_no": rcept_no,
            "tables": results,
        }

    async def aclose(self) -> None:
        await self._client.aclose()
