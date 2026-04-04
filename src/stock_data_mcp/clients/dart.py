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

    async def get_corp_codes(self) -> list[dict]:
        """전체 기업 코드 목록 (ZIP → XML 파싱). corp_code와 stock_code를 포함."""
        resp = await self._client.get("/corpCode.xml", params=self._auth())
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_bytes = zf.read("CORPCODE.xml")

        # 간단한 XML 파싱 (xml.etree 사용)
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_bytes)
        return [
            {
                "corp_code": item.findtext("corp_code", ""),
                "corp_name": item.findtext("corp_name", ""),
                "stock_code": item.findtext("stock_code", ""),
                "modify_date": item.findtext("modify_date", ""),
            }
            for item in root.findall("list")
        ]

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
        """공시 원문 문서 정보."""
        resp = await self._client.get(
            "/document.json", params={**self._auth(), "rcept_no": rcept_no}
        )
        resp.raise_for_status()
        return resp.json()

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
        return resp.json()

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
        return resp.json()

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

    async def aclose(self) -> None:
        await self._client.aclose()
