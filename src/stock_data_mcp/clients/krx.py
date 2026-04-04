"""
KRX Open API client.

KRX uses a two-step auth:
  1. GET GenerateOTP.cmd with your api_key + query intent → one-time token
  2. POST OPPDATA002.cmd with that token → actual data
"""

import httpx
from stock_data_mcp.config import get_settings

_OTP_URL = "https://openapi.krx.co.kr/contents/COM/GenerateOTP.cmd"
_DATA_URL = "https://openapi.krx.co.kr/contents/OPP/DATA/OPPDATA002.cmd"


class KRXClient:
    def __init__(self) -> None:
        self._api_key = get_settings().krx_api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    async def _fetch(self, bld: str, extra: dict | None = None) -> dict:
        """Get OTP then POST to data endpoint."""
        params = {"auth": self._api_key, "bld": bld, **(extra or {})}
        otp_resp = await self._client.get(_OTP_URL, params=params)
        otp_resp.raise_for_status()
        otp = otp_resp.text.strip()

        data_resp = await self._client.post(_DATA_URL, data={"code": otp})
        data_resp.raise_for_status()
        return data_resp.json()

    # ── 종목 정보 ─────────────────────────────────────────────────────────

    async def get_listed_stocks(self, market: str = "STK") -> dict:
        """전체 상장 종목 목록. market: STK=KOSPI, KSQ=KOSDAQ, KNX=KONEX"""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT01901",
            {"mktId": market},
        )

    async def get_stock_info(self, isin: str) -> dict:
        """종목 기본 정보 (종목명, 상장일, 자본금 등)."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT01701",
            {"isuCd": isin},
        )

    # ── 주가 데이터 ───────────────────────────────────────────────────────

    async def get_stock_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """일별 OHLCV + 거래대금. start_date/end_date: YYYYMMDD"""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT01501",
            {"isuCd": isin, "strtDd": start_date, "endDd": end_date},
        )

    async def get_market_ohlcv(self, market: str, start_date: str, end_date: str) -> dict:
        """시장 전체 일별 시세. market: STK=KOSPI, KSQ=KOSDAQ"""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT01401",
            {"mktId": market, "strtDd": start_date, "endDd": end_date},
        )

    async def get_market_cap(self, market: str, date: str) -> dict:
        """시가총액 상위 종목. date: YYYYMMDD"""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT03901",
            {"mktId": market, "trdDd": date},
        )

    # ── 지수 데이터 ───────────────────────────────────────────────────────

    async def get_index_list(self, market: str = "STK") -> dict:
        """전체 지수 목록."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT00401",
            {"mktId": market},
        )

    async def get_index_ohlcv(self, index_id: str, start_date: str, end_date: str) -> dict:
        """지수 일별 OHLCV."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT00301",
            {"idxIndMidclssCd": index_id, "strtDd": start_date, "endDd": end_date},
        )

    async def get_index_components(self, index_id: str, date: str) -> dict:
        """지수 구성 종목."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT00601",
            {"idxIndMidclssCd": index_id, "trdDd": date},
        )

    # ── ETF/ETN ───────────────────────────────────────────────────────────

    async def get_etf_list(self) -> dict:
        """전체 ETF 목록."""
        return await self._fetch("dbms/MDC/STAT/standard/MDCSTAT04601")

    async def get_etf_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """ETF 일별 시세."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT04801",
            {"isuCd": isin, "strtDd": start_date, "endDd": end_date},
        )

    async def get_etn_list(self) -> dict:
        """전체 ETN 목록."""
        return await self._fetch("dbms/MDC/STAT/standard/MDCSTAT07301")

    # ── 투자자별 거래 ─────────────────────────────────────────────────────

    async def get_investor_trading(self, market: str, start_date: str, end_date: str) -> dict:
        """투자자별 매매동향 (기관/외국인/개인)."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT02201",
            {"mktId": market, "strtDd": start_date, "endDd": end_date},
        )

    async def get_investor_trading_by_stock(
        self, isin: str, start_date: str, end_date: str
    ) -> dict:
        """종목별 투자자 매매동향."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT02301",
            {"isuCd": isin, "strtDd": start_date, "endDd": end_date},
        )

    # ── 파생상품 ──────────────────────────────────────────────────────────

    async def get_futures_list(self) -> dict:
        """선물 종목 목록."""
        return await self._fetch("dbms/MDC/STAT/standard/MDCSTAT10301")

    async def get_futures_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """선물 일별 시세."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT10401",
            {"isuCd": isin, "strtDd": start_date, "endDd": end_date},
        )

    # ── 공매도 ────────────────────────────────────────────────────────────

    async def get_short_selling(self, market: str, start_date: str, end_date: str) -> dict:
        """공매도 거래 현황."""
        return await self._fetch(
            "dbms/MDC/STAT/standard/MDCSTAT11701",
            {"mktId": market, "strtDd": start_date, "endDd": end_date},
        )

    async def aclose(self) -> None:
        await self._client.aclose()
