"""
Bank of Korea ECOS API client (한국은행 경제통계시스템).
Base URL: https://ecos.bok.or.kr/api
Auth: API key embedded in URL path
URL pattern: /{service}/{api_key}/{format}/{lang}/{start_idx}/{end_idx}/...
"""

import httpx

from stock_data_mcp.config import get_settings

_BASE_URL = "https://ecos.bok.or.kr/api"


class ECOSClient:
    def __init__(self) -> None:
        self._api_key = get_settings().ecos_api_key
        self._client = httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0)

    def _url(self, service: str, *path_parts: str, start: int = 1, end: int = 100) -> str:
        """ECOS URL 빌더: /{service}/{key}/json/kr/{start}/{end}/{...extras}"""
        parts = [service, self._api_key, "json", "kr", str(start), str(end), *path_parts]
        return "/" + "/".join(parts)

    # ── 통계 목록 탐색 ────────────────────────────────────────────────────

    async def get_statistic_table_list(self, stat_code: str = "") -> dict:
        """ECOS 통계 테이블 목록 탐색.
        stat_code를 비워두면 최상위 분류 반환. 코드 지정 시 하위 항목 반환.
        """
        url = self._url("StatisticTableList", stat_code, end=200)
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def get_statistic_item_list(self, stat_code: str) -> dict:
        """특정 통계표의 세부 항목 목록 (item_code 확인용)."""
        url = self._url("StatisticItemList", stat_code, end=500)
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    # ── 통계 조회 ─────────────────────────────────────────────────────────

    async def search_statistics(
        self,
        stat_code: str,
        item_code: str,
        cycle: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        """시계열 데이터 조회.
        cycle: A=연, Q=분기, M=월, D=일
        start_date/end_date 형식: A→'2020', Q→'2020Q1', M→'202001', D→'20200101'
        """
        url = self._url(
            "StatisticSearch",
            stat_code,
            cycle,
            start_date,
            end_date,
            item_code,
            end=10000,
        )
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    # ── 주요 지표 (핵심 100개) ────────────────────────────────────────────

    async def get_key_statistics(self) -> dict:
        """한국은행 100대 핵심 통계지표 (GDP, 소비자물가, 기준금리, 환율 등)."""
        url = self._url("KeyStatisticList", end=100)
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    # ── 편의 메서드: 주요 거시경제 지표 ──────────────────────────────────

    async def get_base_rate(self, start_date: str, end_date: str) -> dict:
        """한국은행 기준금리 (일별). stat_code=722Y001, item_code=0101000"""
        return await self.search_statistics("722Y001", "0101000", "D", start_date, end_date)

    async def get_exchange_rate_usd(self, start_date: str, end_date: str) -> dict:
        """원/달러 환율 (일별). stat_code=731Y001, item_code=0000001"""
        return await self.search_statistics("731Y001", "0000001", "D", start_date, end_date)

    async def get_exchange_rate_jpy(self, start_date: str, end_date: str) -> dict:
        """원/100엔 환율 (일별). stat_code=731Y001, item_code=0000002"""
        return await self.search_statistics("731Y001", "0000002", "D", start_date, end_date)

    async def get_exchange_rate_eur(self, start_date: str, end_date: str) -> dict:
        """원/유로 환율 (일별). stat_code=731Y001, item_code=0000003"""
        return await self.search_statistics("731Y001", "0000003", "D", start_date, end_date)

    async def get_cpi(self, start_date: str, end_date: str) -> dict:
        """소비자물가지수 (월별). stat_code=901Y009, item_code=0"""
        return await self.search_statistics("901Y009", "0", "M", start_date, end_date)

    async def get_gdp(self, start_date: str, end_date: str) -> dict:
        """GDP 성장률 (분기별). stat_code=200Y001, item_code=10101"""
        return await self.search_statistics("200Y001", "10101", "Q", start_date, end_date)

    async def get_kospi_index(self, start_date: str, end_date: str) -> dict:
        """KOSPI 지수 (일별, ECOS 제공). stat_code=802Y001, item_code=0001000"""
        return await self.search_statistics("802Y001", "0001000", "D", start_date, end_date)

    async def get_money_supply_m2(self, start_date: str, end_date: str) -> dict:
        """광의통화(M2) 잔액 (월별). stat_code=101Y004, item_code=BBHA00"""
        return await self.search_statistics("101Y004", "BBHA00", "M", start_date, end_date)

    async def aclose(self) -> None:
        await self._client.aclose()
