"""
KRX Open API client (data-dbg.krx.co.kr).

REST API with AUTH_KEY query-param auth.
Response format: {"OutBlock_1": [{...}, ...]}

Endpoints are snapshot-based (one trading date per call).
For date-range methods, this client iterates over business days
and concatenates results (skip empty = holiday).
"""

import asyncio
from datetime import date, timedelta

import httpx

from stock_data_mcp.config import get_settings

_BASE_URL = "https://data-dbg.krx.co.kr/svc/apis"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Max business days to iterate for range queries to avoid runaway API calls
_MAX_RANGE_DAYS = 400


def _isin_to_short_code(isin: str) -> str:
    """Extract 6-digit KRX short code from ISIN (e.g. 'KR7000660001' → '000660')."""
    if isin.startswith("KR") and len(isin) == 12:
        return isin[3:9]
    return isin  # already a short code or unknown format


def _business_days(start: str, end: str) -> list[str]:
    """Return YYYYMMDD strings for Mon-Fri between start and end (inclusive)."""
    d = date(int(start[:4]), int(start[4:6]), int(start[6:]))
    e = date(int(end[:4]), int(end[4:6]), int(end[6:]))
    result = []
    while d <= e:
        if d.weekday() < 5:  # Mon=0 … Fri=4
            result.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return result[:_MAX_RANGE_DAYS]


class KRXClient:
    def __init__(self) -> None:
        self._api_key = get_settings().krx_api_key
        self._client = httpx.AsyncClient(timeout=30.0, headers=_HEADERS)

    async def _fetch(self, path: str, params: dict | None = None) -> dict:
        """Single GET request; returns raw JSON dict."""
        url = f"{_BASE_URL}/{path}.json"
        all_params = {"AUTH_KEY": self._api_key, **(params or {})}
        resp = await self._client.get(url, params=all_params)
        resp.raise_for_status()
        return resp.json()

    async def _records(self, path: str, params: dict | None = None) -> list[dict]:
        """Return OutBlock_1 list (empty list if no data)."""
        data = await self._fetch(path, params)
        return data.get("OutBlock_1", [])

    async def _range_records(
        self, path: str, start_date: str, end_date: str, extra: dict | None = None
    ) -> list[dict]:
        """Iterate business days and aggregate OutBlock_1 records.

        Calls the endpoint once per business day between start and end.
        Empty responses (holidays) are skipped automatically.
        """
        days = _business_days(start_date, end_date)

        async def _fetch_day(d: str) -> list[dict]:
            params = {"basDd": d, **(extra or {})}
            return await self._records(path, params)

        # Fetch all days concurrently (up to 10 at a time to be polite)
        results: list[dict] = []
        batch = 10
        for i in range(0, len(days), batch):
            chunk = days[i : i + batch]
            fetched = await asyncio.gather(*[_fetch_day(d) for d in chunk])
            for records in fetched:
                results.extend(records)

        return results

    # ── 종목 기본 정보 ──────────────────────────────────────────────────────

    def _recent_trading_day(self, offset_days: int = 1) -> str:
        """Return YYYYMMDD of the most recent weekday (offset_days back from today)."""
        d = date.today() - timedelta(days=offset_days)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")

    async def get_listed_stocks(self, market: str = "STK") -> dict:
        """전체 상장 종목 목록.
        market: STK=KOSPI, KSQ=KOSDAQ, KNX=KONEX
        Returns {"OutBlock_1": [...]} with ISU_CD (ISIN), ISU_SRT_CD (6-digit),
        ISU_NM, ISU_ABBRV, LIST_DD, PARVAL, LIST_SHRS, …
        """
        path_map = {
            "STK": "sto/stk_isu_base_info",
            "KSQ": "sto/ksq_isu_base_info",
            "KNX": "sto/knx_isu_base_info",
        }
        path = path_map.get(market, "sto/stk_isu_base_info")
        bas_dd = self._recent_trading_day(1)
        records = await self._records(path, {"basDd": bas_dd})
        return {"OutBlock_1": records}

    async def get_stock_info(self, isin: str) -> dict:
        """종목 기본 정보 (ISIN 기준 조회)."""
        bas_dd = self._recent_trading_day(1)
        for path in ("sto/stk_isu_base_info", "sto/ksq_isu_base_info"):
            records = await self._records(path, {"basDd": bas_dd})
            match = [r for r in records if r.get("ISU_CD") == isin]
            if match:
                return {"OutBlock_1": match}
        return {"OutBlock_1": []}

    # ── 주가 데이터 ────────────────────────────────────────────────────────

    async def get_stock_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """종목 일별 OHLCV + 거래대금.
        start_date/end_date: YYYYMMDD
        Iterates business days; filters records by 6-digit short code extracted from ISIN.
        Fields: BAS_DD, ISU_CD (6-digit), ISU_NM, TDD_OPNPRC, TDD_HGPRC, TDD_LWPRC,
                TDD_CLSPRC, ACC_TRDVOL, ACC_TRDVAL, MKTCAP, LIST_SHRS
        """
        short_code = _isin_to_short_code(isin)
        # Try KOSPI first, fall back to KOSDAQ
        for path in ("sto/stk_bydd_trd", "sto/ksq_bydd_trd"):
            all_records = await self._range_records(path, start_date, end_date)
            filtered = [r for r in all_records if r.get("ISU_CD") == short_code]
            if filtered:
                return {"OutBlock_1": filtered}
        return {"OutBlock_1": []}

    async def get_market_ohlcv(self, market: str, start_date: str, end_date: str) -> dict:
        """시장 전체 일별 시세.
        market: STK=KOSPI, KSQ=KOSDAQ
        Returns all stocks for each trading date in the range.
        """
        path = "sto/ksq_bydd_trd" if market == "KSQ" else "sto/stk_bydd_trd"
        records = await self._range_records(path, start_date, end_date)
        return {"OutBlock_1": records}

    async def get_market_cap(self, market: str, date_str: str) -> dict:
        """시가총액 전체 종목 (특정일 스냅샷).
        date_str: YYYYMMDD
        Fields include: MKTCAP, LIST_SHRS, TDD_CLSPRC, …
        """
        path = "sto/ksq_bydd_trd" if market == "KSQ" else "sto/stk_bydd_trd"
        records = await self._records(path, {"basDd": date_str})
        return {"OutBlock_1": records}

    # ── 지수 데이터 ────────────────────────────────────────────────────────

    async def get_index_list(self, market: str = "STK") -> dict:
        """지수 목록 (특정일의 지수명 목록).
        market: STK=KOSPI, KSQ=KOSDAQ
        """
        bas_dd = self._recent_trading_day(1)
        path = "idx/kosdaq_dd_trd" if market == "KSQ" else "idx/kospi_dd_trd"
        records = await self._records(path, {"basDd": bas_dd})
        return {"OutBlock_1": records}

    async def get_index_ohlcv(self, index_id: str, start_date: str, end_date: str) -> dict:
        """지수 일별 OHLCV.
        index_id: 'KOSPI' or 'KOSDAQ' (partial match on IDX_NM)
        Fields: BAS_DD, IDX_CLSS, IDX_NM, CLSPRC_IDX, OPNPRC_IDX, HGPRC_IDX, LWPRC_IDX,
                CMPPREVDD_IDX, FLUC_RT, ACC_TRDVOL, ACC_TRDVAL, MKTCAP
        """
        # Determine path from index_id
        if "KOSDAQ" in index_id.upper() or "KSQ" in index_id.upper():
            path = "idx/kosdaq_dd_trd"
        elif "KRX" in index_id.upper():
            path = "idx/krx_dd_trd"
        else:
            path = "idx/kospi_dd_trd"
        records = await self._range_records(path, start_date, end_date)
        if index_id.upper() not in ("KOSPI", "KOSDAQ", "KRX"):
            records = [r for r in records if index_id in r.get("IDX_NM", "")]
        return {"OutBlock_1": records}

    async def get_index_components(self, index_id: str, date_str: str) -> dict:
        """지수 구성 종목 (일별 전체 시세에서 시장 데이터로 대체).
        Returns all market stocks for the given date as a proxy.
        """
        records = await self._records("sto/stk_bydd_trd", {"basDd": date_str})
        return {"OutBlock_1": records}

    # ── ETF/ETN ────────────────────────────────────────────────────────────

    async def get_etf_list(self) -> dict:
        """전체 ETF 목록 (최근 영업일 기준)."""
        bas_dd = self._recent_trading_day(1)
        records = await self._records("etp/etf_bydd_trd", {"basDd": bas_dd})
        return {"OutBlock_1": records}

    async def get_etf_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """ETF 일별 시세."""
        short_code = _isin_to_short_code(isin)
        all_records = await self._range_records("etp/etf_bydd_trd", start_date, end_date)
        filtered = [r for r in all_records if r.get("ISU_CD") == short_code or r.get("ISU_CD") == isin]
        return {"OutBlock_1": filtered}

    async def get_etn_list(self) -> dict:
        """전체 ETN 목록 (최근 영업일 기준)."""
        bas_dd = self._recent_trading_day(1)
        records = await self._records("etp/etn_bydd_trd", {"basDd": bas_dd})
        return {"OutBlock_1": records}

    # ── 투자자별 거래 ──────────────────────────────────────────────────────

    async def get_investor_trading(self, market: str, start_date: str, end_date: str) -> dict:
        """투자자별 매매동향 — 현재 KRX Open API 미제공.
        대신 해당 기간 시장 전체 시세 데이터를 반환합니다.
        """
        return await self.get_market_ohlcv(market, start_date, end_date)

    async def get_investor_trading_by_stock(
        self, isin: str, start_date: str, end_date: str
    ) -> dict:
        """종목별 투자자 매매동향 — 현재 KRX Open API 미제공.
        대신 해당 종목의 OHLCV 데이터를 반환합니다.
        """
        return await self.get_stock_ohlcv(isin, start_date, end_date)

    # ── 파생상품 ──────────────────────────────────────────────────────────

    async def get_futures_list(self) -> dict:
        """선물 종목 목록 (최근 영업일 기준)."""
        bas_dd = self._recent_trading_day(1)
        records = await self._records("drv/fut_bydd_trd", {"basDd": bas_dd})
        return {"OutBlock_1": records}

    async def get_futures_ohlcv(self, isin: str, start_date: str, end_date: str) -> dict:
        """선물 일별 시세."""
        all_records = await self._range_records("drv/fut_bydd_trd", start_date, end_date)
        filtered = [r for r in all_records if r.get("ISU_CD") == isin]
        return {"OutBlock_1": filtered}

    # ── 공매도 ────────────────────────────────────────────────────────────

    async def get_short_selling(self, market: str, start_date: str, end_date: str) -> dict:
        """공매도 거래 현황 — 현재 KRX Open API 미제공.
        대신 해당 기간 시장 전체 시세 데이터를 반환합니다.
        """
        return await self.get_market_ohlcv(market, start_date, end_date)

    async def aclose(self) -> None:
        await self._client.aclose()
