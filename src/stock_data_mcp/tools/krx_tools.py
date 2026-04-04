from fastmcp import FastMCP
from stock_data_mcp.clients.krx import KRXClient

krx = FastMCP("KRX")
_client = KRXClient()


# ── 종목 정보 ──────────────────────────────────────────────────────────────


@krx.tool
async def list_stocks(market: str = "STK") -> dict:
    """KRX 시장의 전체 상장 종목 목록을 조회합니다.

    Args:
        market: 시장 구분. 'STK'=KOSPI(기본값), 'KSQ'=KOSDAQ, 'KNX'=KONEX
    """
    return await _client.get_listed_stocks(market)


@krx.tool
async def get_stock_info(isin: str) -> dict:
    """종목의 기본 정보를 조회합니다 (종목명, 상장일, 액면가, 자본금, 상장주식수 등).

    Args:
        isin: 종목 ISIN 코드 (예: 'KR7005930003' - 삼성전자)
    """
    return await _client.get_stock_info(isin)


# ── 주가 데이터 ────────────────────────────────────────────────────────────


@krx.tool
async def get_stock_ohlcv(isin: str, start_date: str, end_date: str) -> dict:
    """종목의 기간별 일별 시세 (시가/고가/저가/종가/거래량/거래대금)를 조회합니다.

    Args:
        isin: 종목 ISIN 코드 (예: 'KR7005930003' - 삼성전자)
        start_date: 조회 시작일 (YYYYMMDD 형식, 예: '20240101')
        end_date: 조회 종료일 (YYYYMMDD 형식, 예: '20241231')
    """
    return await _client.get_stock_ohlcv(isin, start_date, end_date)


@krx.tool
async def get_market_ohlcv(market: str, start_date: str, end_date: str) -> dict:
    """시장 전체의 일별 시세 합계 (거래량, 거래대금, 상승/하락 종목 수 등)를 조회합니다.

    Args:
        market: 시장 구분. 'STK'=KOSPI, 'KSQ'=KOSDAQ
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_market_ohlcv(market, start_date, end_date)


@krx.tool
async def get_market_cap(market: str, date: str) -> dict:
    """특정 날짜 기준 시가총액 상위 종목 현황을 조회합니다.

    Args:
        market: 시장 구분. 'STK'=KOSPI, 'KSQ'=KOSDAQ
        date: 기준일 (YYYYMMDD 형식, 예: '20241231')
    """
    return await _client.get_market_cap(market, date)


# ── 지수 데이터 ────────────────────────────────────────────────────────────


@krx.tool
async def list_indices(market: str = "STK") -> dict:
    """KRX 지수 전체 목록을 조회합니다 (KOSPI, KOSPI200, KRX100 등).

    Args:
        market: 시장 구분. 'STK'=KOSPI계열(기본값), 'KSQ'=KOSDAQ계열
    """
    return await _client.get_index_list(market)


@krx.tool
async def get_index_ohlcv(index_id: str, start_date: str, end_date: str) -> dict:
    """지수의 기간별 일별 시세 (시가/고가/저가/종가/거래량)를 조회합니다.

    Args:
        index_id: 지수 코드 (예: '1' = KOSPI, '2' = KOSPI200)
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_index_ohlcv(index_id, start_date, end_date)


@krx.tool
async def get_index_components(index_id: str, date: str) -> dict:
    """지수 구성 종목 목록과 각 종목의 편입 비중을 조회합니다.

    Args:
        index_id: 지수 코드 (예: '2' = KOSPI200)
        date: 기준일 (YYYYMMDD 형식)
    """
    return await _client.get_index_components(index_id, date)


# ── ETF/ETN ────────────────────────────────────────────────────────────────


@krx.tool
async def list_etf() -> dict:
    """KRX에 상장된 전체 ETF 목록과 기본 정보를 조회합니다."""
    return await _client.get_etf_list()


@krx.tool
async def get_etf_ohlcv(isin: str, start_date: str, end_date: str) -> dict:
    """ETF의 기간별 일별 시세 (NAV 포함)를 조회합니다.

    Args:
        isin: ETF ISIN 코드
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_etf_ohlcv(isin, start_date, end_date)


@krx.tool
async def list_etn() -> dict:
    """KRX에 상장된 전체 ETN 목록과 기본 정보를 조회합니다."""
    return await _client.get_etn_list()


# ── 투자자별 거래 ──────────────────────────────────────────────────────────


@krx.tool
async def get_investor_trading(market: str, start_date: str, end_date: str) -> dict:
    """시장 전체의 투자자별 매매동향 (기관/외국인/개인/기타 순매수금액)을 조회합니다.

    Args:
        market: 시장 구분. 'STK'=KOSPI, 'KSQ'=KOSDAQ
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_investor_trading(market, start_date, end_date)


@krx.tool
async def get_investor_trading_by_stock(isin: str, start_date: str, end_date: str) -> dict:
    """특정 종목의 투자자별 매매동향을 조회합니다.

    Args:
        isin: 종목 ISIN 코드
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_investor_trading_by_stock(isin, start_date, end_date)


# ── 파생상품 ───────────────────────────────────────────────────────────────


@krx.tool
async def list_futures() -> dict:
    """KRX에 상장된 전체 선물(Futures) 종목 목록을 조회합니다."""
    return await _client.get_futures_list()


@krx.tool
async def get_futures_ohlcv(isin: str, start_date: str, end_date: str) -> dict:
    """선물 종목의 기간별 일별 시세를 조회합니다.

    Args:
        isin: 선물 ISIN 코드
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_futures_ohlcv(isin, start_date, end_date)


# ── 공매도 ─────────────────────────────────────────────────────────────────


@krx.tool
async def get_short_selling(market: str, start_date: str, end_date: str) -> dict:
    """시장별 공매도 거래 현황 (공매도 거래량, 거래대금, 비중)을 조회합니다.

    Args:
        market: 시장 구분. 'STK'=KOSPI, 'KSQ'=KOSDAQ
        start_date: 조회 시작일 (YYYYMMDD 형식)
        end_date: 조회 종료일 (YYYYMMDD 형식)
    """
    return await _client.get_short_selling(market, start_date, end_date)
