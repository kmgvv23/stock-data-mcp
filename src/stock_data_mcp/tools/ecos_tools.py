from fastmcp import FastMCP
from stock_data_mcp.clients.ecos import ECOSClient

ecos = FastMCP("ECOS")
_client = ECOSClient()


# ── 통계 탐색 ───────────────────────────────────────────────────────────────


@ecos.tool
async def list_statistic_tables(stat_code: str = "") -> dict:
    """한국은행 ECOS 통계 테이블 목록을 탐색합니다.
    stat_code를 비워두면 최상위 분류를 반환하고, 코드를 지정하면 하위 항목을 반환합니다.
    search_statistics 호출 전에 stat_code와 item_code를 확인할 때 사용하세요.

    Args:
        stat_code: 통계표 코드 (비워두면 최상위 분류 반환)
    """
    return await _client.get_statistic_table_list(stat_code)


@ecos.tool
async def list_statistic_items(stat_code: str) -> dict:
    """특정 통계표의 세부 항목 목록을 조회합니다 (item_code 확인용).

    Args:
        stat_code: 통계표 코드 (예: '722Y001' - 기준금리)
    """
    return await _client.get_statistic_item_list(stat_code)


@ecos.tool
async def search_statistics(
    stat_code: str,
    item_code: str,
    cycle: str,
    start_date: str,
    end_date: str,
) -> dict:
    """한국은행 ECOS 통계 시계열 데이터를 조회합니다.

    Args:
        stat_code: 통계표 코드 (예: '722Y001' - 기준금리)
        item_code: 항목 코드 (예: '0101000')
        cycle: 주기. 'A'=연간, 'Q'=분기, 'M'=월간, 'D'=일별
        start_date: 시작 기간 (주기에 맞는 형식 - A:'2020', Q:'2020Q1', M:'202001', D:'20200101')
        end_date: 종료 기간 (start_date와 동일 형식)
    """
    return await _client.search_statistics(stat_code, item_code, cycle, start_date, end_date)


@ecos.tool
async def get_key_statistics() -> dict:
    """한국은행 100대 핵심 통계지표를 조회합니다.
    GDP 성장률, 소비자물가상승률, 기준금리, 원/달러 환율, 경상수지 등
    주요 거시경제 지표의 최신값과 전기 대비 변화를 한 번에 확인할 수 있습니다.
    """
    return await _client.get_key_statistics()


# ── 금리 ────────────────────────────────────────────────────────────────────


@ecos.tool
async def get_base_rate(start_date: str, end_date: str) -> dict:
    """한국은행 기준금리 (Base Rate) 시계열을 조회합니다.

    Args:
        start_date: 시작일 (YYYYMMDD, 예: '20200101')
        end_date: 종료일 (YYYYMMDD, 예: '20241231')
    """
    return await _client.get_base_rate(start_date, end_date)


# ── 환율 ────────────────────────────────────────────────────────────────────


@ecos.tool
async def get_exchange_rate_usd(start_date: str, end_date: str) -> dict:
    """원/달러(KRW/USD) 환율 일별 시계열을 조회합니다.

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    return await _client.get_exchange_rate_usd(start_date, end_date)


@ecos.tool
async def get_exchange_rate_jpy(start_date: str, end_date: str) -> dict:
    """원/100엔(KRW/JPY) 환율 일별 시계열을 조회합니다.

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    return await _client.get_exchange_rate_jpy(start_date, end_date)


@ecos.tool
async def get_exchange_rate_eur(start_date: str, end_date: str) -> dict:
    """원/유로(KRW/EUR) 환율 일별 시계열을 조회합니다.

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    return await _client.get_exchange_rate_eur(start_date, end_date)


# ── 물가 / 경기 ─────────────────────────────────────────────────────────────


@ecos.tool
async def get_cpi(start_date: str, end_date: str) -> dict:
    """소비자물가지수(CPI) 월별 시계열을 조회합니다.

    Args:
        start_date: 시작 월 (YYYYMM, 예: '202001')
        end_date: 종료 월 (YYYYMM, 예: '202412')
    """
    return await _client.get_cpi(start_date, end_date)


@ecos.tool
async def get_gdp(start_date: str, end_date: str) -> dict:
    """GDP 성장률 분기별 시계열을 조회합니다.

    Args:
        start_date: 시작 분기 (YYYYQN, 예: '2020Q1')
        end_date: 종료 분기 (YYYYQN, 예: '2024Q3')
    """
    return await _client.get_gdp(start_date, end_date)


# ── 통화 / 주식 ─────────────────────────────────────────────────────────────


@ecos.tool
async def get_money_supply_m2(start_date: str, end_date: str) -> dict:
    """광의통화(M2) 잔액 월별 시계열을 조회합니다.

    Args:
        start_date: 시작 월 (YYYYMM)
        end_date: 종료 월 (YYYYMM)
    """
    return await _client.get_money_supply_m2(start_date, end_date)


@ecos.tool
async def get_kospi_ecos(start_date: str, end_date: str) -> dict:
    """한국은행 ECOS 제공 KOSPI 지수 일별 시계열을 조회합니다.

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    return await _client.get_kospi_index(start_date, end_date)
