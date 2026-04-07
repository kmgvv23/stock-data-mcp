from fastmcp import FastMCP
from stock_data_mcp.clients.dart import DARTClient

dart = FastMCP("DART")
_client = DARTClient()


# ── 기업 코드 / 개황 ────────────────────────────────────────────────────────


@dart.tool
async def get_corp_codes(
    query: str = "",
    stock_code: str = "",
    listed_only: bool = True,
) -> list[dict]:
    """기업의 DART corp_code를 검색합니다.

    corp_code는 DART 재무제표·공시 조회에 필요한 고유번호입니다.
    반드시 query 또는 stock_code 중 하나를 지정하세요.
    아무것도 지정하지 않으면 상장법인 전체 목록을 반환합니다 (매우 많음).

    Args:
        query: 회사명 검색어, 부분 일치 (예: '삼성증권', '카카오')
        stock_code: 거래소 6자리 종목코드, 정확 일치 (예: '005930')
        listed_only: True=상장법인만(기본값), False=비상장 포함 전체
    """
    return await _client.get_corp_codes(query, stock_code, listed_only)


@dart.tool
async def get_company_info(corp_code: str) -> dict:
    """기업 개황 정보를 조회합니다 (회사명, 대표자, 업종, 설립일, 주소, 홈페이지 등).

    Args:
        corp_code: DART 기업 고유번호 8자리 (예: '00126380' - 삼성전자)
    """
    return await _client.get_company_info(corp_code)


# ── 공시 ────────────────────────────────────────────────────────────────────


@dart.tool
async def search_disclosures(
    corp_code: str = "",
    start_date: str = "",
    end_date: str = "",
    disclosure_type: str = "",
    page_no: int = 1,
    page_count: int = 40,
) -> dict:
    """DART 전자공시 목록을 검색합니다.

    Args:
        corp_code: DART 기업 고유번호 (비워두면 전체 기업 대상)
        start_date: 검색 시작일 (YYYYMMDD, 예: '20240101')
        end_date: 검색 종료일 (YYYYMMDD, 예: '20241231')
        disclosure_type: 공시 유형. 'A'=정기공시, 'B'=주요사항보고, 'C'=발행공시,
                         'D'=지분공시, 'E'=기타공시, 'F'=외감법인, 'G'=펀드공시, 'H'=자산유동화
        page_no: 페이지 번호 (기본값: 1)
        page_count: 페이지당 건수 (기본값: 40, 최대: 100)
    """
    return await _client.search_disclosures(
        corp_code, start_date, end_date, disclosure_type, page_no, page_count
    )


@dart.tool
async def get_disclosure_document(rcept_no: str) -> dict:
    """공시 접수번호로 원문 문서 정보를 조회합니다.

    Args:
        rcept_no: 공시 접수번호 14자리 (search_disclosures 결과의 rcept_no 필드)
    """
    return await _client.get_disclosure_document(rcept_no)


# ── 재무제표 ────────────────────────────────────────────────────────────────


@dart.tool
async def get_financial_statements(
    corp_code: str,
    business_year: str,
    report_code: str,
    fs_type: str = "CFS",
) -> dict:
    """전체 재무제표를 조회합니다 (재무상태표, 손익계산서, 현금흐름표, 자본변동표).

    주의: 은행·증권사·보험사 등 금융업 회사는 DART XBRL 표준 재무제표 API에서 조회가
    불가합니다. 이 경우 status='013'과 함께 관련 공시 문서 목록을 반환하며,
    dart_get_disclosure_document()로 실제 공시 원문을 확인해야 합니다.

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY, 예: '2023')
        report_code: 보고서 코드. '11013'=1분기보고서, '11012'=반기보고서,
                     '11014'=3분기보고서, '11011'=사업보고서(연간)
        fs_type: 재무제표 유형. 'CFS'=연결재무제표(기본값), 'OFS'=별도재무제표
    """
    return await _client.get_financial_statements(corp_code, business_year, report_code, fs_type)


@dart.tool
async def get_financial_statements_multi(
    corp_codes: str,
    business_year: str,
    report_code: str,
    fs_type: str = "CFS",
) -> dict:
    """여러 기업의 재무제표를 한 번에 조회합니다 (최대 100개).

    Args:
        corp_codes: DART 기업 고유번호를 콤마로 구분 (예: '00126380,00164779')
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드. '11013'=1분기, '11012'=반기, '11014'=3분기, '11011'=사업보고서
        fs_type: 'CFS'=연결(기본값), 'OFS'=별도
    """
    return await _client.get_financial_statements_multi(
        corp_codes, business_year, report_code, fs_type
    )


@dart.tool
async def get_major_accounts(
    corp_code: str, business_year: str, report_code: str
) -> dict:
    """주요 재무 항목을 조회합니다 (매출액, 영업이익, 당기순이익, 자산총계, 부채총계, 자본총계).

    주의: 은행·증권사·보험사 등 금융업 회사는 조회가 불가하며, 이 경우 관련 공시 목록을 반환합니다.

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드. '11013'=1분기, '11012'=반기, '11014'=3분기, '11011'=연간
    """
    return await _client.get_major_accounts(corp_code, business_year, report_code)


@dart.tool
async def get_financial_statements_from_document(
    corp_code: str,
    business_year: str,
    report_code: str = "11011",
) -> dict:
    """공시 원문(document.zip)을 다운로드해 HTML에서 재무제표 테이블을 직접 파싱합니다.

    증권사·은행·보험사 등 금융업 회사는 표준 DART 재무제표 API로 조회가 불가하므로
    이 툴을 사용하세요. 모든 업종에서 사용 가능하며, 실제 공시 원문 기준 데이터를
    반환합니다.

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY, 예: '2024')
        report_code: '11011'=사업보고서(기본값·연간), '11012'=반기보고서,
                     '11013'=1분기보고서, '11014'=3분기보고서
    """
    return await _client.get_financial_statements_from_document(
        corp_code, business_year, report_code
    )


# ── 배당 ────────────────────────────────────────────────────────────────────


@dart.tool
async def get_dividend_info(
    corp_code: str, business_year: str, report_code: str
) -> dict:
    """배당 관련 사항을 조회합니다 (주당배당금, 배당수익률, 배당성향, 배당금총액).

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드. '11011'=사업보고서(연간) 권장
    """
    return await _client.get_dividend_info(corp_code, business_year, report_code)


# ── 주주 / 지분 ─────────────────────────────────────────────────────────────


@dart.tool
async def get_major_shareholders(
    corp_code: str, business_year: str, report_code: str
) -> dict:
    """최대주주 및 특수관계인의 주식 소유 현황을 조회합니다.

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드. '11011'=사업보고서 권장
    """
    return await _client.get_major_shareholders(corp_code, business_year, report_code)


@dart.tool
async def get_executive_shareholding(
    corp_code: str, business_year: str, report_code: str
) -> dict:
    """임원 및 주요주주의 주식 소유 현황을 조회합니다.

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드
    """
    return await _client.get_executive_shareholding(corp_code, business_year, report_code)


@dart.tool
async def get_large_shareholders(corp_code: str) -> dict:
    """5% 이상 대량보유 주주 현황을 조회합니다 (대량보유 보고 기준).

    Args:
        corp_code: DART 기업 고유번호 8자리
    """
    return await _client.get_large_shareholders(corp_code)


# ── 임원 ────────────────────────────────────────────────────────────────────


@dart.tool
async def get_executives(
    corp_code: str, business_year: str, report_code: str
) -> dict:
    """임원 현황을 조회합니다 (직책, 성명, 성별, 출생연도, 주요 경력).

    Args:
        corp_code: DART 기업 고유번호 8자리
        business_year: 사업연도 (YYYY)
        report_code: 보고서 코드
    """
    return await _client.get_executives(corp_code, business_year, report_code)
