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

    async def get_financials_for_model(
        self,
        corp_code: str,
        years: list[int],
        report_code: str = "11011",
        fs_type: str = "CFS",
        unit: int = 1_000_000,
    ) -> dict:
        """여러 연도 재무제표를 병렬로 조회하고 모델 채우기에 적합한 구조로 반환합니다.

        XBRL account_id 기반으로 매핑하므로 회사·업종별 계정명 차이와 무관하게
        동일한 필드명으로 반환합니다.

        Args:
            corp_code: DART 기업 고유번호
            years: 조회 연도 리스트 (예: [2020, 2021, 2022, 2023, 2024])
            report_code: '11011'=사업보고서(기본값)
            fs_type: 'CFS'=연결(기본값), 'OFS'=별도
            unit: 반환 단위 (기본값 1_000_000 = 백만원). 1=원, 100_000_000=억원
        """
        import asyncio
        from datetime import date as _date

        # 현재 연도를 항상 포함 (AI가 빠뜨려도 자동 추가)
        current_year = _date.today().year
        if current_year not in years:
            years = sorted(set(years) | {current_year})

        # ── XBRL account_id → 표준 필드명 매핑 테이블 ─────────────────────
        # IFRS 표준 태그 기반. 회사명/계정명이 달라도 account_id는 동일.
        # 우선순위: 앞에 있을수록 먼저 매핑 (같은 필드에 여러 ID가 매핑될 수 있음)
        XBRL_MAP: dict[str, str] = {
            # ── 손익계산서 (IS) ──────────────────────────────────────────────
            "ifrs-full_Revenue":                                                        "revenue",
            "ifrs-full_GrossProfit":                                                    "gross_profit",
            "ifrs-full_CostOfSales":                                                    "cogs",
            "ifrs-full_SellingGeneralAndAdministrativeExpense":                         "sga",
            "ifrs-full_ProfitLossFromOperatingActivities":                              "operating_income",
            "dart-full_OperatingIncomeLoss":                                            "operating_income",
            "ifrs-full_FinanceIncome":                                                  "finance_income",
            "ifrs-full_FinanceCosts":                                                   "finance_costs",
            "ifrs-full_InterestIncome":                                                 "interest_income",
            "ifrs-full_InterestExpense":                                                "interest_expense",
            "ifrs-full_ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod":
                                                                                        "equity_method_income",
            "ifrs-full_OtherIncome":                                                    "other_income",
            "ifrs-full_OtherExpense":                                                   "other_expense",
            "ifrs-full_OtherGainsLosses":                                              "other_net",
            "ifrs-full_ProfitLossBeforeTax":                                            "ebt",
            "ifrs-full_IncomeTaxExpenseContinuingOperations":                           "income_tax",
            "ifrs-full_ProfitLoss":                                                     "net_income",
            "ifrs-full_ProfitLossAttributableToOwnersOfParent":                        "ni_controlling",
            "ifrs-full_ProfitLossAttributableToNoncontrollingInterests":               "ni_nci",
            # ── 재무상태표 (BS) ──────────────────────────────────────────────
            "ifrs-full_Assets":                                                         "total_assets",
            "ifrs-full_CurrentAssets":                                                  "current_assets",
            "ifrs-full_CashAndCashEquivalents":                                         "cash",
            "ifrs-full_ShorttermInvestments":                                           "st_investments",
            "ifrs-full_CurrentFinancialAssets":                                         "st_investments",
            "ifrs-full_TradeAndOtherCurrentReceivables":                               "accounts_receivable",
            "ifrs-full_TradeReceivables":                                               "accounts_receivable",
            "ifrs-full_Inventories":                                                    "inventory",
            "ifrs-full_NoncurrentAssets":                                               "noncurrent_assets",
            "ifrs-full_PropertyPlantAndEquipment":                                      "ppe",
            "ifrs-full_IntangibleAssetsOtherThanGoodwill":                             "intangibles",
            "ifrs-full_IntangibleAssets":                                               "intangibles",
            "ifrs-full_IntangibleAssetsAndGoodwill":                                    "intangibles",
            "ifrs-full_Goodwill":                                                       "goodwill",
            "ifrs-full_Liabilities":                                                    "total_liabilities",
            "ifrs-full_CurrentLiabilities":                                             "current_liabilities",
            "ifrs-full_TradeAndOtherCurrentPayables":                                  "accounts_payable",
            "ifrs-full_TradeAndOtherPayables":                                          "accounts_payable",
            "ifrs-full_ShorttermBorrowings":                                            "st_borrowings",
            "ifrs-full_CurrentPortionOfLongtermBorrowings":                            "current_lt_debt",
            "ifrs-full_AccruedLiabilities":                                             "accrued_liabilities",
            "ifrs-full_NoncurrentLiabilities":                                          "noncurrent_liabilities",
            "ifrs-full_LongtermBorrowings":                                             "lt_borrowings",
            "ifrs-full_Borrowings":                                                     "lt_borrowings",
            "ifrs-full_DebtSecurities":                                                 "bonds_payable",
            "ifrs-full_Equity":                                                         "total_equity",
            "ifrs-full_EquityAttributableToOwnersOfParent":                            "controlling_equity",
            "ifrs-full_NoncontrollingInterests":                                        "nci_equity",
            "ifrs-full_IssuedCapital":                                                  "capital_stock",
            "ifrs-full_SharePremium":                                                   "capital_surplus",
            "ifrs-full_RetainedEarnings":                                               "retained_earnings",
            "ifrs-full_TreasuryShares":                                                 "treasury_shares",
            # ── 현금흐름표 (CF) ──────────────────────────────────────────────
            "ifrs-full_CashFlowsFromUsedInOperatingActivities":                        "operating_cf",
            "ifrs-full_CashFlowsFromUsedInInvestingActivities":                        "investing_cf",
            "ifrs-full_CashFlowsFromUsedInFinancingActivities":                        "financing_cf",
            "ifrs-full_AdjustmentsForDepreciationAndAmortisationExpense":              "da_total",
            "ifrs-full_AdjustmentsForDepreciationExpense":                             "depreciation",
            "ifrs-full_AdjustmentsForAmortisationExpense":                             "amortization",
            "ifrs-full_PurchaseOfPropertyPlantAndEquipment":                           "capex_ppe",
            "ifrs-full_PurchaseOfIntangibleAssets":                                    "capex_intangible",
            "ifrs-full_DividendsPaid":                                                  "dividends_paid",
            "ifrs-full_IncreaseDecreaseInCashAndCashEquivalents":                      "net_cash_change",
            "ifrs-full_CashAndCashEquivalentsAtBeginningOfPeriod":                    "cash_beginning",
            "ifrs-full_CashAndCashEquivalentsAtEndOfPeriod":                          "cash_ending",
            "ifrs-full_EffectOfExchangeRateChangesOnCashAndCashEquivalents":          "fx_effect",
            "ifrs-full_IncomeTaxesPaidRefundClassifiedAsOperatingActivities":         "tax_paid",
            "ifrs-full_PurchaseOfShorttermInvestments":                               "purchase_st_investments",
            "ifrs-full_ProceedsFromSalesOfShorttermInvestments":                      "proceeds_st_investments",
        }

        # account_nm 키워드 폴백 매핑 (XBRL ID 매핑 실패 시)
        # 중요: 긴/구체적 키워드를 짧은 키워드보다 앞에 배치 (부분 일치 오염 방지)
        NM_FALLBACK: list[tuple[str, str]] = [
            ("매출총이익",       "gross_profit"),      # '매출' 보다 먼저
            ("매출채권",         "accounts_receivable"),
            ("매출원가",         "cogs"),
            ("매출액",           "revenue"),
            ("영업수익",         "revenue"),           # 금융업
            ("영업비용",         "cogs"),              # 금융업
            ("판매비와관리비",   "sga"),
            ("판매비",           "sga"),
            ("영업이익",         "operating_income"),
            ("이자수익",         "interest_income"),
            ("이자비용",         "interest_expense"),
            ("금융수익",         "finance_income"),
            ("금융비용",         "finance_costs"),
            ("금융원가",         "finance_costs"),
            ("지분법",           "equity_method_income"),
            ("법인세차감전",     "ebt"),
            ("법인세비용",       "income_tax"),
            ("당기순이익",       "net_income"),
            ("자산총계",         "total_assets"),
            ("유동자산",         "current_assets"),
            ("현금및현금성자산", "cash"),
            ("단기금융상품",     "st_investments"),
            ("재고자산",         "inventory"),
            ("비유동자산",       "noncurrent_assets"),
            ("유형자산",         "ppe"),
            ("무형자산",         "intangibles"),
            ("부채총계",         "total_liabilities"),
            ("유동부채",         "current_liabilities"),
            ("매입채무",         "accounts_payable"),
            ("단기차입금",       "st_borrowings"),
            ("유동성장기부채",   "current_lt_debt"),
            ("비유동부채",       "noncurrent_liabilities"),
            ("장기차입금",       "lt_borrowings"),
            ("사채",             "bonds_payable"),
            ("자본총계",         "total_equity"),
            ("자본금",           "capital_stock"),
            ("자본잉여금",       "capital_surplus"),
            ("이익잉여금",       "retained_earnings"),
            ("감가상각비",       "depreciation"),
            ("무형자산상각",     "amortization"),
            ("유형자산의 취득",  "capex_ppe"),
            ("유형자산 취득",    "capex_ppe"),
            ("무형자산의 취득",  "capex_intangible"),
            ("무형자산 취득",    "capex_intangible"),
            ("배당금",           "dividends_paid"),
            ("영업활동",         "operating_cf"),
            ("투자활동",         "investing_cf"),
            ("재무활동",         "financing_cf"),
        ]

        def _parse_amount(val: str | None) -> int | None:
            if not val:
                return None
            cleaned = val.replace(",", "").replace(" ", "").replace("−", "-").replace("△", "-")
            try:
                return int(cleaned)
            except ValueError:
                return None

        def _map_items(items: list[dict]) -> dict[str, int | None]:
            """account_id → NM 폴백 순으로 매핑. 먼저 만나는 값 우선."""
            result: dict[str, int | None] = {}
            seen_fields: set[str] = set()

            # 1차: XBRL account_id 매핑
            for item in items:
                aid = item.get("account_id", "")
                field = XBRL_MAP.get(aid)
                if field and field not in seen_fields:
                    amt = _parse_amount(item.get("thstrm_amount"))
                    if amt is not None:
                        result[field] = round(amt / unit) if unit != 1 else amt
                        seen_fields.add(field)

            # 2차: account_nm 키워드 폴백 (XBRL 매핑 실패한 필드만)
            for item in items:
                nm = item.get("account_nm", "")
                for keyword, field in NM_FALLBACK:
                    if field in seen_fields:
                        continue
                    if keyword in nm:
                        amt = _parse_amount(item.get("thstrm_amount"))
                        if amt is not None:
                            result[field] = round(amt / unit) if unit != 1 else amt
                            seen_fields.add(field)
                        break

            return result

        # ── 연도별 병렬 조회 ───────────────────────────────────────────────
        async def _fetch_year(year: int) -> tuple[int, dict]:
            data = await self.get_financial_statements(
                corp_code, str(year), report_code, fs_type
            )
            return year, data

        responses = await asyncio.gather(
            *[_fetch_year(y) for y in years], return_exceptions=True
        )

        # ── 연도별 파싱 ───────────────────────────────────────────────────
        unit_label = {1: "원", 1_000: "천원", 1_000_000: "백만원", 100_000_000: "억원"}.get(unit, f"/{unit}원")
        results_by_year: dict[int, dict] = {}
        errors: dict[int, str] = {}

        for idx, resp in enumerate(responses):
            year = years[idx]
            if isinstance(resp, Exception):
                errors[year] = str(resp)
                continue
            year_data, data = resp  # _fetch_year returns (year, data)
            if data.get("status") not in ("000", None) and data.get("status") != "000":
                errors[year] = data.get("message", f"status={data.get('status')}")
                continue

            items = data.get("list", [])
            by_sj: dict[str, list[dict]] = {}
            for item in items:
                sj = item.get("sj_div", "")
                by_sj.setdefault(sj, []).append(item)

            is_data  = _map_items(by_sj.get("IS",  []) + by_sj.get("CIS", []))
            bs_data  = _map_items(by_sj.get("BS",  []))
            cf_data  = _map_items(by_sj.get("CF",  []))

            # EBITDA = 영업이익 + 감가상각비 + 무형자산상각비
            oi   = is_data.get("operating_income")
            dep  = cf_data.get("depreciation") or cf_data.get("da_total")
            amor = cf_data.get("amortization")
            if oi is not None and dep is not None:
                ebitda = oi + dep + (amor or 0)
                is_data["ebitda"] = ebitda

            # 이자발생부채(IBD) = 단기차입금 + 유동성장기부채 + 장기차입금 + 사채
            st  = bs_data.get("st_borrowings", 0) or 0
            clt = bs_data.get("current_lt_debt", 0) or 0
            lt  = bs_data.get("lt_borrowings", 0) or 0
            bnd = bs_data.get("bonds_payable", 0) or 0
            if st or clt or lt or bnd:
                bs_data["interest_bearing_debt"] = st + clt + lt + bnd

            # 순부채(Net Debt) = IBD - 현금 - 단기금융상품
            ibd  = bs_data.get("interest_bearing_debt", 0) or 0
            cash = bs_data.get("cash", 0) or 0
            sti  = bs_data.get("st_investments", 0) or 0
            bs_data["net_debt"] = ibd - cash - sti

            # FCF = 영업CF - |CAPEX_PPE| - |CAPEX_intangible|
            # DART CF 항목: 취득(지출)은 음수, 수입은 양수로 저장됨
            # capex 항목이 양수로 저장된 경우(일부 회사) 음수 처리 필요
            ocf     = cf_data.get("operating_cf")
            cap_ppe = cf_data.get("capex_ppe")
            cap_int = cf_data.get("capex_intangible")
            if ocf is not None and cap_ppe is not None:
                # 양수로 저장된 경우 음수로 전환 (절대값 차감)
                cap_ppe_adj = -abs(cap_ppe)
                cap_int_adj = -abs(cap_int) if cap_int else 0
                cf_data["fcf"] = ocf + cap_ppe_adj + cap_int_adj

            results_by_year[year_data] = {
                "income_statement": is_data,
                "balance_sheet": bs_data,
                "cash_flow": cf_data,
            }

        # ── 필드 메타데이터 (영문→한글 라벨) ─────────────────────────────
        FIELD_LABELS: dict[str, str] = {
            # IS
            "revenue": "매출액", "cogs": "매출원가", "gross_profit": "매출총이익",
            "sga": "판매비와관리비", "operating_income": "영업이익(EBIT)",
            "finance_income": "금융수익", "finance_costs": "금융비용",
            "interest_income": "이자수익", "interest_expense": "이자비용",
            "equity_method_income": "지분법손익", "other_net": "기타손익",
            "ebt": "법인세차감전순이익", "income_tax": "법인세비용",
            "net_income": "당기순이익", "ni_controlling": "지배주주순이익",
            "ni_nci": "비지배주주순이익", "ebitda": "EBITDA",
            # BS
            "total_assets": "자산총계", "current_assets": "유동자산",
            "cash": "현금및현금성자산", "st_investments": "단기금융상품",
            "accounts_receivable": "매출채권(순)", "inventory": "재고자산",
            "noncurrent_assets": "비유동자산", "ppe": "유형자산",
            "intangibles": "무형자산", "goodwill": "영업권",
            "total_liabilities": "부채총계", "current_liabilities": "유동부채",
            "accounts_payable": "매입채무", "st_borrowings": "단기차입금",
            "current_lt_debt": "유동성장기부채", "accrued_liabilities": "미지급금",
            "noncurrent_liabilities": "비유동부채", "lt_borrowings": "장기차입금",
            "bonds_payable": "사채", "interest_bearing_debt": "이자발생부채(IBD)",
            "net_debt": "순부채(Net Debt)",
            "total_equity": "자본총계", "controlling_equity": "지배주주자본",
            "nci_equity": "비지배지분자본", "capital_stock": "자본금",
            "capital_surplus": "자본잉여금", "retained_earnings": "이익잉여금",
            "treasury_shares": "자기주식",
            # CF
            "operating_cf": "영업활동현금흐름", "investing_cf": "투자활동현금흐름",
            "financing_cf": "재무활동현금흐름", "depreciation": "감가상각비",
            "amortization": "무형자산상각비", "da_total": "감가상각비(합계)",
            "capex_ppe": "유형자산취득(CAPEX)", "capex_intangible": "무형자산취득",
            "dividends_paid": "배당금지급", "net_cash_change": "현금순증감",
            "cash_beginning": "기초현금", "cash_ending": "기말현금",
            "fx_effect": "환율변동효과", "tax_paid": "법인세납부",
            "fcf": "잉여현금흐름(FCF)",
        }

        return {
            "corp_code": corp_code,
            "years": sorted(results_by_year.keys()),
            "unit": unit_label,
            "fs_type": fs_type,
            "data": {str(y): results_by_year[y] for y in sorted(results_by_year.keys())},
            "field_labels": FIELD_LABELS,
            "errors": errors if errors else None,
            "note": (
                "XBRL account_id 기반 매핑. 매핑 실패 시 account_nm 키워드 폴백 적용. "
                "금융업(은행·증권·보험)은 DART XBRL 미제공으로 조회 불가."
            ),
        }

    async def aclose(self) -> None:
        await self._client.aclose()
