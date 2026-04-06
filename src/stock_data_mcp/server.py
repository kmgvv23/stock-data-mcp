"""
Korean Stock Data MCP Server

Data sources:
- KRX Open API: stock prices, indices, ETF, investor trading, derivatives
- Open DART API: corporate disclosures, financial statements, shareholders
- Bank of Korea ECOS API: interest rates, exchange rates, macro indicators

Transport modes:
  stdio (default): for Claude Desktop / local MCP clients
  HTTP (--http):   for remote MCP clients like ChatGPT
"""

import sys

from fastmcp import FastMCP

from stock_data_mcp.config import get_settings
from stock_data_mcp.tools.dart_tools import dart
from stock_data_mcp.tools.ecos_tools import ecos
from stock_data_mcp.tools.krx_tools import krx

mcp = FastMCP(
    "Korean Stock Data",
    instructions=(
        "This server provides Korean financial market data from three official sources.\n\n"
        "Tools are prefixed by source:\n"
        "- krx_*  : Korea Exchange (KRX) — stock prices, indices, ETF, investor trading, short selling\n"
        "- dart_* : Open DART (FSS) — corporate disclosures, financial statements, dividends, shareholders\n"
        "- ecos_* : Bank of Korea ECOS — interest rates, exchange rates, CPI, GDP, money supply\n\n"
        "Typical workflow:\n"
        "1. Use dart_get_corp_codes() to find a company's corp_code from its stock ticker\n"
        "2. Use krx_get_stock_ohlcv() for price history (requires ISIN code)\n"
        "3. Use dart_get_financial_statements() for financials\n"
        "4. Use ecos_get_base_rate() / ecos_get_exchange_rate_usd() for macro context\n"
    ),
)

mcp.mount(krx, namespace="krx")
mcp.mount(dart, namespace="dart")
mcp.mount(ecos, namespace="ecos")

# ASGI app for production deployment (uvicorn stock_data_mcp.server:app)
app = mcp.http_app()


def main() -> None:
    settings = get_settings()

    if "--http" in sys.argv or "--sse" in sys.argv:
        mcp.run(
            transport="http",
            host=settings.server_host,
            port=settings.server_port,
        )
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
