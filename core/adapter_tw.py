from datetime import datetime, timedelta
import pandas as pd
from core.finmind_client import FinMindClient


class TaiwanStockAdapter:
    def __init__(self, client: FinMindClient):
        self.client = client

    def get_trading_dates(self, lookback=120) -> list:
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        df = self.client.request_data("TaiwanStockTradingDate", start_date=start_date)
        if df.empty or "date" not in df.columns:
            return []
        return df["date"].astype(str).tolist()

    def get_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        return self.client.request_trading_daily_report(stock_id, date_yyyy_mm_dd)

    def get_stock_name(self, stock_id: str) -> str:
        name = stock_id
        df = self.client.request_data("TaiwanStockInfo", data_id=stock_id)
        if not df.empty:
            if "stock_name" in df.columns and str(df["stock_name"].iloc[0]).strip():
                name = str(df["stock_name"].iloc[0]).strip()
            elif "name" in df.columns and str(df["name"].iloc[0]).strip():
                name = str(df["name"].iloc[0]).strip()
        return name
