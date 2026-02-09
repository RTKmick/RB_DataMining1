import time
import requests
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TDR_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class FinMindClient:
    def __init__(self, token: str, verify_ssl: bool = True):
        self.token = (token or "").strip()
        self.verify_ssl = verify_ssl
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.0,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        if self.token:
            session.headers.update({"Authorization": f"Bearer {self.token}"})
        return session

    def _log(self, tag: str, params: dict, http_status: int, api_status, msg: str, latency: float):
        dataset = params.get("dataset", "")
        data_id = params.get("data_id", "")
        date = params.get("date", "")
        start_date = params.get("start_date", "")
        end_date = params.get("end_date", "")
        print(
            f"[{now_str()}] {tag} | http={http_status} api={api_status} "
            f"| dataset={dataset} data_id={data_id} date={date} "
            f"| start={start_date} end={end_date} | latency={latency:.2f}s | msg={msg}"
        )

    def request_data(self, dataset: str, data_id: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        params = {"dataset": dataset}
        if data_id:
            params["data_id"] = data_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_V4_DATA_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("DATA", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("DATA", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()
        except Exception as e:
            latency = time.time() - t0
            self._log("DATA", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()

    def request_trading_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        params = {"data_id": stock_id, "date": date_yyyy_mm_dd}
        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_TDR_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("TDR", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("TDR", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))

            if api_status in (401, 402, 403) or resp.status_code in (401, 402, 403):
                print(f"⚠️ 分點資料需要 sponsor 權限或 token 無效 | api_status={api_status} | msg={msg}")
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("TDR", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()
