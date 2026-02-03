import requests
import pandas as pd
from datetime import datetime

def get_twse_investors(date_str):
    """
    抓取指定日期的三大法人買賣超
    格式: YYYYMMDD (例如 20260203)
    """
    url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?date={date_str}&response=json"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if data['stat'] == 'OK':
            # 轉換成 Pandas 表格方便觀察
            df = pd.DataFrame(data['data'], columns=data['fields'])
            print(f"📊 {date_str} 三大法人統計：")
            print(df[['單位名稱', '買進金額', '賣出金額', '買賣差額']])
            return df
        else:
            print("⚠️ 該日期查無資料（可能是假日或未開盤）")
    except Exception as e:
        print(f"❌ 爬取失敗: {e}")

# 測試抓取今天（或最近一個交易日）0203
if __name__ == "__main__":
    today = datetime.now().strftime("%Y%m%d")
    get_twse_investors(today)