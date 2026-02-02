import os
from flask import Flask, request, abort
import shioaji as sj
from telegram import Bot
import asyncio
from dotenv import load_dotenv

# 1. 初始化環境
load_dotenv()
app = Flask(__name__)
tg_bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
chat_id = os.getenv("TELEGRAM_CHAT_ID")

# 2. 永豐金 API 登入函數
def login_shioaji():
    api = sj.Shioaji()
    api.login(api_key=os.getenv("SHIOAJI_API_KEY"), secret_key=os.getenv("SHIOAJI_SECRET_KEY"))
    api.activate_ca(
        ca_path=os.getenv("SHIOAJI_CERT_PATH"),
        ca_passwd=os.getenv("SHIOAJI_CERT_PASSWORD"),
        person_id=os.getenv("SHIOAJI_PERSON_ID")
    )
    return api

# 3. 接收 TradingView 訊號的路徑
@app.route('/webhook', methods=['POST'])
def webhook():
    # 取得訊號文字內容
    data = request.get_data(as_text=True)
    
    # 發送通知到 Telegram (非同步執行)
    asyncio.run(tg_bot.send_message(chat_id=chat_id, text=f"🔔 潤鉑訊號通知：\n{data}"))
    
    # 這裡未來可以加入 api.place_order() 的下單邏輯
    print(f"收到訊號: {data}")
    return "OK", 200

if __name__ == '__main__':
    # 僅供測試開發使用，正式生產環境會改用 gunicorn
    app.run(host='0.0.0.0', port=5000)
