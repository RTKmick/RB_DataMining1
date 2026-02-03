@echo off
start cmd /k "cd C:\ngrok\ && ngrok http 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"
echo 🚀 RB 交易中心與 ngrok 已啟動！