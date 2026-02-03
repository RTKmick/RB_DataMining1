@echo off
cd /d C:\ngrok\RB_DataMining

:: 寫入 Log
echo %date% %time% - Run [StartCentor] (start_rb) >> Log.txt

:: 先同步日誌回 GitHub
git add Log.txt
git commit -m "Log: Start_log"
git push origin master

:: 啟動程序
start cmd /k "cd C:\ngrok\ && ngrok http 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"

echo 🚀 RB TradingCentor with ngrok started and log！