@echo off
cd /d C:\ngrok\RB_DataMining

:: 寫入 Log
echo %date% %time% - 執行 [啟動中心] (start_rb) >> Log.txt

:: 先同步日誌回 GitHub
git add Log.txt
git commit -m "Log: 啟動紀錄"
git push origin master

:: 啟動程序
start cmd /k "cd C:\ngrok\ && ngrok http 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"

echo 🚀 RB 交易中心與 ngrok 已啟動並記錄日誌！