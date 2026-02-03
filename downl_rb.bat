@echo off
cd /d C:\ngrok

if exist "RB_DataMining" (
    echo 📂 偵測到已安裝，正在同步...
    cd RB_DataMining
    git pull origin master
) else (
    echo 📥 偵測到新環境，正在下載...
    git clone https://github.com/RTKmick/RB_DataMining.git
    cd RB_DataMining
)

:: 寫入 Log
echo %date% %time% - 執行 [下載/同步] (downl_rb) >> Log.txt

:: 將 Log 上傳到 GitHub
git add Log.txt
git commit -m "Log: 下載同步紀錄"
git push origin master

echo ✅ 同步與日誌更新完成！
pause