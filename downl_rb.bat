@echo off
cd /d C:\ngrok

if exist "RB_DataMining" (
    echo 📂 Installed detected, syncing in progress....
    cd RB_DataMining
    git pull origin master
) else (
    echo 📥 New environment detected, downloading......
    git clone https://github.com/RTKmick/RB_DataMining.git
    cd RB_DataMining
)

:: 寫入 Log
echo %date% %time% - Run [Download/Sync] (downl_rb) >> Log.txt

:: 將 Log 上傳到 GitHub
git add Log.txt
git commit -m "Log: Download sync history"
git push origin master

echo ✅ Synchronization and log update complete!
pause