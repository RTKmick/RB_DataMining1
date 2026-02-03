@echo off
cd /d C:\ngrok\RB_DataMining

echo 🚀 準備上傳變動...

:: 寫入 Log
echo %date% %time% - 執行 [備份上傳] (upl_rb) >> Log.txt

:: 全部上傳（含 Log.txt）
git add .
git commit -m "V1.0.0 - %date% %time% 更新 (含日誌)"
git push origin master

echo ✅ 上傳與日誌更新完成！
pause