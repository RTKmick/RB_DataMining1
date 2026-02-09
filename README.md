For RUMBOR Data Mining
RB_DataMining/
  scraper_chip.py                # 只留 main + run_strategy（流程）
  core/
    __init__.py
core/finmind_client.py：  API I/O、retry、log、回傳 DataFrame
core/adapter_tw.py：      把 API dataset 抽象成 domain method（trading dates、name、daily report）
core/broker_master.py：   靜態資料載入與映射
core/price_data.py：      價格資料抽取 + 欄位 normalize（OHLCV schema 固化）
core/indicators_tv.py：   TV radar 指標（只吃 OHLCV DataFrame，不碰 API）
core/regime.py：          中期 regime（只吃 price_df，不碰 API）
core/signals_whale.py：   分點資料清洗、集中度、廣度、top15、master_trend（只吃 trading df，不碰 API）
core/pipeline.py：        組裝流程（整合 price/regime/tv/whale signals）
core/config.py :          集中所有門檻與權重
core/types.py :           把 signals/pack 的資料契約固定
core/aggregate.py :       把 aggregation 抽成

scraper_chip.py：entrypoint / orchestration（args、run_strategy、輸出 JSON/CSV）

A. 大戶短期力道（Whale）

20 日集中度 WantGoo公式（%）
5 日集中度
NetBuy 1/5/20 日張數
Breadth 买>卖 家數
外資五日主力 vs 本土五日主力
Top6 大戶十日軌跡（量化可視化）
Top15 買 / 賣（含連買/連賣）


C. Regime (中期結構)

趨勢判斷（ma20/60 + slope + breakout120天）
ATR 風險
產生 regime_score（0~100）
趨勢 regime_trend（bull/bear/range/transition）

final_score = 0.6 × whale_score  +  0.4 × regime_score

//---------------------------------------------------------------------
B. TradingView Smart Money Radar（TV 系統）

VWAP 主力成本線站上（sig_vwap）
吸籌偵測（sig_accumulation）
爆量換手（rotation）
HVN 關鍵區（profile zone）
結構突破（BOS）
總分 tv_score 0~5

等級：weak / watch / strong


➜ 20 日 Top15 主力差（張） = 買前15張數 − 賣前15張數 這指標代表：
> 0 → 主力 20 日淨積極收貨
< 0 → 主力 20 日淨賣壓
越大越偏多頭主力行為
貼近 0 → 洗盤 or 中性盤整


python .\scraper_chip.py --stock_id 6239 --days 20 --debug_tv
