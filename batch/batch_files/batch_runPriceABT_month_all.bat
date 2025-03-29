
@echo off
"C:/Users/sharo/Anaconda3/python.exe" "C:/codebase/create_abt/run/run_getPriceABT.py"^
 C:/codebase^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_stock.parquet^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet^
 2018-01-01^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT^
 priceABT_month_stock.parquet^
 priceABT_month_stock.csv
 
"C:/Users/sharo/Anaconda3/python.exe" "C:/codebase/create_abt/run/run_getPriceABT.py"^
 C:/codebase^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_etf.parquet^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_etf.parquet^
 2018-01-01^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT^
 priceABT_month_etf.parquet^
 priceABT_month_etf.csv

"C:/Users/sharo/Anaconda3/python.exe" "C:/codebase/utility/run/run_combineData.py"^
 C:/codebase^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT/priceABT_month_stock.parquet^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT/priceABT_month_etf.parquet^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT/priceABT_month_all.parquet^
 C:/Users/sharo/OneDrive" - "aiinvestor360.com/DATA/ABT/PRICE_ABT/priceABT_month_all.csv^
 ['asset_type','symbol']

exit