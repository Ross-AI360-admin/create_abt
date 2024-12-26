
###############################################################################
# Import the required packages.
###############################################################################
import sys
from pathlib import Path

###############################################################################
# Import the required function.
###############################################################################

# BATCH MODE: Specify the codebase project folder path.
src_path = f'{sys.argv[1]}\pull_data\src'

# MANUALMODE: Specify the codebase project folder path.
# src_path = 'C:\codebase\pull_data\src'

# Import the required function.
sys.path.append(src_path)
from def_getPriceStats_v1 import getPriceStats

###############################################################################
# BATCH MODE: Run the function that creates ALL monthly price statistics. 
############################################################################### 
print(f"\nRunning the code that creates the monthly price stats for both stocks and ETFs.")
print(f"in_fp   = {sys.argv[2]}")
print(f"in_company_fp = {sys.argv[3]}")
print(f"outpath = {sys.argv[4]}")
print(f"outdsn_parquet = {sys.argv[5]}")
print(f"outdsn_csv     = {sys.argv[6]}")
idf, odf = getPriceStats(
    in_df          = '',   
    in_fp          = sys.argv[2],     
    in_company_fp  = sys.argv[3],
    min_date       = sys.argv[4],
    max_date       = '',
    outpath        = sys.argv[5],
    outdsn_parquet = sys.argv[6],
    outdsn_csv     = sys.argv[7]   
)
print(f"Done.\n")

###############################################################################
# MANUAL MODE: Run the function that creates monthly price statistics. 
############################################################################### 

# STOCK SAMPLE: Create monthly price stats.
# import pandas as pd
# fp = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_stock.parquet'
# df = pd.read_parquet(fp, engine='pyarrow')
# symbol_list = df['symbol'].unique().tolist()
# sample_df = df[df['symbol'].isin(['AAPL','BWAY','MSFT'])]
# idf, odf = getPriceStats(
#     in_df          = sample_df,    
#     in_company_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
#     min_date       = '2019-01-01',
#     max_date       = '',
#     outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\STATS',
#     outdsn_parquet = 'monthlyPriceStats_av_stock.parquet',
#     outdsn_csv     = 'monthlyPriceStats_av_stock.csv'   
# )

# STOCK: Create monthly price stats.
# idf, odf = getPriceStats(
#     in_df          = '',   
#     in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_stock.parquet',      
#     min_date       = '2010-01-01',
#     max_date       = '',
#     outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\STATS',
#     outdsn_parquet = 'monthlyPriceStats_av_stock.parquet',
#     outdsn_csv     = 'monthlyPriceStats_av_stock.csv'   
# )

# ETF: Create monthly price stats.
# idf, odf = getPriceStats(
#     in_df          = '',   
#     in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_etf.parquet',      
#     min_date       = '2010-01-01',
#     max_date       = '',
#     outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\STATS',
#     outdsn_parquet = 'monthlyPriceStats_av_etf.parquet',
#     outdsn_csv     = 'monthlyPriceStats_av_etf.csv'   
# )

# ALL: Create monthly price stats.
# idf, odf = getPriceStats(
#     in_df          = '',   
#     in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_all.parquet',      
#     min_date       = '2010-01-01',
#     max_date       = '',
#     outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\STATS',
#     outdsn_parquet = 'monthlyPriceStats_av_all.parquet',
#     outdsn_csv     = 'monthlyPriceStats_av_all.csv'   
# )










