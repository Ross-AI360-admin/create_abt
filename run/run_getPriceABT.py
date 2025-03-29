###################################################################################################
###################################################################################################
# Batch Parameters:
#
# sys.arv[1] = The path to the top-level codebase project folder
# sys.arv[2] = The complete filepath to the monthly price data
# sys.arv[3] = The complete filepath to the company overview data 
# sys.arv[4] = The minimum date filter that is applied to the input monthly price data
# sys.arv[5] = The complete folderpath where the output parquet and csv files will be saved
# sys.arv[6] = The name of the output parquet file containg the price stats data
# sys.arv[7] = The name of the output csv file containing the price stats data
# sys.arv[8] = The complete filepath to the ETF info data (only for creating ETF price stats w/ expense ratio fees) 
###################################################################################################
###################################################################################################

###############################################################################
# BATCH MODE: Import the required packages and functions.
###############################################################################

# Import the required packages.
import sys
from pathlib import Path

# Import the required functions.
src_path = f'{sys.argv[1]}/create_abt/src'
sys.path.append(src_path)
from def_getPriceABT_v1 import getPriceABT

# Run the function to create monthly price statistics.
print(f"\nRunning the code that creates the monthly price stats for both stocks and ETFs.")
print(f"in_fp   = {sys.argv[2]}")
print(f"in_company_fp = {sys.argv[3]}")
print(f"outpath = {sys.argv[5]}")
print(f"outdsn_parquet = {sys.argv[6]}")
print(f"outdsn_csv     = {sys.argv[7]}")
print(f"in_etfinfo_fp  = {sys.argv[8]}")
idf, odf = getPriceABT(
    in_df          = '',   
    in_fp          = sys.argv[2],     
    in_company_fp  = sys.argv[3],
    min_date       = sys.argv[4],
    max_date       = '',
    outpath        = sys.argv[5],
    outdsn_parquet = sys.argv[6],
    outdsn_csv     = sys.argv[7],
    in_etfinfo_fp  = sys.argv[8]
)
print(f"Done.\n")

###############################################################################
# MANUAL MODE: Run the function that creates monthly price statistics. 
############################################################################### 

# STOCK: Create monthly price ABT.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getPriceABT_v1 import getPriceABT

# idf, odf = getPriceABT(
#     in_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_stock.parquet',
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',
#     min_date       = '2016-01-01',
#     max_date       = '',
#     outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/PRICE_ABT',
#     outdsn_parquet = 'priceABT_month_stock.parquet',
#     outdsn_csv     = 'priceABT_month_stock.csv'   
# )

# ETF: Create monthly price ABT w/out adding in ETF info data.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getPriceABT_v1 import getPriceABT

# idf, odf = getPriceABT(
#     in_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_etf.parquet',      
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_etf.parquet',
#     min_date       = '2010-01-01',
#     max_date       = '',
#     outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/PRICE_ABT',
#     outdsn_parquet = 'priceABT_month_etf.parquet',
#     outdsn_csv     = 'priceABT_month_etf.csv',
#     in_etfinfo_fp  = ''   
# )

# ETF: Create monthly price ABT with the ETF info data.
import sys
from pathlib import Path
src_path = 'C:/codebase/create_abt/src'
sys.path.append(src_path)
from def_getPriceABT_v1 import getPriceABT

idf, odf = getPriceABT(
    in_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_etf.parquet',  
    in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_etf.parquet',    
    min_date       = '2018-01-01',
    max_date       = '',
    outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/PRICE_ABT',
    outdsn_parquet = 'priceABT_month_etfInfo.parquet',
    outdsn_csv     = 'priceABT_month_etfInfo.csv',
    in_etfinfo_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ETF_INFO/etfInfo_fmp.parquet'   
)

# ALL: Create monthly price ABT.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getPriceABT_v1 import getPriceABT

# idf, odf = getPriceABT(   
#     in_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/PRICE/MONTHLY/monthlyPrices_av_all.parquet',      
#     min_date       = '2010-01-01',
#     max_date       = '',
#     outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/PRICE_ABT',
#     outdsn_parquet = 'priceABT_month_all.parquet',
#     outdsn_csv     = 'priceABT_month_all.csv'   
# )










