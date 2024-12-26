
###############################################################################
# Import the required packages.
###############################################################################
import sys
from pathlib import Path

###############################################################################
# Import the required function.
###############################################################################

# BATCH MODE: Specify the codebase project folder path.
src_path = f'{sys.argv[1]}\create_abt\src'

# Import the required function.
sys.path.append(src_path)
from def_getKeyMetricABT_v1 import getKeyMetricABT_qtr

###############################################################################
# BATCH MODE: Run the function that creates the quarterly key metric stats for 
# stocks. 
###############################################################################
indf, keyMetricABT = getKeyMetricABT_qtr(
    symbol_filters  = [],
    in_df           = '',
    in_fp           = sys.argv[2],      
    in_company_fp   = sys.argv[3],
    in_piotroski_fp = sys.argv[4],   
    in_price_fp     = sys.argv[5],
    min_date        = sys.argv[6],
    max_date        = '',
    outpath         = sys.argv[7],
    outdsn_parquet  = sys.argv[8],
    outdsn_csv      = sys.argv[9]   
)


###############################################################################
# MANUAL MODE: Run the function that creates the quarterly income statement
# statistics. 
###############################################################################

# Import packages.
import sys
from pathlib import Path
src_path = 'C:\codebase\create_abt\src'
sys.path.append(src_path)
from def_getKeyMetricABT_v1 import getKeyMetricABT_qtr

# Create the quarterly Key Metrics for stocks.
indf, keyMetricABT = getKeyMetricABT_qtr(
    symbol_filters  = [],
    in_df           = '',
    in_fp           = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\keyMetrics_qtr_fmp_stock.parquet',      
    in_company_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
    in_piotroski_fp = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\FIN_ABT\piotroskiABT_qtr_stock.parquet',   
    in_price_fp     = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\PRICE_ABT\priceABT_month_stock.parquet',
    min_date        = '2020-01-01',
    max_date        = '',
    outpath         = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\FIN_ABT',
    outdsn_parquet  = 'keyMetricABT_qtr_stock.parquet',
    outdsn_csv      = 'keyMetricABT_qtr_stock.csv'   
)






