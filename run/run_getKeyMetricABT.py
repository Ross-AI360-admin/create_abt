###################################################################################################
###################################################################################################
# Batch Parameters:
#
# sys.arv[1] = The path to the top-level codebase project folder
# sys.arv[2] = The complete filepath to the quarterly key metrics data
# sys.arv[3] = The complete filepath to the company overview data 
# sys.arv[4] = The minimum date filter that is applied to the output data
# sys.arv[5] = The complete folderpath where the output parquet and csv files will be saved
# sys.arv[6] = The name of the output parquet file containing the key metric ABT
# sys.arv[7] = The name of the output csv file containing the key metric ABT
###################################################################################################
###################################################################################################

###############################################################################
# BATCH MODE: Import the required packages and functions.
###############################################################################

# Import the required packages.
import sys
from pathlib import Path

# Import the required functions.
src_path = f'{sys.argv[1]}\create_abt\src'
sys.path.append(src_path)
from def_getKeyMetricABT_v1 import getKeyMetricABT_qtr

# Run the function that creates the quarterly key metric stats for stocks. 
keyMetricABT = getKeyMetricABT_qtr(
    symbol_filters  = [],
    in_df           = '',
    in_fp           = sys.argv[2],      
    in_company_fp   = sys.argv[3],
    min_date        = sys.argv[4],
    max_date        = '',
    outpath         = sys.argv[5],
    outdsn_parquet  = sys.argv[6],
    outdsn_csv      = sys.argv[7]   
)

###############################################################################
# MANUAL MODE: Run the function that creates the quarterly key metric stats for
# stocks. 
###############################################################################

# Create the quarterly Key Metrics for stocks dataframe only.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getKeyMetricABT_v1 import getKeyMetricABT_qtr

# keyMetricABT = getKeyMetricABT_qtr(
#     symbol_filters  = ['AAPL','MSFT'],
#     in_df           = '',
#     in_fp           = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/keyMetrics_qtr_fmp_stock.parquet',      
#     in_company_fp   = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',
#     min_date        = '2020-01-01',
#     max_date        = '',
#     outpath         = '',
#     outdsn_parquet  = '',
#     outdsn_csv      = ''   
# )

# Create the quarterly Key Metrics for stocks dataframe and save the output files.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getKeyMetricABT_v1 import getKeyMetricABT_qtr

# keyMetricABT = getKeyMetricABT_qtr(
#     symbol_filters  = ['AAPL','MSFT'],
#     in_df           = '',
#     in_fp           = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/keyMetrics_qtr_fmp_stock.parquet',      
#     in_company_fp   = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',
#     min_date        = '2020-01-01',
#     max_date        = '',
#     outpath         = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/FIN_ABT',
#     outdsn_parquet  = 'keyMetricABT_qtr_stock.parquet',
#     outdsn_csv      = 'keyMetricABT_qtr_stock.csv'   
# )




