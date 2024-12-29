###################################################################################################
###################################################################################################
# Batch Parameters:
#
# sys.arv[1] = The path to the top-level codebase project folder
# sys.arv[2] = The complete filepath to the quarterly income statement data
# sys.arv[3] = The complete filepath to the quarterly balance sheet data
# sys.arv[4] = The complete filepath to the quarterly cashflow statement data
# sys.arv[5] = The complete filepath to the company overview data 
# sys.arv[6] = The minimum date filter that is applied to the output data
# sys.arv[7] = The complete folderpath where the output parquet and csv files will be saved
# sys.arv[8] = The name of the output parquet file containing the financial statement ABT
# sys.arv[9] = The name of the output csv file containing the financial statement ABT
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
from def_getFinStatementABT_v1 import *

# Run the function to create quarterly financial statement ABT for stocks.
print(f"\nRunning the code that creates the Financial Statement ABT for stocks.")
print(f"is_fp = {sys.argv[2]}")
print(f"bs_fp = {sys.argv[3]}")
print(f"cf_fp = {sys.argv[4]}")
print(f"in_company_fp = {sys.argv[5]}")
print(f"out_path = {sys.argv[7]}")
print(f"outdsn_parquet = {sys.argv[8]}")
print(f"outdsn_csv     = {sys.argv[9]}\n")
fs_df = getFinStatementABT(
    symbol_filters = [],
    is_fp          = sys.argv[2],
    bs_fp          = sys.argv[3],
    cf_fp          = sys.argv[4],
    in_company_fp  = sys.argv[5],    
    min_date       = sys.argv[6],
    max_date       = '',       
    outpath        = sys.argv[7],
    outdsn_parquet = sys.argv[8],
    outdsn_csv     = sys.argv[9]     
)

###############################################################################
# MANUAL MODE: Run the function that calculates the Financial Statement ABTs. 
############################################################################### 

# Create the quarterly financial statement ABT for AAPL and MSFT (dataframe only).
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getFinStatementABT_v1 import *

# fs_df = getFinStatementABT(
#     symbol_filters = ['AAPL','MSFT'],
#     is_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/incomeStatements_qtr_fmp_stock.parquet',
#     bs_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/balanceSheets_qtr_fmp_stock.parquet',
#     cf_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/cashflows_qtr_fmp_stock.parquet',
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',    
#     min_date       = '2020-01-01',
#     max_date       = '',     
#     outpath        = '',
#     outdsn_parquet = '',
#     outdsn_csv     = ''     
# )

# Create the quarterly financial statement ABT for all stocks (dataframe only).
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getFinStatementABT_v1 import *

# fs_df = getFinStatementABT(
#     symbol_filters = ['AAPL','MSFT'],
#     is_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/incomeStatements_qtr_fmp_stock.parquet',
#     bs_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/balanceSheets_qtr_fmp_stock.parquet',
#     cf_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/cashflows_qtr_fmp_stock.parquet',
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',    
#     min_date       = '2020-01-01',
#     max_date       = '',     
#     outpath        = '',
#     outdsn_parquet = '',
#     outdsn_csv     = ''         
# )

# Create the quarterly financial statement ABT for all stocks and save the output as files.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getFinStatementABT_v1 import *

# fs_df = getFinStatementABT(
#     symbol_filters = ['AAPL','MSFT'],
#     is_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/incomeStatements_qtr_fmp_stock.parquet',
#     bs_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/balanceSheets_qtr_fmp_stock.parquet',
#     cf_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/cashflows_qtr_fmp_stock.parquet',
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',    
#     min_date       = '2020-01-01',
#     max_date       = '',     
#     outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/FIN_ABT',
#     outdsn_parquet = 'finStatementABT_qtr_stock.parquet',
#     outdsn_csv     = 'finStatementABT_qtr_stock.csv'     
# )






