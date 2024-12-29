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
# sys.arv[8] = The name of the output parquet file containing the Piotroski score ABT
# sys.arv[9] = The name of the output csv file containing the Piotroski score ABT
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
from def_getPiotroskiABT_v1 import *

# Run the function to create quarterly annualized Piotroski scores.
print(f"\nRunning the code that computes the Piotroski Scores.")
print(f"is_fp = {sys.argv[2]}")
print(f"bs_fp = {sys.argv[3]}")
print(f"cf_fp = {sys.argv[4]}")
print(f"in_company_fp = {sys.argv[5]}")
print(f"out_path = {sys.argv[7]}")
print(f"outdsn_parquet = {sys.argv[8]}")
print(f"outdsn_csv     = {sys.argv[9]}\n")
scores_df = getPiotroskiABT(
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
# MANUAL MODE: Run the function that calculates the Piotroski scores. 
############################################################################### 

# Create the Piotroski scores for stock data using quarterly financial statements.
# import sys
# from pathlib import Path
# src_path = 'C:/codebase/create_abt/src'
# sys.path.append(src_path)
# from def_getPiotroskiABT_v1 import *

# scores_df = getPiotroskiABT(
#     is_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/incomeStatements_qtr_fmp_stock.parquet',
#     bs_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/balanceSheets_qtr_fmp_stock.parquet',
#     cf_fp          = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/FINANCIAL/QUARTERLY/cashflows_qtr_fmp_stock.parquet',
#     in_company_fp  = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/COMPANY/companyOverviews_fmp_stock.parquet',    
#     min_date       = '2018-01-01',
#     max_date       = '',     
#     outpath        = r'C:/Users/sharo/OneDrive - aiinvestor360.com/DATA/ABT/FIN_ABT',
#     outdsn_parquet = 'piotroskiABT_qtr_stock.parquet',
#     outdsn_csv     = 'piotroskiABT_qtr_stock.csv'      
# )



