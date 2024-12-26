
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
from def_getPiotroskiABT_v1 import *

###############################################################################
# BATCH MODE: Run the function that calculates the Piotroski scores. 
############################################################################### 
print(f"\nRunning the code that computes the Piotroski Scores.")
print(f"is_fp = {sys.argv[2]}")
print(f"bs_fp = {sys.argv[3]}")
print(f"cf_fp = {sys.argv[4]}")
print(f"out_path = {sys.argv[5]}")
print(f"outdsn_parquet = {sys.argv[6]}")
print(f"outdsn_csv     = {sys.argv[7]}\n")
scores_df = getPiotroskiABT(
    is_fp          = sys.argv[2],
    bs_fp          = sys.argv[3],
    cf_fp          = sys.argv[4],
    outpath        = sys.argv[5],
    outdsn_parquet = sys.argv[6],
    outdsn_csv     = sys.argv[7]     
)

###############################################################################
# MANUAL MODE: Run the function that calculates the Piotroski scores. 
############################################################################### 

# Import packages.
# import sys
# from pathlib import Path
# src_path = 'C:\codebase\create_abt\src'
# sys.path.append(src_path)
# from def_getPiotroskiABT_v1 import *

# Create the Piotroski scores for stock data using quarterly financial statements.
# scores_df = getPiotroskiABT(
#     is_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\incomeStatements_qtr_fmp_stock.parquet',
#     bs_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\balanceSheets_qtr_fmp_stock.parquet',
#     cf_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\cashflows_qtr_fmp_stock.parquet',
#     outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\FIN_ABT',
#     outdsn_parquet = 'piotroskiABT_qtr_stock.parquet',
#     outdsn_csv     = 'piotroskiABT_qtr_stock.csv'      
# )



