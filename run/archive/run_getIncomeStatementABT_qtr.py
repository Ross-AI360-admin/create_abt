
###############################################################################
# Import the required packages.
###############################################################################
import sys
from pathlib import Path

###############################################################################
# Import the required function.
###############################################################################

# BATCH MODE: Specify the codebase project folder path.
# src_path = f'{sys.argv[1]}\pull_data\src'

# MANUALMODE: Specify the codebase project folder path.
src_path = 'C:\codebase\pull_data\src'

# Import the required function.
sys.path.append(src_path)
from def_getFinancialStatementStats_v1 import getIncomeStatementStats_qtr

###############################################################################
# MANUAL MODE: Run the function that creates the quarterly income statement
# statistics. 
###############################################################################

# Income Statements (Quarterly)
is_stats_qtr = getIncomeStatementStats_qtr(
    in_df          = '',
    in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\incomeStatements_qtr_fmp_stock.parquet',      
    in_company_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
    min_date       = '2020-01-01',
    max_date       = '',
    outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY',
    outdsn_parquet = 'incomeStatementStats_qtr_fmp_stock.parquet',
    outdsn_csv     = 'incomeStatementStats_qtr_fmp_stock.csv'   
)

