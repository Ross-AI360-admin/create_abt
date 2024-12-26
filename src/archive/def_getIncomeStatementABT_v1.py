###############################################################################
###############################################################################
#
###############################################################################
###############################################################################
def getIncomeStatementStats_qtr(
    in_df          = '',
    in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\incomeStatements_qtr_fmp_stock.parquet',      
    in_company_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
    min_date       = '2018-01-01',
    max_date       = '',
    outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY',
    outdsn_parquet = 'incomeStatementStats_qtr_fmp_stock.parquet',
    outdsn_csv     = 'incomeStatementStats_qtr_fmp_stock.csv'   
):
    
    ###########################################################################
    # Import Packages
    ###########################################################################
    import pandas as pd    
    import numpy as np
    
    ###########################################################################
    # Load input data, if no input dataframe was specfied.
    ###########################################################################
    if len(in_df)==0:
        in_df = pd.read_parquet(in_fp, engine='pyarrow')  
    numStocks = len(pd.unique(in_df['symbol']))
    print(f'The number of distinct stocks in the input list = {numStocks}')

    ###########################################################################
    # Keep only the columns that are needed.
    ###########################################################################
    keeplist  = ['symbol','date','date_qtr','fiscal_qtr']
    keeplist += ['eps_qtr','numShares','revenue','netIncome','netIncomeRatio']
    in_df = in_df[keeplist]
    
    ###########################################################################
    # Sort by symbol and date.
    ###########################################################################  
    in_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    in_df.reset_index(level=0, drop=True, inplace=True)
    
    ###########################################################################
    # Initialize the output dataframe.
    ###########################################################################
    out_df = in_df.copy()

    ###########################################################################
    # Merge in company overview data, if it is specified.
    ###########################################################################
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        in_company_df = in_company_df[['symbol','sector','industry','ipo_date','mktCap','companyName','description']]
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')

    ###########################################################################
    # Rename columns.
    ###########################################################################
    out_df.rename({
        'eps_qtr':'eps_0_1q',
        'revenue':'rev_0_1q',
        'netIncome':'netInc_0_1q',
        'netIncomeRatio':'netIncRatio_0_1q'

    }, axis='columns', inplace=True) 

    ###########################################################################
    # Apply the buffered min and max date thresholds, if they were specified.
    # We use a buffered minimum date threshold, since we will be using lagged 
    # measurements from 2 to 3 years ago.
    ###########################################################################
    buffer_yr = 3
    min_date_buffer = str(int(min_date[0:4])-buffer_yr)+min_date[4:len(min_date)]
    if len(min_date)>0:
        out_df = out_df.loc[out_df['date']>=pd.to_datetime(min_date_buffer)] 
    if len(max_date)>0:
        out_df = out_df.loc[out_df['date']<=pd.to_datetime(max_date)]

    ###########################################################################
    # Create the rolling 1-year aggregate net income column by summing the last  
    # 4 quarterly values. Note that the last 4 reported values in the data 
    # might not represent a 1-year period. Some stocks are missing or don't
    # report quarterly data (ie, they report every 2 quarters). To keep track
    # of the time duration of the last 4 measurements, we create a column that  
    # contains the duration in days of the 4 lagged measurements.  
    ###########################################################################
    
    # EPS: measure quarterly EPS values from last 1-, 2-, and 3-years ago
    out_df['eps_4_5q']= out_df.groupby(['symbol'])['eps_0_1q'].shift(4)
    cond = out_df['eps_4_5q']>0
    out_df['eps_pctChg_0_1q'] = np.where(cond, out_df['eps_0_1q']/out_df['eps_4_5q']-1.0, np.nan)     

    out_df['eps_8_9q']= out_df.groupby(['symbol'])['eps_0_1q'].shift(8)
    cond = out_df['eps_8_9q']>0
    out_df['eps_pctChg_4_5q'] = np.where(cond, out_df['eps_4_5q']/out_df['eps_8_9q']-1.0, np.nan)     

    out_df['eps_pctChg_0_5q'] = pow( (1+out_df['eps_pctChg_0_1q'])*(1+out_df['eps_pctChg_4_5q']) , 1/2 ) - 1

    # EPS: 1-, 2- and 3-year rolling aggregations
    temp_col = out_df.groupby(['symbol'])['eps_0_1q'].rolling(4, min_periods=4).sum()
    out_df['eps_0_1y'] = temp_col.reset_index(level=0,drop=True)
    
    temp_col = out_df.groupby(['symbol'])['eps_0_1q'].rolling(8, min_periods=8).sum()
    out_df['eps_1_2y'] = temp_col.reset_index(level=0,drop=True)
    out_df['eps_1_2y'] = out_df['eps_1_2y'] - out_df['eps_0_1y']       
    cond = out_df['eps_1_2y']>0
    out_df['eps_pctChg_0_1y'] = np.where(cond, out_df['eps_0_1y']/out_df['eps_1_2y']-1.0, np.nan) 

    temp_col = out_df.groupby(['symbol'])['eps_0_1q'].rolling(12, min_periods=12).sum()
    out_df['eps_2_3y'] = temp_col.reset_index(level=0,drop=True)
    out_df['eps_2_3y'] = out_df['eps_2_3y'] - out_df['eps_0_1y'] - out_df['eps_1_2y']
    cond = out_df['eps_2_3y']>0
    out_df['eps_pctChg_1_2y'] = np.where(cond, out_df['eps_1_2y']/out_df['eps_2_3y']-1.0, np.nan) 

    out_df['eps_pctChg_0_2y'] = pow( (1+out_df['eps_pctChg_0_1y'])*(1+out_df['eps_pctChg_1_2y']) , 1/2 ) - 1

    # Create the overall missing data columns for returns and sharpe ratios.
    cond1 = (pd.isnull(out_df['eps_pctChg_0_5q'])==True)
    cond2 = (pd.isnull(out_df['eps_pctChg_0_2y'])==True)
    out_df['dqPass_flag'] = np.where(cond1|cond2 ,'0','1')
    
    ###########################################################################
    # Calculate the number of days from current date to the date 4 periods ago. 
    # We round this number of days to increments of (365/4) days and if its
    # value is within +/- 10 days of the increment value. For companies that 
    # report financials quarterly, the duration after rounding should be 274 
    # days (9 months=365-91). For companies that report financials bi-annually,  
    # the duration should be 639 days (730-91).
    ###########################################################################
    out_df['days_0_1y'] = (out_df['date']-out_df.groupby(['symbol'])['date'].shift(3)).dt.days + 91
    out_df['days_1_2y'] = (out_df['date']-out_df.groupby(['symbol'])['date'].shift(7)).dt.days + 91
    for i in [274,365,456,547,639,730,821,913,1004,1095,1186,1278,1369,1460]:
        out_df['days_0_1y'] = np.where((out_df['days_0_1y']-i).abs()<=10, i, out_df['days_0_1y'])
        out_df['days_1_2y'] = np.where((out_df['days_1_2y']-i).abs()<=10, i, out_df['days_1_2y'])
    
    ###########################################################################
    # Apply the min date thresholds, if they were specified.
    ###########################################################################
    # if len(min_date)>0:
    #     out_df = out_df.loc[out_df['date']>=pd.to_datetime(min_date)]    
    
    ###########################################################################
    # Create a record count (nlag) for each symbol, and then add the max 
    # record count of each symbol and a first/last record flag.
    ###########################################################################
    out_df['nlag'] = out_df.groupby(['symbol']).cumcount()
    out_df['max_nlag'] = out_df.groupby(['symbol'])['nlag'].transform('max')
    conds = [ out_df['nlag']==out_df['max_nlag'], out_df['nlag']==0 ]
    out_df['firstLast_flag'] = np.select(conds, ['L','F'], default='I') 
    
    ###################################################################
    # Reorder the output columns and also only keep columns specified.
    ###################################################################
    col_order = []    
    col_order += ['symbol','date','date_qtr','fiscal_qtr']
    col_order += ['numShares']
    col_order += ['nlag','max_nlag','firstLast_flag']
    col_order += ['dqPass_flag']
    col_order += ['days_0_1y','days_1_2y']
    col_order += ['eps_pctChg_0_1q','eps_pctChg_4_5q','eps_pctChg_0_5q']
    col_order += ['eps_pctChg_0_1y','eps_pctChg_1_2y','eps_pctChg_0_2y']
    col_order += ['eps_0_1q','eps_4_5q','eps_8_9q']
    col_order += ['eps_0_1y','eps_1_2y','eps_2_3y']
    col_order += ['rev_0_1q','netInc_0_1q']
    col_order += ['sector','industry','ipo_date','mktCap','companyName','description']
    col_remain = [col for col in out_df.columns if col not in col_order]
    out_df = out_df[col_order+col_remain] 
    
    ###################################################################
    # SAVE the output dataframe as a file.
    ###################################################################
    
    # Save as a PARQUET file, if one was given.
    curr_len = len(outdsn_parquet)
    if curr_len>=9 and outdsn_parquet[curr_len-8:curr_len]=='.parquet':
        out_parquet = f'{outpath}\{outdsn_parquet}' 
        out_df.to_parquet(f'{out_parquet}',index=False)
    
    # Save as a CSV file, if one was given.
    curr_len = len(outdsn_csv)
    if curr_len>=5 and outdsn_csv[curr_len-4:curr_len]=='.csv':
        out_csv = f'{outpath}\{outdsn_csv}' 
        out_df.to_csv(f'{out_csv}',index=False)       
    
    ###################################################################
    # RETURN the output dataframe.
    ###################################################################
    return out_df 



        