############################################################################################################
############################################################################################################
# FUNCTION DEFINITION: getFinStatementABT()
#
# DESCRIPTION: This function computes a wide set of summary metrics for the income statement, balance 
# sheet, and cashflow statement data provided by the FMP API service.
#
# FUNCTION INPUT ARGS
#   - symbol_filters = input list of stocks that are used to filter in_df (optional)
#   - is_df          = the income statement dataframe, which takes precedent over is_fp
#   - bs_df          = the balance sheet dataframe, which takes precedent over bs_fp
#   - cf_df          = the cashflow statement dataframe, which takes precedent over cf_fp
#   - is_fp          = the complete filepath to the income statement parquet file
#   - bs_fp          = the complete filepath to the balance sheet statement parquet file
#   - cf_fp          = the complete filepath to the cashflow statement parquet file
#   - in_company_fp  = input company overview complete filepath (optional)
#   - min_date       = the minimum date filter to apply to output data (optional, format is '2018-01-01')
#   - max_date       = the maximum date filter to apply to output data (optional)
#   - outpath        = the folder path where all output data will be saved
#   - outdsn_parquet = the name of output parquet file
#   - outdsn_csv     = the name of the output csv file
#
# OUTPUT DATAFRAMES
#   - out_df
#
# OUTPUT FILES
#   - outdsn_parquet (optional)
#   - outdsn_csv (optional)
############################################################################################################
############################################################################################################
def getFinStatementABT(
    symbol_filters = [],    
    is_df          = '',
    bs_df          = '',
    cf_df          = '',         
    is_fp          = '',
    bs_fp          = '',
    cf_fp          = '',
    in_company_fp  = '',
    min_date       = '2020-01-01',
    max_date       = '',    
    outpath        = '',
    outdsn_parquet = '',
    outdsn_csv     = ''      
):
    
    ###################################################################
    # Import packages.
    ###################################################################
    import pandas as pd
    from pandas.tseries.offsets import MonthEnd
    import numpy as np

    ###################################################################
    # Load input data, if no input dataframe was specfied.
    ###################################################################
    if len(is_df)==0:    
        is_df = pd.read_parquet(is_fp, engine='pyarrow')
    if len(bs_df)==0:     
        bs_df = pd.read_parquet(bs_fp, engine='pyarrow')
    if len(cf_df)==0:    
        cf_df = pd.read_parquet(cf_fp, engine='pyarrow')

    ###########################################################################
    # If a stock filter list was specified, filter the input dataframe.
    ###########################################################################
    if len(symbol_filters)>0:
        symbol_filters = list(map(lambda x: x.upper(),symbol_filters))
        is_df = is_df[is_df['symbol'].isin(symbol_filters)]
        bs_df = bs_df[bs_df['symbol'].isin(symbol_filters)]
        cf_df = cf_df[cf_df['symbol'].isin(symbol_filters)]

    ###########################################################################
    # Sort by symbol and date.
    ###########################################################################  
    is_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    is_df.reset_index(level=0, drop=True, inplace=True)

    bs_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    bs_df.reset_index(level=0, drop=True, inplace=True)
    
    cf_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    cf_df.reset_index(level=0, drop=True, inplace=True)   

    ###########################################################################
    # Keep only the columns that are needed for the desired summaries. 
    ###########################################################################
    is_cols = ['symbol','date','date_qtr','fiscal_year','fiscal_qtr','reportedCurrency']
    is_cols += ['numShares','revenue','netIncome','netIncomeRatio']
    is_df = is_df[is_cols]
    
    bs_cols = ['symbol','date']
    bs_cols += ['totalAssets','totalLiabilities','totalDebt','netDebt']
    bs_df = bs_df[bs_cols]
    
    cf_cols = ['symbol','date','fiscal_year','fiscal_qtr','reportedCurrency']
    cf_df = cf_df[cf_cols] 
    
    ###########################################################################
    # Merge the 3 financial statement dataframes into one overall dataframe
    # and then sort.
    ###########################################################################
    out_df = pd.merge(is_df, bs_df, on=['symbol','date'], how='left')     
    out_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    out_df.reset_index(level=0, drop=True, inplace=True) 

    ###################################################################
    # Apply the min and max date thresholds, if they were specified.
    ###################################################################
    if len(min_date)>0:
        out_df = out_df.loc[out_df['date']>=pd.to_datetime(min_date)] 
    if len(max_date)>0:
        out_df = out_df.loc[out_df['date']<=pd.to_datetime(max_date)]  

    ###########################################################################
    # Create a row record count (nlag) for each symbol, the max record count,
    # the reverese record count, and a first/last record flag.
    ###########################################################################
    out_df['nlag'] = out_df.groupby(['symbol']).cumcount()
    out_df['max_nlag'] = out_df.groupby(['symbol'])['nlag'].transform('max')
    out_df['reverse_nlag'] = out_df['max_nlag'] -out_df['nlag']  
    conds = [ out_df['nlag']==out_df['max_nlag'], out_df['nlag']==0 ]
    out_df['firstLast_flag'] = np.select(conds, ['L','F'], default='I') 

    ###################################################################
    # Merge in company overview data, if it is specified.
    ###################################################################
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        in_company_df = in_company_df[['symbol','sector','industry','ipo_date','isActivelyTrading']]
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')

    ###################################################################
    # Reorder the output columns and also only keep columns specified.
    ###################################################################
    col_order = []    
    col_order += ['symbol','date','date_qtr','fiscal_year','fiscal_qtr']
    col_order += ['max_nlag','firstLast_flag','nlag','reverse_nlag']
    
    col_remain = [col for col in out_df.columns if col not in col_order]
    out_df = out_df[col_order+col_remain] 

    ###################################################################
    # SAVE the output dataframe as a file.
    ###################################################################
    
    # Save as a PARQUET file, if one was given.
    curr_len = len(outdsn_parquet)
    if curr_len>=9 and outdsn_parquet[curr_len-8:curr_len]=='.parquet':
        out_parquet = f'{outpath}/{outdsn_parquet}' 
        out_df.to_parquet(f'{out_parquet}',index=False)
    
    # Save as a CSV file, if one was given.
    curr_len = len(outdsn_csv)
    if curr_len>=5 and outdsn_csv[curr_len-4:curr_len]=='.csv':
        out_csv = f'{outpath}/{outdsn_csv}' 
        out_df.to_csv(f'{out_csv}',index=False)  

    ###################################################################
    # RETURN the output dataframe.
    ###################################################################
    return out_df  




    
    
        
    
    