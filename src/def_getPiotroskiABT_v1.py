############################################################################################################
############################################################################################################
# FUNCTION DEFINITION: getPiotroskiABT()
#
# DESCRIPTION: This function takes income statements, balance sheets, and cashflows as input and then 
# computes the Piotroski score for each stock. The Piotroski is discrete score that represents the overall
# financial health of a company. The scores range from 0 to 9, with a score of 9 being the best score and 
# a score of 0 being the worst score. The scoring method takes into account 9 individual scores, where each
# score measures a specific financial metric. Some of the scores represent the trend of a specific metric
# between the current period and the previous period 1-year ago. The total score results from summing up
# the 9 individual scores. If a company has a score of eight or nine, then it is considered to have to be 
# in great financial shape. If a company has a score of between zero and two points, it financially 
# unstable. 
#
# PIOTROSKI RULE DATA SOURCES: is = income statement, bs = balance sheet, cf = cashflow statement 
#   CR1: netIncome (is)
#   CR2: totalAssets CY/PY (bs), netIncome(is)
#   CR3: operatingCashFlow (cf)
#   CR4: cashflow from operations > net income
#   CR5: longTermDebt CY/PY (bs) 
#   CR6: totalAssets CY/PY (bs), totalLiabilities CY/PY (bs), minorityInterest CY/PY (bs)
#   CR7: weightedAverageShsOut CY/PY (is)    
#   CR8: grossProfitit CY/PY (is), totalRevenue CY/PY (is)
#   CR9: totalAssets CY/PY/PPY (bs), totalRevenue CY/PY (is)
#
# DOCUMENTATION: https://www.investopedia.com/terms/p/piotroski-score.asp
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
#   - min_date       = the minimum date filter to apply to the data (optional)
#   - max_date       = the maximum date filter to apply to the data (optional)
#   - outpath        = the folder path where the output data is saved (optional)
#   - outdsn_parquet = the name of the output parquet file (optional)
#   - outdsn_csv     = the name of the output csv file (optional)
#
# FUNCTION DEPENDENCIES: This function calls the function computePiotroskiRules(),
# which is defined in below in this file.
#
# OUTPUT DATA SCHEMA: see API documentation for complete data schemas
#   - symbol (str)
#   - date (date)
#
# OUTPUT DATAFRAMES
#   - out_df 
#
# OUTPUT FILES
#   - outdsn_parquet (optional)
#   - outdsn_csv (optional)
############################################################################################################
############################################################################################################
def getPiotroskiABT(
    symbol_filters = [],    
    is_df          = '',
    bs_df          = '',
    cf_df          = '',         
    is_fp          = '',
    bs_fp          = '',
    cf_fp          = '',
    in_company_fp  = '',
    min_date       = '2015-01-01',
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

    ###################################################################
    # Keep only the columns that are needed to compute the Piotroski 
    # score.
    ###################################################################
    is_cols = ['date','symbol','netIncome','numShares','revenue','grossProfitRatio']
    is_df = is_df[is_cols]
    
    bs_cols = ['date','symbol','totalAssets','longTermDebt','totalLiabilities','minorityInterest']
    bs_cols += ['cashAndCashEquivalents','shortTermInvestments','netReceivables','totalCurrentLiabilities']
    bs_df = bs_df[bs_cols]
    
    cf_cols = ['date','symbol','operatingCashFlow']
    cf_df = cf_df[cf_cols] 

    ###################################################################
    # Merge the 3 financial statement dataframes into one overall 
    # dataframe.
    ###################################################################
    out_df = pd.merge(is_df, bs_df, on=['symbol','date'], how='left') 
    out_df = pd.merge(out_df, cf_df, on=['symbol','date'], how='left') 

    ###################################################################
    # Create the rolling 1-year financial metric columns by summing the 
    # last 4 quarterly values. Note that in some cases, the last 4 
    # reported values in the data might not be a 1-year period. We 
    # perform the aggregation on those columns that are quarterly 
    # values, as opposed to those columns that are already rolling 
    # 1-year values. We drop the quarterly measurement columns as the  
    # last step, since they are no longer needed.
    ###################################################################
    for colname in ['netIncome','revenue','operatingCashFlow']:
        
        # Rename the key metric columms to denote they are quarterly measurements.
        out_df.rename(columns={colname:'qtr_'+colname}, inplace=True) 
        
        # Aggregate the values of the metrics over the last 4 quarters. 
        temp_col = out_df.groupby(['symbol'])['qtr_'+colname].rolling(4,min_periods=4).sum()
        out_df[colname] = temp_col.reset_index(level=0,drop=True)
        
        # Drop the quarterly metricl columns, since they are no longer needed.
        out_df = out_df.drop(['qtr_'+colname], axis=1)

    ###################################################################
    # Create any additional columns that are needed to compute the 
    # Piotroski score. Note that to get the measurement values from  
    # 1-year ago by lagging 4 quarters.
    ###################################################################
    out_df['grossProfitRatio_lag4'] = out_df.groupby(['symbol'])[['grossProfitRatio']].shift(4)
    
    out_df['totalAssets_lag4'] = out_df.groupby(['symbol'])['totalAssets'].shift(4)
    
    out_df['returnOnAssets'] = out_df['netIncome'] / ((out_df['totalAssets']+out_df['totalAssets_lag4'])/2)
    out_df['returnOnAssets_lag4'] = out_df.groupby(['symbol'])['returnOnAssets'].shift(4)
    
    out_df['longTermDebt_lag4'] = out_df.groupby(['symbol'])['longTermDebt'].shift(4)
    
    out_df['currentRatio'] = out_df['totalAssets'] / (out_df['totalLiabilities']-out_df['minorityInterest'])
    out_df['currentRatio_lag4'] = out_df.groupby(['symbol'])[['currentRatio']].shift(4)
    
    out_df['numShares_lag4'] = out_df.groupby(['symbol'])[['numShares']].shift(4)
    
    out_df['assetTurnover'] = out_df['revenue'] / ((out_df['totalAssets']+out_df['totalAssets_lag4'])/2)
    out_df['assetTurnover_lag4'] = out_df.groupby(['symbol'])['assetTurnover'].shift(4)
    
    out_df['quickRatio'] = out_df['cashAndCashEquivalents'] + out_df['shortTermInvestments'] + out_df['netReceivables']
    out_df['quickRatio'] = out_df['quickRatio'] / out_df['totalCurrentLiabilities']
    
    out_df['longTermDebtToTotalAssetsRatio'] = out_df['longTermDebt'] / out_df['totalAssets']

    ###################################################################
    # Apply the min and max date thresholds, if they were specified.
    ###################################################################
    if len(min_date)>0:
        out_df = out_df.loc[out_df['date']>=pd.to_datetime(min_date)] 
    if len(max_date)>0:
        out_df = out_df.loc[out_df['date']<=pd.to_datetime(max_date)]  
    out_df.sort_values( ['symbol','date'], ascending=[True,True], inplace=True)
    out_df.reset_index(level=0,drop=True,inplace=True)    

    ###################################################################
    # Merge in company overview data, if it is specified.
    ###################################################################
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        in_company_df = in_company_df[['symbol','sector','industry','ipo_date']]
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')

    ###################################################################
    # Compute the Piotroski scores by calling the Piotroski function.
    ###################################################################
    
    # Compute the Piotroski score for each stock.
    out_df[['Piotroski_Score','CR1','CR2','CR3','CR4','CR5','CR6','CR7','CR8','CR9']] = out_df.apply(computePiotroskiRules, axis=1)    

    # Drop the lag columns, since they are not needed anymore.
    out_df = out_df.loc[:, ~out_df.columns.str.endswith('lag4')]

    # Compute the mean of the last 4 Piotroski scores.
    out_df['Piotroski_Score_1yrAvg'] = out_df.groupby(['symbol'])['Piotroski_Score'].rolling(4).mean().reset_index(level=0,drop=True)
    out_df['Piotroski_Score_1yrMin'] = out_df.groupby(['symbol'])['Piotroski_Score'].rolling(4).min().reset_index(level=0,drop=True)
    out_df['Piotroski_Score_1yrMax'] = out_df.groupby(['symbol'])['Piotroski_Score'].rolling(4).max().reset_index(level=0,drop=True)

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
    # Reorder the out_df columns.
    ###################################################################
    col_order=[]
    col_order += ['symbol','date'] 
    col_order += ['nlag','reverse_nlag','max_nlag','firstLast_flag'] 
    col_order += ['Piotroski_Score','Piotroski_Score_1yrAvg','Piotroski_Score_1yrMin','Piotroski_Score_1yrMax']  
    col_order += ['CR1','CR2','CR3','CR4','CR5','CR6','CR7','CR8','CR9']  
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


###############################################################################
###############################################################################
# FUNCTION DEFINITION: computePiotroskiRules()
###############################################################################
###############################################################################
def computePiotroskiRules(row):
    
    # Import packages.
    import pandas as pd
    
    # Initialize the invididual Piotroski scores to 0.
    Piotroski_Score=CR1=CR2=CR3=CR4=CR5=CR6=CR7=CR8=CR9=0
    
    #################################
    # Profilitability Rules
    #################################
    
    # Rule 1: Positive Net Income/ROA in the CY
    if row['netIncome']>0:
        CR1 = 1
    
    # Rule 2: Positive Operating Cashflow in the CY       
    if row['operatingCashFlow']>0:
        CR2 = 1
    
    # Rule 3: Increasing Return on Assets from PY to CY
    if row['returnOnAssets'] > row['returnOnAssets_lag4']>0: 
        CR3 = 1
    
    # Rule 4: Operating Cashflow higher than Net Income in the CY
    if row['operatingCashFlow'] > row['netIncome']:
        CR4 = 1
        
    #################################
    # Leverage, Liquidity & Dilution
    #################################
     
    # Rule 5: Decreasing Long-Term Debt (Leverage) from PY to CY
    if row['longTermDebt'] < row['longTermDebt_lag4']>0: 
        CR5 = 1    
    
    # Rule 6: Increasing Current Ratio (Liquidity) from PY to CY
    if row['currentRatio'] > row['currentRatio_lag4']>0: 
        CR6 = 1    
    
    # Rule 7: Decreasing or No-Change in number of Outstanding Shares from PY to CY
    if row['numShares'] <= row['numShares_lag4']>0: 
        CR7 = 1  
    
    #################################
    # Operating Efficiency
    #################################
    
    # Rule 8: Increasing Gross Profit Ratio from PY to CY
    if row['grossProfitRatio'] > row['grossProfitRatio_lag4']>0: 
        CR8 = 1      
    
    # Rule 9: 
    if row['assetTurnover'] > row['assetTurnover_lag4']>0: 
        CR9 = 1       
    
    #################################
    # Calculate the Piotroski Score.
    #################################
    Piotroski_Score = CR1 + CR2 + CR3 + CR4 + CR5 + CR6 + CR7 + CR8 + CR9    
        
    # if np.isnan(row['netIncome']):    
    
    # Return the row level column values.
    return pd.Series([Piotroski_Score,CR1,CR2,CR3,CR4,CR5,CR6,CR7,CR8,CR9])




