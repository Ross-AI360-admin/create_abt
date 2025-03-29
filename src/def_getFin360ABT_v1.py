############################################################################################################
############################################################################################################
# FUNCTION DEFINITION: getStatevectorABT()
#
# DESCRIPTION: This function computes a comprehensive set of performance related metrics by integrating  
# company profile, piotroski score, monthly price, and financial statement data together.
############################################################################################################
############################################################################################################
def getStatevectorABT_qtr(
    symbol_filters  = [],
    in_df           = '',
    in_fp           = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\keyMetrics_qtr_fmp_stock.parquet',      
    in_company_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
    in_piotroski_fp = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY\piotroskiScores_qtr_fmp_stock.parquet',
    in_price_fp     = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_stock.parquet',
    min_date        = '2018-01-01',
    max_date        = '',
    outpath         = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\FINANCIAL\QUARTERLY',
    outdsn_parquet  = 'keyMetricStats_qtr_fmp_stock.parquet',
    outdsn_csv      = 'keyMetricStats_qtr_fmp_stock.csv'   
):
    
    ###########################################################################
    # Import Packages
    ###########################################################################
    import pandas as pd    
    import numpy as np
    import fastparquet as fp
    
    ###########################################################################
    # Load input data from the specified input file, if no input dataframe was 
    # specfied.
    ###########################################################################
    if len(in_df)==0:
        in_df = pd.read_parquet(in_fp, engine='pyarrow')   
    numStocks = len(pd.unique(in_df['symbol']))
    print(f'The number of distinct stocks in the input list = {numStocks}')

    ###########################################################################
    # If a stock filter list was specified, filter the input dataframe.
    ###########################################################################
    if len(symbol_filters)>0:
        symbol_filters = list(map(lambda x: x.upper(),symbol_filters))
        in_df = in_df[in_df['symbol'].isin(symbol_filters)]

    ###########################################################################
    # Keep only the Key Metric columns that are needed ('date_qtr' is removed 
    # b/c it is has a data type in pandas that is not compatible with parquet).
    # Also, we drop marketCap and instead use the market cap coming from the 
    # Company Overview data since it is more reliable.
    ###########################################################################
    keeplist  = ['symbol','date','fiscal_qtr','peRatio']
    keeplist += ['revenuePerShare','netIncomePerShare','cashPerShare','freeCashFlowPerShare','bookValuePerShare']
    keeplist += ['shareholdersEquityPerShare','interestDebtPerShare']    
    keeplist += ['earningsYield','freeCashFlowYield','debtToEquity','debtToAssets']
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
    # Create a year column.
    ###########################################################################
    out_df['date_year'] = out_df['date'].dt.year
                
    ###########################################################################
    # Rename columns.
    ###########################################################################
    out_df.rename({
        'peRatio':'PE',
        'revenuePerShare':'RPS_0_1q',
        'netIncomePerShare':'NIPS_0_1q',
        'cashPerShare':'CPS_0_1q',
        'freeCashFlowPerShare':'FCPS_0_1q',
        'bookValuePerShare':'BVPS_0_1q',
        'shareholdersEquityPerShare': 'SEPS_0_1q',
        'interestDebtPerShare': 'IDPS_0_1q',
        'debtToEquity': 'DtoE',
        'debtToAssets': 'DtoA'
    }, axis='columns', inplace=True) 
     
    ###########################################################################
    # DATA QUALITY: Remove the most recent row of data for a stock if is has
    # incomplete RPS, NIPS, or BVPS data. This is needed since the FMP API can
    # give missing or zero values for key metrics near the time periods when
    # companies are reporting financials.
    ###########################################################################
    
    # Create temporary nlag and max_nlag columns.
    out_df['nlag'] = out_df.groupby(['symbol']).cumcount()
    out_df['max_nlag'] = out_df.groupby(['symbol'])['nlag'].transform('max')
        
    # Get rid of the most recent row of data for a stock if it is incomplete.
    cond1 = (out_df['nlag']==out_df['max_nlag']) 
    cond2 = ((pd.isnull(out_df['RPS_0_1q'])==True)|(pd.isnull(out_df['NIPS_0_1q'])==True)|(pd.isnull(out_df['BVPS_0_1q'])==True))
    cond3 = ((out_df['RPS_0_1q']==0)|(out_df['BVPS_0_1q']==True) )
    out_df = out_df.loc[~(cond1&(cond2|cond3))]
    
    # Drop the temporary columns.
    out_df = out_df.drop(['nlag','max_nlag'], axis=1)
    
    ###########################################################################
    # DELETE HISTORICAL KEY METRIC DATA THAT IS NOT NEEDED: Apply 4-year 
    # buffered min date threshold, if a min date threshold was specified. We  
    # use a buffered minimum date threshold, since we only need enough 
    # historical to compute lagged values up to 4 years ago. For the max date
    # threshold, we just apply this directly as a filter.
    ###########################################################################
    
    # Create the 4-year min date buffer threshold value.
    buffer_yr = 4
    min_date_buffer = str(int(min_date[0:4])-buffer_yr)+min_date[4:len(min_date)]
    
    # Filter the date using the buffer min date and the max date as thresholds.
    if len(min_date)>0:
        out_df = out_df.loc[ out_df['date'] >= pd.to_datetime(min_date_buffer) ] 
    if len(max_date)>0:
        out_df = out_df.loc[ out_df['date'] <= pd.to_datetime(max_date) ]

    # Sort and reindex the data after the filtering step.
    out_df.sort_values(by=['symbol','date'], ascending=[True,True], inplace=True)
    out_df.reset_index(level=0, drop=True, inplace=True) 

    ###########################################################################
    # Merge in the Company Overview data, the Piotroski Score data, and the
    # Stock Price Return Stats data, if specified.
    ###########################################################################
    
    # Merge in the Company Overviews.
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        keeplist = ['symbol','sector','industry','ipo_date','companyName','description']
        keeplist += ['beta','volAvg','mktCap','isActivelyTrading']
        in_company_df = in_company_df[keeplist]
        in_company_df.rename({
            'companyName':'name',
            'description': 'desc',
            'beta': 'curr_beta',
            'volAvg': 'curr_volAvg',
            'mktCap': 'curr_mktCap'
        }, axis='columns', inplace=True)         
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')
       
    # Merge in the Piotroski Scores.    
    curr_len = len(in_piotroski_fp)
    if curr_len>=9 and in_piotroski_fp[curr_len-8:curr_len]=='.parquet':        
        in_piotroski_df = pd.read_parquet(in_piotroski_fp, engine='pyarrow') 
        keeplist = ['symbol','date','Piotroski_Score','CR1','CR2','CR3','CR4','CR5','CR7','CR8','CR9']                
        in_piotroski_df = in_piotroski_df[keeplist]
        out_df = pd.merge(out_df, in_piotroski_df, on=['symbol','date'], how='left')

    # Merge in the Stock Price Return Stats.
    curr_len = len(in_price_fp)
    if curr_len>=9 and in_price_fp[curr_len-8:curr_len]=='.parquet':    
        in_price_df = pd.read_parquet(in_price_fp, engine='pyarrow') 
        # keeplist = ['symbol','date','date_1m','date_3m','date_6m','date_1y','r_1m','r_3m','r_6m','r_1y']      
        keeplist = ['symbol','date_14m','r_1y']
        in_price_df = in_price_df[keeplist]
        in_price_df.rename({
            'date_14m':'date'
        }, axis='columns', inplace=True)   
        out_df = pd.merge(out_df, in_price_df, on=['symbol','date'], how='left')
        
    ###########################################################################
    # Create the rolling quarter and annual Key Metric summary columns. 
    # Note that the last 4 reported values in the data might not represent a 
    # 1-year time period. This can result since some stocks are missing or 
    # don't report quarterly data. To keep track of the time duration of the 
    # last 4 measurements, we create a column that contains the duration, in   
    # days, of the last 4 lagged measurements.  
    ###########################################################################
    
    # Specify the list of metrics for which analysis columns will calculated.
    metric_list = ['RPS','NIPS','BVPS']
    
    # Create the metrics analysis columns by looping over the list items.
    for i, cm in enumerate(metric_list):
        
        ##########################
        # QUARTER SUMMARY METRICS
        ##########################
        
        # Calculate the lag values for 1-, 2-, 3-, 4-, 5-, 6-, 7-, and 8-qtrs 
        # ago. Note that the current quarter value (_0_1q) already exists.
        out_df[cm+'_1_2q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(1)
        out_df[cm+'_2_3q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(2)
        out_df[cm+'_3_4q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(3)
        out_df[cm+'_4_5q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(4)
        out_df[cm+'_5_6q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(5)  
        out_df[cm+'_6_7q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(6)
        out_df[cm+'_7_8q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(6)
        out_df[cm+'_8_9q']= out_df.groupby(['symbol'])[cm+'_0_1q'].shift(8)

        # Calculate the percent change columns over a 1-year period.
        out_df[cm+'_pc_0_1q'] = np.where(out_df[cm+'_4_5q']>0, out_df[cm+'_0_1q']/out_df[cm+'_4_5q']-1.0, np.nan)        
        out_df[cm+'_pc_1_2q'] = np.where(out_df[cm+'_5_6q']>0, out_df[cm+'_1_2q']/out_df[cm+'_5_6q']-1.0, np.nan)        
        out_df[cm+'_pc_2_3q'] = np.where(out_df[cm+'_6_7q']>0, out_df[cm+'_2_3q']/out_df[cm+'_6_7q']-1.0, np.nan)     
        out_df[cm+'_pc_3_4q'] = np.where(out_df[cm+'_7_8q']>0, out_df[cm+'_3_4q']/out_df[cm+'_7_8q']-1.0, np.nan)     
        out_df[cm+'_pc_4_5q'] = np.where(out_df[cm+'_8_9q']>0, out_df[cm+'_4_5q']/out_df[cm+'_8_9q']-1.0, np.nan) 
        
        ##########################
        # ANNUAL SUMMARY METRICS
        ##########################
        
        # Calculate the lag values for 0-, 1-, 2-, and 3-years ago. 
        temp_col = out_df.groupby(['symbol'])[cm+'_0_1q'].rolling(4, min_periods=4).sum()
        out_df[cm+'_0_1y'] = temp_col.reset_index(level=0,drop=True)
         
        temp_col = out_df.groupby(['symbol'])[cm+'_0_1q'].rolling(8, min_periods=8).sum()
        out_df[cm+'_1_2y'] = temp_col.reset_index(level=0,drop=True)
        out_df[cm+'_1_2y'] = out_df[cm+'_1_2y'] - out_df[cm+'_0_1y']       
        
        temp_col = out_df.groupby(['symbol'])[cm+'_0_1q'].rolling(12, min_periods=12).sum()
        out_df[cm+'_2_3y'] = temp_col.reset_index(level=0,drop=True)
        out_df[cm+'_2_3y'] = out_df[cm+'_2_3y'] - out_df[cm+'_0_1y'] - out_df[cm+'_1_2y']    
      
        temp_col = out_df.groupby(['symbol'])[cm+'_0_1q'].rolling(16, min_periods=16).sum()
        out_df[cm+'_3_4y'] = temp_col.reset_index(level=0,drop=True)
        out_df[cm+'_3_4y'] = out_df[cm+'_3_4y'] - out_df[cm+'_0_1y'] - out_df[cm+'_1_2y'] - out_df[cm+'_2_3y']      
    
        # Calculate the percent change columns over a 1-year period.  
        out_df[cm+'_pc_0_1y'] = np.where(out_df[cm+'_1_2y']>0, out_df[cm+'_0_1y']/out_df[cm+'_1_2y']-1.0, np.nan) 
        out_df[cm+'_pc_1_2y'] = np.where(out_df[cm+'_2_3y']>0, out_df[cm+'_1_2y']/out_df[cm+'_2_3y']-1.0, np.nan) 
        out_df[cm+'_pc_2_3y'] = np.where(out_df[cm+'_3_4y']>0, out_df[cm+'_2_3y']/out_df[cm+'_3_4y']-1.0, np.nan)

    ###########################################################################
    # DATA QUALITY checks on the RPS, NIPS, and BVPS lagged values and their
    # percent change values.
    #
    # Missing Date Logic: Check that all the key RPS, NIPS, and BVPS values are 
    # not missing. If NONE of these values are missing, then we only then do we
    # set the missing data value to '1' and '0' o/w. The values we check are
    # based on those specified in chapters 3 and 4 of O'Neil's book.
    #
    # Notes: MMYT, SFM are interesting case studies. 
    ###########################################################################

    # Create the combined RPS and NIPS missing data column.
    cond1 = ( pd.isnull(out_df['RPS_0_1q'])==True )
    cond2 = ( pd.isnull(out_df['RPS_1_2q'])==True )
    cond3 = ( pd.isnull(out_df['RPS_4_5q'])==True )
    cond4 = ( pd.isnull(out_df['RPS_5_6q'])==True )    
    cond5 = ( pd.isnull(out_df['RPS_0_1y'])==True )
    cond6 = ( pd.isnull(out_df['RPS_1_2y'])==True )
    cond7 = ( pd.isnull(out_df['RPS_2_3y'])==True )
    cond8 = ( pd.isnull(out_df['RPS_3_4y'])==True )
    cond9 = ( pd.isnull(out_df['NIPS_0_1q'])==True )
    cond10 = ( pd.isnull(out_df['NIPS_1_2q'])==True )
    cond11 = ( pd.isnull(out_df['NIPS_4_5q'])==True )
    cond12 = ( pd.isnull(out_df['NIPS_5_6q'])==True )    
    cond13 = ( pd.isnull(out_df['NIPS_0_1y'])==True )
    cond14 = ( pd.isnull(out_df['NIPS_1_2y'])==True )
    cond15 = ( pd.isnull(out_df['NIPS_2_3y'])==True )       
    cond16 = ( pd.isnull(out_df['NIPS_3_4y'])==True )       
    cond = cond1|cond2|cond3|cond4|cond5|cond6|cond7|cond8
    cond = cond|cond9|cond10|cond11|cond12|cond13|cond14|cond15|cond16
    out_df['dqPass_notNull'] = np.where(cond,'0','1')

    # Create the combined RPS and NIPS lower and upper-bound check column.
    cond1 = ( out_df[['RPS_0_1q','RPS_1_2q','RPS_4_5q','RPS_5_6q']].min(axis=1) > 0.0 )
    cond2 = ( out_df[['RPS_0_1q','RPS_1_2q','RPS_4_5q','RPS_5_6q']].max(axis=1) <= 1200.0 )     
    cond3 = ( out_df[['RPS_0_1y','RPS_1_2y','RPS_2_3y','RPS_3_4y']].min(axis=1) > 0.0 )
    cond4 = ( out_df[['RPS_0_1y','RPS_1_2y','RPS_2_3y','RPS_3_4y']].max(axis=1) <= 1200.0 )
    cond5 = ( out_df[['NIPS_0_1q','NIPS_1_2q','NIPS_4_5q','NIPS_5_6q']].min(axis=1) > -1000.0 )
    cond6 = ( out_df[['NIPS_0_1q','NIPS_1_2q','NIPS_4_5q','NIPS_5_6q']].max(axis=1) <= 1200.0 )   
    cond7 = ( out_df[['NIPS_0_1y','NIPS_1_2y','NIPS_2_3y','NIPS_3_4y']].min(axis=1) > -1000.0 )
    cond8 = ( out_df[['NIPS_0_1y','NIPS_1_2y','NIPS_2_3y','NIPS_3_4y']].max(axis=1) <= 1200.0 )
    cond = cond1&cond2&cond3&cond4&cond5&cond6&cond7&cond8
    out_df['dqPass_limits'] = np.where(cond,'1','0')    
    
    # Create the combined RPS and NIPS percent change check column.
    cond1 = ( out_df[['RPS_pc_0_1q','RPS_pc_1_2q']].min(axis=1) > -2.0 )
    cond2 = ( out_df[['RPS_pc_0_1q','RPS_pc_1_2q']].max(axis=1) <= 12.0 )        
    cond3 = ( out_df[['RPS_pc_0_1y','RPS_pc_1_2y','RPS_pc_2_3y']].min(axis=1) > -2.0 )
    cond4 = ( out_df[['RPS_pc_0_1y','RPS_pc_1_2y','RPS_pc_2_3y']].max(axis=1) <= 12.0 )
    cond5 = ( out_df[['NIPS_pc_0_1q','NIPS_pc_1_2q']].min(axis=1) > -2.0 )
    cond6 = ( out_df[['NIPS_pc_0_1q','NIPS_pc_1_2q']].max(axis=1) <= 12.0 )        
    cond7 = ( out_df[['NIPS_pc_0_1y','NIPS_pc_1_2y','NIPS_pc_2_3y']].min(axis=1) > -2.0 )
    cond8 = ( out_df[['NIPS_pc_0_1y','NIPS_pc_1_2y','NIPS_pc_2_3y']].max(axis=1) <= 12.0 )
    cond = cond1&cond2&cond3&cond4&cond5&cond6&cond7&cond8
    out_df['dqPass_pc'] = np.where(cond,'1','0')  
    
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
    if len(min_date)>0:
        out_df = out_df.loc[out_df['date']>=pd.to_datetime(min_date)]    
    
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
    # Reorder the output columns and also only keep columns specified.
    ###################################################################
    col_order = []    
    col_order += ['symbol','date','date_year','fiscal_qtr']
    col_order += ['nlag','max_nlag','firstLast_flag','reverse_nlag']
    col_order += ['dqPass_notNull','dqPass_limits','dqPass_pc']
    col_order += ['days_0_1y','days_1_2y']
    col_order += ['r_1y']
    for i, cm in enumerate(metric_list):
                
        col_order += [cm+'_0_1q',cm+'_1_2q',cm+'_2_3q',cm+'_3_4q',cm+'_4_5q',cm+'_5_6q',cm+'_6_7q',cm+'_7_8q',cm+'_8_9q']
        col_order += [cm+'_pc_0_1q',cm+'_pc_1_2q',cm+'_pc_2_3q',cm+'_pc_3_4q',cm+'_pc_4_5q']
        
        col_order += [cm+'_0_1y',cm+'_1_2y',cm+'_2_3y',cm+'_3_4y']
        col_order += [cm+'_pc_0_1y',cm+'_pc_1_2y',cm+'_pc_2_3y']
    
    col_order += ['sector','industry','ipo_date','name','desc']
    col_order += ['curr_beta','curr_volAvg','curr_mktCap','isActivelyTrading']
    
    col_remain = [col for col in out_df.columns if col not in col_order]
    out_df = out_df[col_order+col_remain] 
    
    ###################################################################
    # SAVE the output dataframe as a file.
    ###################################################################
    
    # Save as a PARQUET file, if one was given.
    curr_len = len(outdsn_parquet)
    if curr_len>=9 and outdsn_parquet[curr_len-8:curr_len]=='.parquet':
        out_parquet = f'{outpath}\{outdsn_parquet}' 
        fp.write(f'{out_parquet}',out_df)
        # out_df.to_parquet(f'{out_parquet}',index=False)
    
    # Save as a CSV file, if one was given.
    curr_len = len(outdsn_csv)
    if curr_len>=5 and outdsn_csv[curr_len-4:curr_len]=='.csv':
        out_csv = f'{outpath}\{outdsn_csv}' 
        out_df.to_csv(f'{out_csv}',index=False)       
    
    ###################################################################
    # RETURN the output dataframe.
    ###################################################################
    return in_df, out_df 



        