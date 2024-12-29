############################################################################################################
############################################################################################################
# FUNCTION DEFINITION: getPriceStats()
#
# DESCRIPTION: This function computes a wide set of performance related statistics on price data. The input
# price data can either be specified as a dataframe or as a complete filepath to a parquet file. The  
# function also allows for specifying a company overview data as a filepath, which is merged into the price  
# to provide additional details to the output price statistics data. This function is currently designed for
# monthly data, but could be extended to daily price data.
#
# FUNCTION INPUT ARGS
#   - symbol_filters = input list of stocks that are used to filter in_df (optional)
#   - in_df          = input price dataframe, where in_df takes priority over in_fp
#   - in_fp          = input price complete filepath
#   - in_company_fp  = input company overview complete filepath (optional)
#   - min_date       = the minimum date filter to apply to price data (optional)
#   - max_date       = the maximum date filter to apply to price data (optional)
#   - outpath        = the folder path where all output data will be saved
#   - outdsn_parquet = the name of output parquet file
#   - outdsn_csv     = the name of the output csv file
#
# OUTPUT DATA SCHEMA
#
#
# OUTPUT DATAFRAMES    
#   - out_df 
#
# OUTPUT FILES
#   - outdsn_parquet (optional)
#   - outdsn_csv (optional)
############################################################################################################
############################################################################################################
def getPriceABT(
    symbol_filters = [],    
    in_df          = '',   
    in_fp          = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\PRICE\MONTHLY\monthlyPrices_av_stock.parquet',      
    in_company_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_stock.parquet',
    min_date       = '2010-01-01',
    max_date       = '',
    outpath        = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\PRICE_ABT',
    outdsn_parquet = 'monthlyPriceABT_stock.parquet',
    outdsn_csv     = 'monthlyPriceABT_stock.csv'   
):
    
    ###################################################################
    # Import Packages
    ###################################################################
    import pandas as pd
    import csv
    import numpy as np
    import math
    from scipy import stats
    import datetime 
    from datetime import datetime
    from datetime import date
    from pandas.tseries.offsets import MonthEnd
    import calendar
   
    ###################################################################
    # Load input data, if no input dataframe was specfied.
    ###################################################################
    if len(in_df)==0:
        in_df = pd.read_parquet(in_fp, engine='pyarrow')  
    
    ###########################################################################
    # If a stock filter list was specified, filter the input dataframe.
    ###########################################################################
    if len(symbol_filters)>0:
        symbol_filters = list(map(lambda x: x.upper(),symbol_filters))
        in_df = in_df[in_df['symbol'].isin(symbol_filters)]    
    
    ###################################################################
    # Apply the min and max date thresholds, if they were specified.
    ###################################################################
    if len(min_date)>0:
        in_df = in_df.loc[in_df['date']>=pd.to_datetime(min_date)] 
    if len(max_date)>0:
        in_df = in_df.loc[in_df['date']<=pd.to_datetime(max_date)]     
    
    ###################################################################
    # Filter out any incomplete month's data. This is accomplished by 
    # building an adjusted date filter based on whether the last date  
    # in the pricing data is the last day of the month or is the month
    # January. The adjusted date is then used to filter the data.
    ###################################################################
    curr_maxDay   = in_df['date'].max().day
    curr_maxMonth = in_df['date'].max().month
    curr_maxYear  = in_df['date'].max().year
    if curr_maxDay==calendar.monthrange(curr_maxYear,curr_maxMonth)[1]: # is the max day the last day of the month?
        adj_maxMonth = curr_maxMonth
        adj_maxYear = curr_maxYear
    elif curr_maxMonth==1:                                              # is the max month January?
        adj_maxMonth = 12
        adj_maxYear = curr_maxYear-1
    else:                                                               # the max day isn't the last day of the month
        adj_maxMonth = curr_maxMonth-1                                  # and the max month isn't January   
        adj_maxYear = curr_maxYear
    filt = (in_df['date'] <= datetime( adj_maxYear, adj_maxMonth, calendar.monthrange(adj_maxYear,adj_maxMonth)[1] ))
    in_df = in_df[filt]    
    
    ###################################################################
    # Adjust the date values so that they are always the very last day  
    # of the month. This is done so that any data merges done using the
    # date column will work without any alignment issues.
    ###################################################################
    in_df['date'] = pd.to_datetime(in_df['date'], format="%Y%m") + MonthEnd(0)
    in_df.style.format({"date": lambda t: t.strftime("%d-%m-%Y")}) 

    ###################################################################
    # Sort the input price data by symbol and date.
    ###################################################################
    in_df.sort_values( ['symbol','date'], ascending=[True,True], inplace=True)
    in_df.reset_index(level=0,drop=True,inplace=True)    
        
    ###################################################################
    # Create the output dataframe.
    ###################################################################    
    
    # Initialize the output dataframe.
    out_df = in_df.copy()
    
    # Merge in company overview data, if it is specified.
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        in_company_df = in_company_df[['symbol','sector','industry','ipo_date']]
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')
        
    # Create a record count for each symbol.
    out_df['nlag'] = out_df.groupby(['symbol']).cumcount()
   
    # Create the 1-month, 3-month, 6-month and 1-year lagged dates.
    out_df['date_1m'] = out_df.groupby(['symbol'])['date'].shift(1) 
    out_df['date_3m'] = out_df.groupby(['symbol'])['date'].shift(3)     
    out_df['date_6m'] = out_df.groupby(['symbol'])['date'].shift(6)
    out_df['date_8m'] = out_df.groupby(['symbol'])['date'].shift(8) 
    out_df['date_9m'] = out_df.groupby(['symbol'])['date'].shift(9)     
    out_df['date_12m'] = out_df.groupby(['symbol'])['date'].shift(12)
    out_df['date_14m'] = out_df.groupby(['symbol'])['date'].shift(14)     
    out_df['date_15m'] = out_df.groupby(['symbol'])['date'].shift(15)
    
    # Create the total number of dividend payouts for each window year.
    conds = [out_df['div_amount']>0]
    choices = [1]
    out_df['div_payout'] = np.select( conds, choices, default=0 ) # =1 if div_amount>0 & =0 o/w
    temp_col = out_df.groupby(['symbol'])['div_payout'].rolling(12,min_periods=1).sum()
    out_df['divN_1y'] = temp_col.reset_index(level=0,drop=True)
    out_df['divN_2y'] = out_df.groupby(['symbol'])['divN_1y'].shift(12)
    out_df['divN_3y'] = out_df.groupby(['symbol'])['divN_1y'].shift(24)
    out_df.drop( ['div_payout'] , axis=1, inplace=True, errors='ignore' )    
   
    # Create the running total dividend amount columns. We create the 1 year sum
    # and then it to populate all other year sums.
    temp_col = out_df.groupby(['symbol'])['div_amount'].rolling(12,min_periods=1).sum()
    out_df['totDiv_1y'] = temp_col.reset_index(level=0,drop=True)
    out_df['totDiv_2y'] = out_df.groupby(['symbol'])['totDiv_1y'].shift(12)
    out_df['totDiv_3y'] = out_df.groupby(['symbol'])['totDiv_1y'].shift(24)    
    
    # Create the dividend yield columns.    
    out_df['div_yield'] = out_df['div_amount']/out_df['adj_close']
    temp_col = out_df.groupby(['symbol'])['div_yield'].rolling(12,min_periods=1).sum()
    out_df['divYld_1y'] = temp_col.reset_index(level=0,drop=True)
    out_df['divYld_2y'] = out_df.groupby(['symbol'])['divYld_1y'].shift(12)
    out_df['divYld_3y'] = out_df.groupby(['symbol'])['divYld_1y'].shift(24)     
    
    # Create a grouping object by symbol.
    group = out_df.groupby(['symbol'])       
    
    # Create the 1-month, 3-month, and 6-month returns.
    out_df['r_1m'] = out_df.adj_close.div(group.adj_close.shift(1)) - 1    
    out_df['r_3m'] = out_df.adj_close.div(group.adj_close.shift(3)) - 1    
    out_df['r_6m'] = out_df.adj_close.div(group.adj_close.shift(6)) - 1    
    
    # Create the annualized returns over time periods ranging from 1 to 7 years.
    out_df['r_1y'] = out_df.adj_close.div(group.adj_close.shift(12)) - 1
    out_df['r_2y'] = pow( out_df.adj_close.div(group.adj_close.shift(24)), 1/2 ) - 1
    out_df['r_3y'] = pow( out_df.adj_close.div(group.adj_close.shift(36)), 1/3 ) - 1
    out_df['r_4y'] = pow( out_df.adj_close.div(group.adj_close.shift(48)), 1/4 ) - 1
    out_df['r_5y'] = pow( out_df.adj_close.div(group.adj_close.shift(60)), 1/5 ) - 1
    out_df['r_6y'] = pow( out_df.adj_close.div(group.adj_close.shift(72)), 1/6 ) - 1
    out_df['r_7y'] = pow( out_df.adj_close.div(group.adj_close.shift(84)), 1/7 ) - 1    
    
    # Create the one year returns for the time windows 1-2, 2-3, 3-4, and 4-5
    # years. 
    out_df['r_1_2y'] = out_df.adj_close.shift(12).div(group.adj_close.shift(24)) - 1
    out_df['r_2_3y'] = out_df.adj_close.shift(24).div(group.adj_close.shift(36)) - 1
    out_df['r_3_4y'] = out_df.adj_close.shift(36).div(group.adj_close.shift(48)) - 1    
    out_df['r_4_5y'] = out_df.adj_close.shift(48).div(group.adj_close.shift(60)) - 1  
    
    # Create the cumulative returns over time periods ranging from 1 to 7 years.
    out_df['cr_2y'] = out_df.adj_close.div(group.adj_close.shift(24)) - 1
    out_df['cr_3y'] = out_df.adj_close.div(group.adj_close.shift(36)) - 1
    out_df['cr_4y'] = out_df.adj_close.div(group.adj_close.shift(48)) - 1
    out_df['cr_5y'] = out_df.adj_close.div(group.adj_close.shift(60)) - 1
    out_df['cr_6y'] = out_df.adj_close.div(group.adj_close.shift(72)) - 1
    out_df['cr_7y'] = out_df.adj_close.div(group.adj_close.shift(84)) - 1    
    
    # Create the monthly return volatilities for 1 through 7 year time windows.    
    out_df['vol_1y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(12).std().reset_index(level=0,drop=True)
    out_df['vol_2y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(24).std().reset_index(level=0,drop=True)
    out_df['vol_3y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(36).std().reset_index(level=0,drop=True)
    out_df['vol_4y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(48).std().reset_index(level=0,drop=True)    
    out_df['vol_5y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(60).std().reset_index(level=0,drop=True)
    out_df['vol_6y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(72).std().reset_index(level=0,drop=True)    
    out_df['vol_7y'] = math.sqrt(12)*out_df.groupby(['symbol'])['r_1m'].rolling(84).std().reset_index(level=0,drop=True)     
    
    # Create the Sharpe ratios for 1 through 7 year time windows, where the 
    # numerator uses annualized returns and the denominator uses the standard
    # deviations for the monthly returns.
    out_df['shp_1y'] = out_df['r_1y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(12).std().reset_index(level=0,drop=True))
    out_df['shp_2y'] = out_df['r_2y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(24).std().reset_index(level=0,drop=True))
    out_df['shp_3y'] = out_df['r_3y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(36).std().reset_index(level=0,drop=True))
    out_df['shp_4y'] = out_df['r_4y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(48).std().reset_index(level=0,drop=True))    
    out_df['shp_5y'] = out_df['r_5y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(60).std().reset_index(level=0,drop=True))
    out_df['shp_6y'] = out_df['r_6y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(72).std().reset_index(level=0,drop=True))    
    out_df['shp_7y'] = out_df['r_7y'] / (out_df.groupby(['symbol'])['r_1m'].rolling(84).std().reset_index(level=0,drop=True))      
    
    # Create the overall missing data columns for returns and sharpe ratios.
    out_df['isnull_1y'] = np.where( (pd.isnull(out_df['r_1y'])==True) | (pd.isnull(out_df['shp_1y'])==True) , True, False )
    out_df['isnull_2y'] = np.where( (pd.isnull(out_df['r_2y'])==True) | (pd.isnull(out_df['shp_2y'])==True) , True, False )
    out_df['isnull_3y'] = np.where( (pd.isnull(out_df['r_3y'])==True) | (pd.isnull(out_df['shp_3y'])==True) , True, False )
    out_df['isnull_4y'] = np.where( (pd.isnull(out_df['r_4y'])==True) | (pd.isnull(out_df['shp_4y'])==True) , True, False )
    out_df['isnull_5y'] = np.where( (pd.isnull(out_df['r_5y'])==True) | (pd.isnull(out_df['shp_5y'])==True) , True, False )
    out_df['isnull_6y'] = np.where( (pd.isnull(out_df['r_6y'])==True) | (pd.isnull(out_df['shp_6y'])==True) , True, False )
    out_df['isnull_7y'] = np.where( (pd.isnull(out_df['r_7y'])==True) | (pd.isnull(out_df['shp_7y'])==True) , True, False )     
    
    # Create the overall missing data column, which gives the number of
    # complete number of years that we have non-missing data.
    conds = [out_df['isnull_7y']==False, out_df['isnull_6y']==False, out_df['isnull_5y']==False, out_df['isnull_4y']==False,
             out_df['isnull_3y']==False, out_df['isnull_2y']==False, out_df['isnull_1y']==False]
    choices = [7,6,5,4,3,2,1]
    out_df['data_years'] = np.select(conds,choices,default=0)     
    
    # Drop columns that are not needed in the final output dataset.
    drop_list = ['isnull_1y','isnull_2y','isnull_3y','isnull_4y','isnull_5y','isnull_6y','isnull_7y']     
    out_df.drop(drop_list, axis=1, inplace=True, errors='ignore')    
    
    # Create columns that make general purpose data filtering easy to perform.
    out_df['year'] = out_df['date'].dt.year
    out_df['min_date'] = out_df.groupby(['symbol'])['date'].transform('min')
    out_df['max_date'] = out_df.groupby(['symbol'])['date'].transform('max')
    out_df['max_nlag'] = out_df.groupby(['symbol'])['nlag'].transform('max')
    out_df['max_data_years'] = out_df.groupby(['symbol'])['data_years'].transform('max')
    conds = [ out_df['nlag']==out_df['max_nlag'], out_df['nlag']==0 ]
    out_df['firstLast_flag'] = np.select(conds, ['L','F'], default='I')    
    
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
    return in_df, out_df    



    