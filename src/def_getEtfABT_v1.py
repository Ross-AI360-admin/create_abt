

def getEtfStatevector(
    in_priceabt_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\PRICE_ABT\priceABT_month_etf.parquet',    
    in_company_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_etf.parquet',
    in_etfinfo_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ETF_INFO\etfInfo_fmp.parquet',    
    min_date        = '2018-01-01',
    max_date        = '',
    outpath         = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\ETF_ABT',
    outdsn_parquet  = 'etfPriceABT_month.parquet',
    outdsn_csv      = 'etfPriceABT_month.csv'   
):

    ###########################################################################
    # Import Packages
    ###########################################################################
    import pandas as pd   
    
    ###########################################################################
    # Load input data from the specified input file, if no input dataframe was 
    # specfied.
    ###########################################################################
    in_df = pd.read_parquet(in_priceabt_fp, engine='pyarrow')   
    numStocks = len(pd.unique(in_df['symbol']))
    print(f'The number of distinct ETFs in the input price file = {numStocks}')

    ###########################################################################
    # Initialize the output dataframe.
    ###########################################################################
    out_df = in_df.copy()    

    ###########################################################################
    # Merge in the Company Overviews.
    ###########################################################################
    curr_len = len(in_company_fp)
    if curr_len>=9 and in_company_fp[curr_len-8:curr_len]=='.parquet':
        in_company_df = pd.read_parquet(in_company_fp, engine='pyarrow') 
        keeplist = ['symbol','sector','industry','exchange']
        keeplist += ['beta','volAvg','mktCap','isActivelyTrading','admin_runDate']
        in_company_df = in_company_df[keeplist]
        in_company_df.rename({
            'admin_runDate':'company_admin_runDate',
            'beta': 'curr_beta',
            'volAvg': 'curr_volAvg',
            'mktCap': 'curr_mktCap'
        }, axis='columns', inplace=True)         
        out_df = pd.merge(out_df, in_company_df, on=['symbol'], how='left')

    ###################################################################
    # RETURN the output dataframe.
    ###################################################################
    return out_df    


etf_sv = getEtfStatevector(
    in_priceabt_fp  = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\PRICE_ABT\priceABT_month_etf.parquet',    
    in_company_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\COMPANY\companyOverviews_fmp_etf.parquet',
    in_etfinfo_fp   = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ETF_INFO\etfInfo_fmp.parquet',    
    min_date        = '2018-01-01',
    max_date        = '',
    outpath         = r'C:\Users\sharo\OneDrive - aiinvestor360.com\DATA\ABT\ETF_ABT',
    outdsn_parquet  = 'etfPriceABT_month.parquet',
    outdsn_csv      = 'etfPriceABT_month.csv'   
)




