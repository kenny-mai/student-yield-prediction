import yield_boosting
from datetime import date

def define_yield_forecast(run_date, query_dir, local_mode):
    df = yield_boosting.make_predictions(run_date, query_dir, local_mode)
    df['ds'] = run_date
    return df

def lambda_handler(event, context):
    local_mode = event.get('local_mode', False)
    query_dir = event.get('query_dir', '')
    run_date = event.get('run_date', str(date.today()))
    
    df = define_yield_forecast(run_date, query_dir, local_mode)
    if event.get('return_df_no_writes', False):
        return df
    df.to_csv(query_dir+'raw_data_science.raw_yield_predictions_'+run_date+'.csv', index=False)

if __name__ == "__main__":
    lambda_handler({'local_mode':True, 'return_df_no_writes':False, 'query_dir':'/Users/kenny.mai/Documents/yield_forecast/edna-data-science-talend-models/yield_dev/'}, None) 
