import pandas as pd

def fill_new_col(df, new_col, cumulative_col):
    # Create a copy of the dataframe to avoid modifying the original
    result_df = df.copy()
    
    # Initialize the new column if it doesn't exist
    if new_col not in result_df.columns:
        result_df[new_col] = None
    
    # Process each country separately
    for country in result_df['Country'].unique():
        # Create a mask for the current country
        mask = result_df['Country'] == country
        
        # Get the country-specific data
        country_data = result_df.loc[mask]
        
        # Calculate differences and fill values
        result_df.loc[mask, new_col] = country_data[cumulative_col].diff()
        
        # Fill first value (which will be NaN after diff()) with the cumulative value
        first_idx = country_data.index[0]
        result_df.loc[first_idx, new_col] = result_df.loc[first_idx, cumulative_col]
    
    return result_df

#def date_key_creation(df):
    # df["key_date"] = df["key_date"].astype('str').strip('-').astype('int')
    # return df["key_date"].astype('int')



def data_transform(file_path, rep_negative=True):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    
    df["Date_reported"] = pd.to_datetime(df.Date_reported)

    # fill missing value in country code
    # nambia country code is NA
    df["Country_code"] = df["Country_code"].fillna('NA')

    # fill the missing value where cummulative counterpart = 0
    df.loc[(df['New_cases'].isna()) & (df['Cumulative_cases'].eq(0)), 'New_cases'] = 0
    df.loc[(df['New_deaths'].isna()) & (df['Cumulative_deaths'].eq(0)), 'New_deaths'] = 0


    df_final = fill_new_col(df, new_col='New_cases', cumulative_col='Cumulative_cases')
    df_final = fill_new_col(df_final, new_col='New_deaths', cumulative_col='Cumulative_deaths')

    if rep_negative == True:
        df_final['New_cases'] = df_final['New_cases'].clip(lower=0)
        df_final['New_deaths'] = df_final['New_deaths'].clip(lower=0)
    
    # create suragete keys for date and location dimentions
    df_final["date_key"] = df_final["Date_reported"].astype('str').str.replace('-', '').astype('int')
    df_final["location_key"] = df_final["Country_code"].factorize()[0] + 1

    return df_final
