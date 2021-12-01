import requests
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import urllib.parse as url
from sqlalchemy import create_engine

# I stored the login data as an environment variable at .env make sure you have that file in the same path as this python script filled with the login info
env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)
user = os.environ['user']
password = os.environ['password']
host = os.environ['host']
database = os.environ['database']

def connect_to_db():
    # creating engine:
    return create_engine(f"mysql://{user}:{url.quote_plus(password)}@{host}/{database}?charset=utf8")

def upload_df(df,table_name):
    # recieves a dataframe and a table_name and uploades the data from the dataframe to the relevant table
    # the connection we use is created in the function connect_to_db
    df.to_sql(table_name, con=connect_to_db(),index=False, if_exists="replace")

def get_users_df():
    # Function gets record of 4500 users and returns it in a dataframe
    # first i need to extract the data by using randomuser api service with the following parameters:
    # results=4500 - will give us 4500 results as requested    
    # noinfo - we only want the data without extra info and noinfo allows it
    # fmt=prettyjson - the format that is most comfortable for us to get the needed data
    url = 'https://randomuser.me/api/?results=4500&noinfo&fmt=prettyjson'
    data = requests.get(url).json()['results']
    # by pandas function json_normalize I'll put the data that's in the json into a dataframe:
    df = pd.json_normalize(data)
    return df

def create_and_load_gender_dfs(df):
    # in order to create datasets for both genders I'll just separate into two dataframes by gender column by using the two functions
    female_df = df[df["gender"]=="female"]
    male_df = df[df["gender"]=="male"]
    # in order to upload the data to the relevant tables first I've created the tables and then I use upload_df function:
    # Load male/female tables to DB
    upload_df(female_df,"ISABELLA_OREN_test_female")
    upload_df(male_df,"ISABELLA_OREN_test_male")

if __name__ == "__main__":
    # First part - create a dataset of 4500 users:
    df = get_users_df()
    
    # Second part - Split the dataset to 2 gender datasets and store each one of the datasets in separated mysql table named “YOUR_NAME_test_male/female”:
    # for that I'll send df to create_and_load_gender_dfs function:
    create_and_load_gender_dfs(df)

    # Third part & Fourth part - 
    # Split the dataset to 10 subsets by dob.age column in groups of 10 (10s 20s 30s etc.)
    # Store each one of subsets in “YOUR_NAME_test_{subset_number} in mysql
    # decade represents the age range, if decade = 0 than the age range will be 0 to 10
    decade = 0
    # num_in_table reprresents the number we will add in the table name
    num_in_table = 1
    df['dob.age'] = df['dob.age'].astype(int)
    while decade < 100:
        # creating a new dataframe that will contain only those who have the relevant age for each decade
        df_current_decade = df[(df['dob.age'] >= decade) & (df['dob.age'] < (decade + 10))]
        # uploading the new dataframe to the relevant table
        upload_df(df_current_decade, 'ISABELLA_OREN_test_' + str(num_in_table))
        num_in_table = num_in_table + 1
        decade = decade + 10
        
    # Fith part - Write a sql query that will return the top 20 last registered males and females form each one of gender tables you created in 2 and save it as YOUR_NAME_test_20
    top_20_each_query = " (select * from interview.ISABELLA_OREN_test_female order by `registered.date` desc limit 20) union all (select * from interview.ISABELLA_OREN_test_male order by `registered.date` desc limit 20)"
    df_top_20 = pd.read_sql_query(top_20_each_query, con=connect_to_db())
    upload_df(df_top_20, "ISABELLA_OREN_test_20")
    
    # Sixth part - Create a dataset that combines data from YOUR_NAME_test_20 and data from YOUR_NAME_test_5 table. 
    # Make sure each row presented only once and there is no multiplication of data. 
    # Create json from the mentioned dataset and store it locally as first.json
    # just in case changes were made, I'll extract the data by query from the table instead of taking df_top_20 from a few rows before.
    df_20 = pd.read_sql_query("select * from interview.ISABELLA_OREN_test_20", con=connect_to_db())
    df_5 = pd.read_sql_query("select * from interview.ISABELLA_OREN_test_5", con=connect_to_db())
    df_6th_part = pd.concat([df_20,df_5]).drop_duplicates().reset_index()
    #creating the first json:
    df_6th_part.to_json("first.json", orient="records")
    
    # Seventh part - Create a dataset that combines data from YOUR_NAME_test_20 and data from YOUR_NAME_test2 table. 
    # In case the same rows are presented in 2 datasets both of rows supposed to be presented. 
    #Create json from the mentioned dataset and store itlocally as second.json
    # just in case changes were made, I'll extract the data by query from the table instead of taking df_top_20 from a few rows before.
    df_20 = pd.read_sql_query("select * from interview.ISABELLA_OREN_test_20", con=connect_to_db())
    df_2 = pd.read_sql_query("select * from interview.ISABELLA_OREN_test_2", con=connect_to_db())
    df_7th_part = pd.concat([df_20,df_2]).reset_index()
    #creating the first json:
    df_7th_part.to_json("second.json", orient="records")
    
