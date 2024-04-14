import logging
import azure.functions as func
import datetime
import logging
import requests
import pandas as pd
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


def extract_and_upload_data():
    # Extract data
    team_stats = extract_team_stats()
    player_stats = extract_player_stats()

    # Upload data to Azure Blob Storage
    upload_to_blob(team_stats, 'EPL_teams.csv')
    upload_to_blob(player_stats, 'EPL_players.csv')

def extract_team_stats():
    url_df = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'
    df = pd.read_html(url_df)
    team_stats = pd.read_html(url_df)[0]

# creating a data with the same headers but without multi indexing
    team_stats.columns = [' '.join(col).strip() for col in team_stats.columns]

    team_stats = team_stats.reset_index(drop=True)
# creating a list with new names
    new_columns = []
    for col in team_stats.columns:
        if 'level_0' in col:
            new_col = col.split()[-1]  # takes the last name
        else:
            new_col = col
        new_columns.append(new_col)

# rename columns
    team_stats.columns = new_columns
    team_stats = team_stats.fillna(0)

    return team_stats

def extract_player_stats():
    url= 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'
    player_stats=pd.read_html(
        requests.get(url).text.replace('<!--','').replace('-->','')
        ,attrs={'id':'stats_standard'})[0]
# creating a data with the same headers but without multi indexing
    player_stats.columns = [' '.join(col).strip() for col in player_stats.columns]

    player_stats = player_stats.reset_index(drop=True)

# creating a list with new names
    new_columns = []
    for col in player_stats.columns:
        if 'level_0' in col:
            new_col = col.split()[-1]  # takes the last name
        else:
            new_col = col
        new_columns.append(new_col)

# rename columns
    player_stats.columns = new_columns
    player_stats = player_stats.fillna(0).reset_index(drop=True)
    player_stats['Age'] = player_stats['Age'].str[:2]
    player_stats['Nation'] = player_stats['Nation'].str.split(' ').str[1]
    player_stats['Position_2'] = player_stats['Pos'].str[3:]
    player_stats['Position'] = player_stats['Pos'].str[:2]
    player_stats.drop(columns='Pos', inplace=True)
    player_stats = player_stats[player_stats['Player'] != 'Player']

    return player_stats

def upload_to_blob(df, blob_name):
    # Convert DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Create BlobServiceClient using environment variables
    connection_string = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Specify your container name here
    container_name = 'eplstats2024'

    # Get Blob Client and upload the CSV data
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)



@app.schedule(schedule="0 0 11 * * 1", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def eplstats(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function started.')    
    extract_and_upload_data()
    logging.info('Python timer trigger function executed.')