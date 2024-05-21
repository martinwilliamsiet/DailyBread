#database import all match level stats
import soccerdata as sd
from soccerdata import FBref 
from datetime import datetime
import pandas as pd
import psycopg2
import sys
import traceback
import requests
import os 
import time
from multiprocessing import Pool

#5/20 moving away from ALL tier 1 and focusing on out of the box supported big 5 leauges first
#error with team names on match 3c098794, unsupported

# Set up a SOCKS proxy 
#socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050)
#socket.socket = socks.socksocket

# Define the proxy dictionary
proxies = {
'http':'http://1.85.33.94:6666',
'http':'http://139.198.112.223:17620',
'http':'http://134.209.28.98:3128',
'http':'http://216.176.187.99:8889',
'http':'http://36.111.191.127:808',
'http':'http://185.94.167.98:8080',
'http':'http://186.215.87.194:6024',
'http':'http://102.134.181.142:9999'


}

# Make a request using the proxy
response = requests.get(' https://fbref.com/en/comps/', proxies=proxies)

# Print the response content
print(response.text)


#testing to manipulate columns better 
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_colwidth', None)  # No limit on column width
pd.set_option('display.width', None)  # No limit on the display width
pd.reset_option('display.max_rows')

#also using example match id 'f97b9a22'

#pick sample league and season to create tables to base chart off of
ENGstats = sd.FBref(leagues="ENG-Premier League", seasons='2324')

#player summary stats
ENGpmatchsummary = ENGstats.read_player_match_stats(stat_type="summary")
#leave match_id variable off to include all match ids, testing with f97b9a22 for db import

# Reset the index to turn multi-index into columns
ENGpmatchsummary.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ENGpmatchsummary.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchsummary.columns = [''.join(map(str, col)).strip() for col in ENGpmatchsummary.columns]

prefix = 'pm'
prefix_conditions = ['Performance', 'Expected', 'Passes', 'Carries', 'Take-Ons', 'SCA', 'Touches', 'Receiving', 
                     'Aerial Duels', 'Total', 'Short', 'Medium','Long', 'Ast', 'xA','KP','1/3','P','Crs',
                      'Prg','Pass','Corner','Outcomes','Tackles','Challenges','Blocks','Int','Tkl','Err', 'Clr' ]

def add_prefix_to_columns(columns, prefix, conditions):
    new_columns = []
    for col in columns:
        if any(col.startswith(word) for word in conditions):
            new_columns.append(prefix + col)
        else:
            new_columns.append(col)
    return new_columns

ENGpmatchsummary.columns = add_prefix_to_columns(ENGpmatchsummary.columns, prefix, prefix_conditions)

ENGpmatchsummary = ENGpmatchsummary.rename(columns={'game_id': 'match_id'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'min': 'Playermin'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmPassesCmp%': 'pmpassescmppc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmAerial DuelsWon%': 'pmaerialduelswonpc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmTake-OnsSucc%': 'pmtakeonssuccpc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmTake-OnsAtt': 'pmtakeonsatt'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmTake-OnsSucc': 'pmtakeonssucc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmTotalCmp%': 'pmtotalcmppc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmShortCmp%': 'pmchortcmppc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmMediumCmp%': 'pmmediumcmppc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmLongCmp%': 'pmlongcmppc'})
ENGpmatchsummary = ENGpmatchsummary.rename(columns={'pmChallengesTkl%': 'pmchallengestklpc'})

# Identify numeric columns
numeric_columns = ENGpmatchsummary.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchsummary.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchsummary[numeric_columns] = ENGpmatchsummary[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchsummary[string_columns] = ENGpmatchsummary[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ENGpmatchsummary[numeric_columns] = ENGpmatchsummary[numeric_columns].astype(float)

# #check new column renaming
# print(ENGpmatchsummary)

#possession match stats
ENGpmatchpossession = ENGstats.read_player_match_stats(stat_type="possession")

# Reset the index to turn multi-index into columns
ENGpmatchpossession.reset_index(inplace=True)


# Check if the columns are a MultiIndex
if isinstance(ENGpmatchpossession.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchpossession.columns = [''.join(map(str, col)).strip() for col in ENGpmatchpossession.columns]

ENGpmatchpossession.columns = add_prefix_to_columns(ENGpmatchpossession.columns, prefix, prefix_conditions)
ENGpmatchpossession = ENGpmatchpossession.rename(columns={'pmTake-OnsSucc%': 'pmtakeonssuccpc'})
ENGpmatchpossession = ENGpmatchpossession.rename(columns={'pmTake-OnsTkld%': 'pmtakeonstkldpc'})

ENGpmatchpossession = ENGpmatchpossession.rename(columns={'pmTake-OnsAtt': 'pmtakeonsatt'})
ENGpmatchpossession = ENGpmatchpossession.rename(columns={'pmTake-OnsSucc': 'pmtakeonssucc'})
ENGpmatchpossession = ENGpmatchpossession.rename(columns={'pmTake-OnsTkld': 'pmtakeonstkld'})

# Identify numeric columns
numeric_columns = ENGpmatchpossession.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchpossession.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchpossession[numeric_columns] = ENGpmatchpossession[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchpossession[string_columns] = ENGpmatchpossession[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ENGpmatchpossession[numeric_columns] = ENGpmatchpossession[numeric_columns].astype(float)

#misc match stats
ENGpmatchmisc = ENGstats.read_player_match_stats(stat_type="misc")

# Reset the index to turn multi-index into columns
ENGpmatchmisc.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ENGpmatchmisc.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchmisc.columns = [''.join(map(str, col)).strip() for col in ENGpmatchmisc.columns]
ENGpmatchmisc.columns = add_prefix_to_columns(ENGpmatchmisc.columns, prefix, prefix_conditions)

ENGpmatchmisc = ENGpmatchmisc.rename(columns={'pmAerial DuelsWon%': 'pmaerialduelswonpc'})

# Identify numeric columns
numeric_columns = ENGpmatchmisc.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchmisc.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchmisc[numeric_columns] = ENGpmatchmisc[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchmisc[string_columns] = ENGpmatchmisc[string_columns].fillna('UNK')

#passing match stats
ENGpmatchpassing = ENGstats.read_player_match_stats(stat_type="passing")

# Reset the index to turn multi-index into columns
ENGpmatchpassing.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ENGpmatchpassing.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchpassing.columns = [''.join(map(str, col)).strip() for col in ENGpmatchpassing.columns]
ENGpmatchpassing.columns = add_prefix_to_columns(ENGpmatchpassing.columns, prefix, prefix_conditions)

ENGpmatchpassing = ENGpmatchpassing.rename(columns={'pmTotalCmp%': 'pmtotalcmppc'})
ENGpmatchpassing = ENGpmatchpassing.rename(columns={'pmShortCmp%': 'pmshortcmppc'})
ENGpmatchpassing = ENGpmatchpassing.rename(columns={'pmMediumCmp%': 'pmmediumcmppc'})
ENGpmatchpassing = ENGpmatchpassing.rename(columns={'pmLongCmp%': 'pmlongcmppc'})

# Identify numeric columns
numeric_columns = ENGpmatchpassing.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchpassing.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchpassing[numeric_columns] = ENGpmatchpassing[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchpassing[string_columns] = ENGpmatchpassing[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ENGpmatchpassing[numeric_columns] = ENGpmatchpassing[numeric_columns].astype(float)

#passing_types match stats
ENGpmatchpassing_types = ENGstats.read_player_match_stats(stat_type="passing_types")

# Reset the index to turn multi-index into columns
ENGpmatchpassing_types.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ENGpmatchpassing_types.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchpassing_types.columns = [''.join(map(str, col)).strip() for col in ENGpmatchpassing_types.columns]
ENGpmatchpassing_types.columns = add_prefix_to_columns(ENGpmatchpassing_types.columns, prefix, prefix_conditions)

# Identify numeric columns
numeric_columns = ENGpmatchpassing_types.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchpassing_types.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchpassing_types[numeric_columns] = ENGpmatchpassing_types[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchpassing_types[string_columns] = ENGpmatchpassing_types[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ENGpmatchpassing_types[numeric_columns] = ENGpmatchpassing_types[numeric_columns].astype(float)

#defense match stats
ENGpmatchdefense = ENGstats.read_player_match_stats(stat_type="defense")

# Reset the index to turn multi-index into columns
ENGpmatchdefense.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ENGpmatchdefense.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ENGpmatchdefense.columns = [''.join(map(str, col)).strip() for col in ENGpmatchdefense.columns]
ENGpmatchdefense.columns = add_prefix_to_columns(ENGpmatchdefense.columns, prefix, prefix_conditions)

ENGpmatchdefense = ENGpmatchdefense.rename(columns={'pmChallengesTkl%': 'pmchallengestklpc'})
ENGpmatchdefense = ENGpmatchdefense.rename(columns={'pmTacklesDef 3rd': 'pmTacklesTklDef 3rd'})
ENGpmatchdefense = ENGpmatchdefense.rename(columns={'pmTacklesMid 3rd': 'pmTacklesTklMid 3rd'})
ENGpmatchdefense = ENGpmatchdefense.rename(columns={'pmTacklesAtt 3rd': 'pmTacklesTklAtt 3rd'})

# Identify numeric columns
numeric_columns = ENGpmatchdefense.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ENGpmatchdefense.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ENGpmatchdefense[numeric_columns] = ENGpmatchdefense[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ENGpmatchdefense[string_columns] = ENGpmatchdefense[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ENGpmatchdefense[numeric_columns] = ENGpmatchdefense[numeric_columns].astype(float)

# Concatenate with outer join to include all rows
PlayerMatch = pd.concat([ENGpmatchsummary, ENGpmatchpossession, ENGpmatchmisc, ENGpmatchpassing,  
                         ENGpmatchpassing, ENGpmatchpassing_types, ENGpmatchdefense ], axis=1, join='outer')
PlayerMatch = PlayerMatch.loc[:, ~PlayerMatch.columns.duplicated()]
PlayerMatch = PlayerMatch.drop(columns=['Att', 'min', 'game_id'])
PlayerMatch.columns = PlayerMatch.columns.str.replace('-', '')
print(PlayerMatch)
print(PlayerMatch.shape[1])




#Inserts data into database, creates each game row.
def insert_data_into_db(PlayerMatch, conn):
    cur = conn.cursor()

    for index, row in PlayerMatch.iterrows():
        try:
            start_time = time.time()  # Start timing the query

            # Define the unique key or combination of columns to check for duplicates
            unique_columns = ['player', 'match_id', 'league', 'season', 'team', 'game', 'nation', 
                              'pos', 'age', 'jersey_number', 'Playermin'  ]  
            unique_values = [row[col] for col in unique_columns]

            # Construct the query to check if the row already exists
            check_query = 'SELECT COUNT(*) FROM "public"."PlayerMatch" WHERE ' + ' AND '.join(
                [f'"{col}" = %s' for col in unique_columns]
            )
            cur.execute(check_query, unique_values)
            count = cur.fetchone()[0]

            if count == 0:
                # Get the column names from the DataFrame
                columns = list(PlayerMatch.columns)

                # Ensure all column names are properly quoted
                quoted_columns = [f'"{col}"' for col in columns]

                # Get the list of all column names and data types from the PlayerMatch table
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'PlayerMatch'")
                column_info = {row[0]: row[1] for row in cur.fetchall()}

                # Prepare the data for insertion
                values = [row[col] for col in PlayerMatch.columns]

                #last working insert statement, worked for every column that wasnt made into decimal(4,1) ( @5/18 pm)
                insert_statement = 'INSERT INTO "public"."PlayerMatch" ({}) VALUES ({})'.format(
                        ', '.join(['"{}"'.format(col) for col in columns]), 
                        ', '.join(['%s'] * len(columns))
                    )


                print("Values before insertion:", values)
                for i, value in enumerate(values):
                    print(f"Index {i}: {value}")
                print(f"Insert Statement: {insert_statement}")
                cur.execute(insert_statement, values)
                end_time = time.time()  # End timing the query
                print(f"Query time for row {index}: {end_time - start_time} seconds")

            else:
                print(f"Row {index} already exists in the database, skipping insertion.")

        except Exception as e:
            print(f"Error inserting data: {e}")
            print(f"Error occurred at row index {index}: {row}")
            print(traceback.format_exc())  # This prints the stack trace
            conn.rollback()  # Rollback the transaction on error
            continue
    conn.commit()  
    cur.close()

def main():
    try:
        db_password = os.environ.get('DB_PASSWORD')

        # Establish connection to your SQL database
        conn = psycopg2.connect(
            dbname="Tier1", 
            user="postgres",  
            password=db_password,  
            host="localhost",  
            port="5432"
    )

        insert_data_into_db(PlayerMatch, conn)  
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()


# #keepers in seperate table/import file

# #keepers match stats
# ENGpmatchkeepers = ENGstats.read_player_match_stats(stat_type="keepers")













