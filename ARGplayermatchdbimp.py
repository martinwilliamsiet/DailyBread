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
ARGstats = sd.FBref(leagues="ARG-ArgentinePrimeraDivisión", seasons='2324')

#player summary stats
ARGpmatchsummary = ARGstats.read_player_match_stats(stat_type="summary")
#leave match_id variable off to include all match ids, testing with f97b9a22 for db import

# Reset the index to turn multi-index into columns
ARGpmatchsummary.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ARGpmatchsummary.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchsummary.columns = [''.join(map(str, col)).strip() for col in ARGpmatchsummary.columns]

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

ARGpmatchsummary.columns = add_prefix_to_columns(ARGpmatchsummary.columns, prefix, prefix_conditions)

ARGpmatchsummary = ARGpmatchsummary.rename(columns={'game_id': 'match_id'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'min': 'Playermin'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmPassesCmp%': 'pmpassescmppc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmAerial DuelsWon%': 'pmaerialduelswonpc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmTake-OnsSucc%': 'pmtakeonssuccpc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmTake-OnsAtt': 'pmtakeonsatt'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmTake-OnsSucc': 'pmtakeonssucc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmTotalCmp%': 'pmtotalcmppc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmShortCmp%': 'pmchortcmppc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmMediumCmp%': 'pmmediumcmppc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmLongCmp%': 'pmlongcmppc'})
ARGpmatchsummary = ARGpmatchsummary.rename(columns={'pmChallengesTkl%': 'pmchallengestklpc'})

# Identify numeric columns
numeric_columns = ARGpmatchsummary.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchsummary.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchsummary[numeric_columns] = ARGpmatchsummary[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchsummary[string_columns] = ARGpmatchsummary[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ARGpmatchsummary[numeric_columns] = ARGpmatchsummary[numeric_columns].astype(float)

# #check new column renaming
# print(ARGpmatchsummary)

#possession match stats
ARGpmatchpossession = ARGstats.read_player_match_stats(stat_type="possession")

# Reset the index to turn multi-index into columns
ARGpmatchpossession.reset_index(inplace=True)


# Check if the columns are a MultiIndex
if isinstance(ARGpmatchpossession.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchpossession.columns = [''.join(map(str, col)).strip() for col in ARGpmatchpossession.columns]

ARGpmatchpossession.columns = add_prefix_to_columns(ARGpmatchpossession.columns, prefix, prefix_conditions)
ARGpmatchpossession = ARGpmatchpossession.rename(columns={'pmTake-OnsSucc%': 'pmtakeonssuccpc'})
ARGpmatchpossession = ARGpmatchpossession.rename(columns={'pmTake-OnsTkld%': 'pmtakeonstkldpc'})

ARGpmatchpossession = ARGpmatchpossession.rename(columns={'pmTake-OnsAtt': 'pmtakeonsatt'})
ARGpmatchpossession = ARGpmatchpossession.rename(columns={'pmTake-OnsSucc': 'pmtakeonssucc'})
ARGpmatchpossession = ARGpmatchpossession.rename(columns={'pmTake-OnsTkld': 'pmtakeonstkld'})

# Identify numeric columns
numeric_columns = ARGpmatchpossession.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchpossession.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchpossession[numeric_columns] = ARGpmatchpossession[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchpossession[string_columns] = ARGpmatchpossession[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ARGpmatchpossession[numeric_columns] = ARGpmatchpossession[numeric_columns].astype(float)

# ARGpmatchpossessiondrop = ARGpmatchpossession.drop(columns=[('game_id', 'season', 'league', 'team', 'game',
#                                                                 'player', 'nation', 'pos', 'age', 'jersey_number',
#                                                                 'min', 'pmTake-OnsAtt', 'pmTake-OnsSucc', 'pmCarriesCarries',
#                                                                   'pmCarriesPrgC')])



#misc match stats
ARGpmatchmisc = ARGstats.read_player_match_stats(stat_type="misc")

# Reset the index to turn multi-index into columns
ARGpmatchmisc.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ARGpmatchmisc.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchmisc.columns = [''.join(map(str, col)).strip() for col in ARGpmatchmisc.columns]
ARGpmatchmisc.columns = add_prefix_to_columns(ARGpmatchmisc.columns, prefix, prefix_conditions)

ARGpmatchmisc = ARGpmatchmisc.rename(columns={'pmAerial DuelsWon%': 'pmaerialduelswonpc'})

# Identify numeric columns
numeric_columns = ARGpmatchmisc.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchmisc.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchmisc[numeric_columns] = ARGpmatchmisc[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchmisc[string_columns] = ARGpmatchmisc[string_columns].fillna('UNK')

#passing match stats
ARGpmatchpassing = ARGstats.read_player_match_stats(stat_type="passing")

# Reset the index to turn multi-index into columns
ARGpmatchpassing.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ARGpmatchpassing.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchpassing.columns = [''.join(map(str, col)).strip() for col in ARGpmatchpassing.columns]
ARGpmatchpassing.columns = add_prefix_to_columns(ARGpmatchpassing.columns, prefix, prefix_conditions)

ARGpmatchpassing = ARGpmatchpassing.rename(columns={'pmTotalCmp%': 'pmtotalcmppc'})
ARGpmatchpassing = ARGpmatchpassing.rename(columns={'pmShortCmp%': 'pmshortcmppc'})
ARGpmatchpassing = ARGpmatchpassing.rename(columns={'pmMediumCmp%': 'pmmediumcmppc'})
ARGpmatchpassing = ARGpmatchpassing.rename(columns={'pmLongCmp%': 'pmlongcmppc'})

# Identify numeric columns
numeric_columns = ARGpmatchpassing.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchpassing.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchpassing[numeric_columns] = ARGpmatchpassing[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchpassing[string_columns] = ARGpmatchpassing[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ARGpmatchpassing[numeric_columns] = ARGpmatchpassing[numeric_columns].astype(float)

#passing_types match stats
ARGpmatchpassing_types = ARGstats.read_player_match_stats(stat_type="passing_types")

# Reset the index to turn multi-index into columns
ARGpmatchpassing_types.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ARGpmatchpassing_types.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchpassing_types.columns = [''.join(map(str, col)).strip() for col in ARGpmatchpassing_types.columns]
ARGpmatchpassing_types.columns = add_prefix_to_columns(ARGpmatchpassing_types.columns, prefix, prefix_conditions)

# Identify numeric columns
numeric_columns = ARGpmatchpassing_types.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchpassing_types.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchpassing_types[numeric_columns] = ARGpmatchpassing_types[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchpassing_types[string_columns] = ARGpmatchpassing_types[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ARGpmatchpassing_types[numeric_columns] = ARGpmatchpassing_types[numeric_columns].astype(float)

#defense match stats
ARGpmatchdefense = ARGstats.read_player_match_stats(stat_type="defense")

# Reset the index to turn multi-index into columns
ARGpmatchdefense.reset_index(inplace=True)

# Check if the columns are a MultiIndex
if isinstance(ARGpmatchdefense.columns, pd.MultiIndex):
    # Flatten the multi-level column headers
    ARGpmatchdefense.columns = [''.join(map(str, col)).strip() for col in ARGpmatchdefense.columns]
ARGpmatchdefense.columns = add_prefix_to_columns(ARGpmatchdefense.columns, prefix, prefix_conditions)

ARGpmatchdefense = ARGpmatchdefense.rename(columns={'pmChallengesTkl%': 'pmchallengestklpc'})
ARGpmatchdefense = ARGpmatchdefense.rename(columns={'pmTacklesDef 3rd': 'pmTacklesTklDef 3rd'})
ARGpmatchdefense = ARGpmatchdefense.rename(columns={'pmTacklesMid 3rd': 'pmTacklesTklMid 3rd'})
ARGpmatchdefense = ARGpmatchdefense.rename(columns={'pmTacklesAtt 3rd': 'pmTacklesTklAtt 3rd'})

# Identify numeric columns
numeric_columns = ARGpmatchdefense.select_dtypes(include=['number']).columns

# Identify string columns
string_columns = ARGpmatchdefense.select_dtypes(include=['object', 'string']).columns

# Replace NaN with 0 in numeric columns
ARGpmatchdefense[numeric_columns] = ARGpmatchdefense[numeric_columns].fillna(0)

# Replace NaN with UNK in string columns
ARGpmatchdefense[string_columns] = ARGpmatchdefense[string_columns].fillna('UNK')

# Ensure the data types are correct for numeric columns
ARGpmatchdefense[numeric_columns] = ARGpmatchdefense[numeric_columns].astype(float)

# Concatenate with outer join to include all rows
PlayerMatch = pd.concat([ARGpmatchsummary, ARGpmatchpossession, ARGpmatchmisc, ARGpmatchpassing,  
                         ARGpmatchpassing, ARGpmatchpassing_types, ARGpmatchdefense ], axis=1, join='outer')
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
                              'pos', 'age', 'jersey_number', 'Playermin'  ]  # Replace with your actual unique columns
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
# ARGpmatchkeepers = ARGstats.read_player_match_stats(stat_type="keepers")













