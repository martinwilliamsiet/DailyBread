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
import io
from io import StringIO

import csv #temp csv step

#takes 8 min with csv export and write

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

# # Define the path to your log file
# log_file_path = r'C:\Users\User\Desktop\NBASOCCERPY2SQLETLDataProject\Soccer Local\Player Match Stats CSVs and DB Import\Argentine League\ARGDB Import Scripts\output.log'

# # Function to redirect stdout and stderr to a log file
# def redirect_output_to_log(log_file_path):
#     # Open the log file in append mode
#     log_file = open(log_file_path, 'a')

#     # Redirect stdout and stderr to the log file
#     sys.stdout = log_file
#     sys.stderr = log_file

# # Redirect output to the log file
# redirect_output_to_log(log_file_path)

# # Your script goes here...
# # All print statements and errors will be logged to 'output.log'

# # Example print statement
# print("This will be logged to the output.log file.")

# # Remember to close the log file when done
# sys.stdout.close()
# sys.stderr.close()


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
expected_columns = [
    "player", "match_id", "league", "season", "team", "game", "nation", "pos", "age", "jersey_number", 
    "Playermin", "pmPerformanceGls", "pmPerformanceAst", "pmPerformancePK", "pmPerformancePKatt", 
    "pmPerformanceSh", "pmPerformanceSoT", "pmPerformanceCrdY", "pmPerformanceCrdR", "pmPerformanceTouches", 
    "pmPerformanceTkl", "pmPerformanceInt", "pmPerformanceBlocks", "pmExpectedxG", "pmExpectednpxG", 
    "pmExpectedxAG", "pmSCASCA", "pmSCAGCA", "pmPassesCmp", "pmPassesAtt", "pmpassescmppc", "pmPassesPrgP", 
    "pmCarriesCarries", "pmCarriesPrgC", "pmtakeonsatt", "pmtakeonssucc", "pmPerformance2CrdY", 
    "pmPerformanceFls", "pmPerformanceFld", "pmPerformanceOff", "pmPerformanceCrs", "pmPerformanceTklW", 
    "pmPerformancePKwon", "pmPerformancePKcon", "pmPerformanceOG", "pmPerformanceRecov", "pmAerial DuelsWon", 
    "pmAerial DuelsLost", "pmaerialduelswonpc", "pmTouchesTouches", "pmTouchesDef Pen", "pmTouchesDef 3rd", 
    "pmTouchesMid 3rd", "pmTouchesAtt 3rd", "pmTouchesAtt Pen", "pmTouchesLive", "pmtakeonssuccpc", 
    "pmtakeonstkld", "pmtakeonstkldpc", "pmCarriesTotDist", "pmCarriesPrgDist", "pmCarries1/3", "pmCarriesCPA", 
    "pmCarriesMis", "pmCarriesDis", "pmReceivingRec", "pmReceivingPrgR", "pmTotalCmp", "pmTotalAtt", 
    "pmtotalcmppc", "pmTotalTotDist", "pmTotalPrgDist", "pmShortCmp", "pmShortAtt", "pmshortcmppc", 
    "pmMediumCmp", "pmMediumAtt", "pmmediumcmppc", "pmLongCmp", "pmLongAtt", "pmlongcmppc", "pmAst", "pmxAG", 
    "pmxA", "pmKP", "pm1/3", "pmPPA", "pmCrsPA", "pmPrgP", "pmPass TypesLive", "pmPass TypesDead", "pmPass TypesFK", 
    "pmPass TypesTB", "pmPass TypesSw", "pmPass TypesCrs", "pmPass TypesTI", "pmPass TypesCK", "pmCorner KicksIn", 
    "pmCorner KicksOut", "pmCorner KicksStr", "pmOutcomesCmp", "pmOutcomesOff", "pmOutcomesBlocks", 
    "pmTacklesTkl", "pmTacklesTklW", "pmTacklesTklDef 3rd", "pmTacklesTklMid 3rd", "pmTacklesTklAtt 3rd", 
    "pmChallengesTkl", "pmChallengesAtt", "pmchallengestklpc", "pmChallengesLost", "pmBlocksBlocks", 
    "pmBlocksSh", "pmBlocksPass", "pmInt", "pmTkl+Int", "pmClr", "pmErr"
]  
PlayerMatch = PlayerMatch[expected_columns]
print(PlayerMatch)
print(PlayerMatch.shape[1])

# # Specify the CSV file path
# csv_file = "C:/Users/User/Desktop/NBASOCCERPY2SQLETLDataProject/Soccer Local/Player Match Stats CSVs and DB Import/Argentine League/ARGDB Import Scripts/PlayerMatch.csv"

# # Write the data to a CSV file
# PlayerMatch.to_csv(csv_file, index=False, encoding='utf-8')  # Write DataFrame to CSV without index

# Convert DataFrame to CSV in memory using StringIO INSTEad of using csv to write to disk
csv_buffer = io.StringIO()
PlayerMatch.to_csv(csv_buffer, index=False, encoding='utf-8', sep='\t')
csv_buffer.seek(0)

# Read the data
data = pd.read_csv(csv_buffer, delimiter='\t')
print(f"Total rows read: {len(data)}")

# Verify the contents of the DataFrame
print(data.head())  # Print the first few rows of the DataFrame

# Write to a new in-memory CSV string using StringIO
output_buffer  = StringIO()
PlayerMatch.to_csv(output_buffer, index=False, sep='\t')
output_buffer.seek(0)

# Verify the contents of the processed data
print(output_buffer.getvalue())


# def main():
#     try:
#         db_password = os.environ.get('DB_PASSWORD')

#         # Establish connection to your SQL database
#         conn = psycopg2.connect(
#             dbname="Tier1", 
#             user="postgres",  
#             password=db_password,  
#             host="localhost",  
#             port="5432"
#     )

#         # csv_file = "C:/Users/User/Desktop/NBASOCCERPY2SQLETLDataProject/Soccer Local/Player Match Stats CSVs and DB Import/Argentine League/ARGDB Import Scripts/PlayerMatch.csv"
#         # with conn.cursor() as cur:
#         #     # Use the COPY command to import data from the CSV file into the PostgreSQL table
#         #     with open(csv_file, 'r', encoding='utf-8') as file:
#         #         cur.copy_expert("COPY public.PlayerMatch FROM STDIN WITH CSV HEADER", file)
#         # conn.commit()
#         # print("Data imported successfully")
#         chunk_size = 500  # Number of rows per batch

#         # Iterate over chunks of the DataFrame
#         for chunk_start in range(0, len(PlayerMatch), chunk_size):
#             chunk_end = min(chunk_start + chunk_size, len(PlayerMatch))
#             chunk = PlayerMatch.iloc[chunk_start:chunk_end]

#             with conn.cursor() as cur:
#                 # Create a StringIO object to hold CSV data
#                 csv_buffer = io.StringIO()
#                 chunk.to_csv(csv_buffer, sep='\t', index=False, header=False)

#                 # Reset the buffer position to start
#                 csv_buffer.seek(0)

#                 # Use copy_from to copy the data from the StringIO buffer to the database
#                 cur.copy_from(csv_buffer, "PlayerMatch", sep='\t') #, null='UNK'

#                 conn.commit()

#         print("Data imported successfully")
#     except psycopg2.IntegrityError as e:
#         # IntegrityError is raised when there's a violation of constraints
#         print(f"IntegrityError: {e}")
#         conn.rollback()  # Rollback the transaction
#     except Exception as e:
#         print(f"An error occurred: {e}")

#     finally:
#         if conn:
#             conn.close()

# if __name__ == "__main__":
#     main()

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

        # Chunk size for processing data in batches
        chunk_size = 500 

        # Iterate over chunks of the DataFrame
        for chunk_start in range(0, len(PlayerMatch), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(PlayerMatch))
            chunk = PlayerMatch.iloc[chunk_start:chunk_end]

            with conn.cursor() as cur:
                # Create a StringIO object to hold CSV data
                csv_buffer = io.StringIO()
                chunk.to_csv(csv_buffer, sep='\t', index=False, header=False)

                # Reset the buffer position to start
                csv_buffer.seek(0)

                try:
                    # Use copy_from to copy the data from the StringIO buffer to the database
                    cur.copy_from(csv_buffer, "PlayerMatch", sep='\t')
                    conn.commit()
                except psycopg2.IntegrityError as e:
                    # IntegrityError is raised when there's a violation of constraints
                    print(f"IntegrityError: {e}")
                    conn.rollback()  # Rollback the transaction for the current chunk

        print("Data imported successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()


# #keepers in seperate table/import file

# #keepers match stats
# ARGpmatchkeepers = ARGstats.read_player_match_stats(stat_type="keepers")