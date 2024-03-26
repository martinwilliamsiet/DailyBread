#Script is specifically for NBAscraper api to create rows in NBA database, I have made seperate script for current 
#and past NBA seasons to fill in qt/ot scores AFTER this one creates the row.

from nba_data_scraper import NBAScraper
from datetime import datetime
import pandas as pd
import psycopg2
import sys
import traceback
import os

def parse_DateStr(DateStr):
    # Extract components from the string
    year = int(DateStr[0:4])
    month = int(DateStr[4:6])
    day = int(DateStr[6:8])
    hour = int(DateStr[8:10])
    minute = int(DateStr[10:12])

    # Create a datetime object
    date_time_obj = datetime(year, month, day, hour, minute)

    # Format date and time in SQL-friendly formats
    GameDate = date_time_obj.strftime('%Y-%m-%d')
    GameStartTime = date_time_obj.strftime('%H:%M:%S')
    return GameDate, GameStartTime

#Inserts data into database, creates each game row.
def insert_data_into_db(schedule_data, conn):
    cur = conn.cursor()
    for index, row in schedule_data.iterrows():
        try:
            # Convert 'DateStr' into 'date' and 'start_time'
            GameDate, GameStartTime = parse_DateStr(row['DateStr'])
            GameID = row['game_id']
            # Check if a record with this GameID already exists, if not create, if exists, skip
            cur.execute("SELECT 1 FROM games WHERE GameID = %s", (GameID,))
            if cur.fetchone():
                print(f"GameID {GameID} already exists. Skipping.")
                continue
            VisitorTeamName = row['Visitor']  
            HomeTeamName = row['Home']
            # Set quarter and overtime scores to 0 as placeholders
            VisitorQ1 = VisitorQ2 = VisitorQ3 = VisitorQ4 = VisitorOT1 = VisitorOT2 = 0
            HomeQ1 = HomeQ2 = HomeQ3 = HomeQ4 = HomeOT1 = HomeOT2 = 0
            # Convert 'Visitor PTS' and 'Home PTS' to integers or set to None if NaN
            VisitorTOTPoints = None if pd.isna(row['Visitor PTS']) else int(row['Visitor PTS'])
            #OLDHomeTOTPoints = row.get('Home PTS', None)
            HomeTOTPoints = None if pd.isna(row['Home PTS']) else int(row['Home PTS'])
            #OLDAttendance = row.get('Attendance', None) 
            if pd.isna(row['Attendance']) or row['Attendance'] == '':
                Attendance = None
            else:
                Attendance = int(float(row['Attendance']))
            Arena = row['Arena']
            #INSERT statement based on table structure, """ prevents sql injection attack"
            insert_query = """
            INSERT INTO games (GameID, VisitorTeamName, HomeTeamName, GameStartTime, GameDate, VisitorQ1, VisitorQ2,
                               VisitorQ3, VisitorQ4, VisitorOT1, VisitorOT2, VisitorTOTPoints, HomeQ1, HomeQ2, HomeQ3,
                               HomeQ4, HomeOT1, HomeOT2, HomeTOTPoints, Attendance, Arena      )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """ 
            #data type investigation prints below
            #print("Home total points data type:", type(HomeTOTPoints))
            #print("Visitor total points data type:", type(VisitorTOTPoints))
            #print("Attendance data type:", type(Attendance))
            # two print debugs below
            #print("Attendance:", Attendance)
            #print("Integer:", [type(Integer) for value in Attendance])
            cur.execute(insert_query, (GameID, VisitorTeamName, HomeTeamName, GameStartTime, GameDate, 
                           VisitorQ1, VisitorQ2, VisitorQ3, VisitorQ4, VisitorOT1, VisitorOT2, VisitorTOTPoints, 
                           HomeQ1, HomeQ2, HomeQ3, HomeQ4, HomeOT1, HomeOT2, HomeTOTPoints, 
                           Attendance, Arena))
        except Exception as e:
            print(f"Error inserting data: {e}")
            print(f"Error occurred at row index {index}: {row}")
            print(traceback.format_exc())  # This prints the stack trace
            conn.rollback()  # Rollback the transaction on error
            continue
    conn.commit()  
    cur.close()

def main():
    nba_scraper = NBAScraper()
    schedule_data = nba_scraper.scrape_schedule_data(year='2024', month='march') 
    df = pd.DataFrame(schedule_data)
    
    db_password = os.environ.get('DB_PASSWORD')

    # Establish connection to your SQL database
    conn = psycopg2.connect(
        dbname="NBA", 
        user="postgres",  
        password=db_password,  
        host="localhost",  
        port="5432"
)

    insert_data_into_db(df, conn)  
    conn.close()

if __name__ == "__main__":
    main()