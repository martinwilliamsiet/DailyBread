from selenium import webdriver
from selenium.common.exceptions import WebDriverException #debugging
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys  # scroll
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from bs4 import BeautifulSoup
import csv
import time
import re
import os
import pandas as pd
import psycopg2 # read gameids and match to insert qt/ot scores
import traceback
from selenium.common.exceptions import TimeoutException

# Get the directory of the current script
current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the path to your custom Chrome executable, using developer test version
chrome_binary_path = os.path.join(current_directory,"chrome-win64", "chrome.exe")

# Define the path to the Chrome driver executable relative to the current script directory, driver matches chrome test version
chrome_driver_path = os.path.join(current_directory,"chromedriver-win64", "chromedriver.exe")

# # Check if the path exists -testing
# if not os.path.exists(chrome_driver_path):
#     raise FileNotFoundError(f"ChromeDriver not found at path: {chrome_driver_path}")

# Print paths for debugging
print("Chrome binary path:", chrome_binary_path)
print("Chrome driver path:", chrome_driver_path)

URL = "https://www.flashscore.ca/basketball/usa/nba-2021-2022/results/"

# Initialize WebDriver
chrome_options = Options()
chrome_options.binary_location = chrome_binary_path
chrome_options.add_argument("--window-size=1920,1080")
# driver = webdriver.Chrome(service=service, options=chrome_options)
# driver.get(URL)

#debugging
# Attempt to initialize the WebDriver and catch any exceptions
# try:
#     driver = webdriver.Chrome(service=service, options=chrome_options)
# except WebDriverException as e:
#     print(f"Failed to start WebDriver: {e}")
#     service.stop()
#     raise


# Use WebDriver
try:
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(URL)
    # Do other tasks with the driver


    # Execute JavaScript to dismiss the cookie pop-up
    driver.execute_script("document.querySelector('.ot-pc-refuse-all-handler').click();")

    # Wait for the page to fully load after dismissing the pop-up
    WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'ot-floating-bar-show')))

    # Define a function to click the "Show more games" button
    def click_show_more_games(driver):
        show_more_button = driver.find_element(By.CSS_SELECTOR, 'a.event__more.event__more--static')
        actions = ActionChains(driver)
        actions.move_to_element(show_more_button).click().perform()

    # Define the function to convert time to 24-hour format
    def convert_to_24_hour(time_str):
        return datetime.strptime(time_str, "%I:%M %p").strftime("%H%M")

    # Click the "Show more games" button until it's no longer clickable
    while True:
        try:
            click_show_more_games(driver)
            time.sleep(6)  # Wait for more games to load
        except:
            break  # Break the loop if the button is no longer clickable

    # Wait for the parent div to load
    parent_div = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'sportName.basketball')))

    # Get the HTML content of the parent div
    html_content = parent_div.get_attribute('innerHTML')

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all divs within the parent div
    divs_in_parent = soup.find_all('div', class_='event__match')

    # Initialize game_data as an empty list
    game_data = []

    # Dictionary to map team names to their abbreviations
    team_abbreviations = {
        "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHH",
        "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE", "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN",
        "Detroit Pistons": "DET", "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
        "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM", "Miami Heat": "MIA",
        "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP", "New York Knicks": "NYK",
        "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
        "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
        "Utah Jazz": "UTA", "Washington Wizards": "WAS"
    }

    # Extract game information, inverted from scrape so inverting here
    for div in divs_in_parent:
        game_info = {}
        # Extract home team name PARSE ABBR
        home_team_div = div.find('div', class_='event__participant--home')
        if home_team_div:
            away_team_name = home_team_div.text.strip() #swap home/vis here
            game_info['visitorteamname'] = away_team_name #swap home/vis here
            away_team_abbr = team_abbreviations.get(away_team_name, 'UNK') #swap home/vis here

        # REMOVE '@', PARSE ABBR Extract away team name
        away_team_div = div.find('div', class_='event__participant--away')
        if away_team_div:
            home_team_name = away_team_div.text.strip().lstrip('@')  #swap home/vis here # Remove leading '@' symbol
            game_info['hometeamname'] = home_team_name #swap home/vis here
            home_team_abbr = team_abbreviations.get(home_team_name, 'UNK') #swap home/vis here

        # PARSE MONTH/TIME Extract event time 
        event_time_div = div.find('div', class_='event__time')
        if event_time_div:
            event_time = event_time_div.text.strip().lstrip('AOT')
            game_info['eventtime'] = event_time

        for i in range(1, 7):  
            if i <= 4:  # For quarters 1 to 4
                score_type = 'q'
            else:  # For overtime periods
                score_type = 'ot'

            if i <= 4:
                home_label = f'visitorq{i}' #swap home/vis here
                visitor_label = f'homeq{i}' #swap home/vis here
            else:
                ot_number = i - 4
                home_label = f'visitorot{ot_number}' #swap home/vis here
                visitor_label = f'homeot{ot_number}' #swap home/vis here

            home_score_div = div.find('div', class_=f'event__part event__part--home event__part--{i}')
            visitor_score_div = div.find('div', class_=f'event__part event__part--away event__part--{i}')

            if home_score_div:
                game_info[home_label] = home_score_div.text.strip()
            else:
                game_info[home_label] = None

            if visitor_score_div:
                game_info[visitor_label] = visitor_score_div.text.strip()
            else:
                game_info[visitor_label] = None


        game_data.append(game_info)

finally:
    driver.quit()
    service.stop()

# Convert game_data into a pandas DataFrame
games_df = pd.DataFrame(game_data)

# Define a month mapping dictionary
month_map = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
    'ct': '10', 'pr': '04'
}

# Create the gameid column
games_df['gameid'] = ''

for index, row in games_df.iterrows():
    # Get the home team name from the current row
    home_team_name = row['hometeamname']
    
    # Get the abbreviation for the home team
    home_team_abbr = team_abbreviations.get(home_team_name, 'UNK')
    
    # Assign the abbreviation to the 'homeabbr' column in the current row
    games_df.at[index, 'homeabbr'] = home_team_abbr

    # Get the away team name from the current row
    away_team_name = row['visitorteamname']
    
    # Get the abbreviation for the away team
    away_team_abbr = team_abbreviations.get(away_team_name, 'UNK')
    
    # Assign the abbreviation to the 'visitorabbr' column in the current row
    games_df.at[index, 'visitorabbr'] = away_team_abbr

    # Get event time from the current row
    event_time = row['eventtime']

    # Extract month, day, time, and period from the event time
    month_abbr, original_day_time, period = event_time.replace('AOT', '').replace('OT', '').split(' ')

    # Split the original_day_time into day and time
    day = original_day_time[:2]
    time = original_day_time[2:]

    # Reconstruct time_period with the desired format
    time_period = f"{time} {period}"

    time_24_hour = convert_to_24_hour(time_period) 

    #Map period to 24-hour format
    #if period == 'PM' and time == '12:00' and ':' in time:  # Check if time is not empty and contains ':'
     #   time_parts = time.split(':')
      #  hour = int(time_parts[0])
       # if hour != 12:
        #    hour += 12  # Convert PM hour to 24-hour format
        #time = f"{hour02d}{time_parts[1]}"  # Format the hour to two digits
                                            
    # Extract month abbreviation and convert to numeric month
    month = month_map.get(month_abbr, '00')

    # Determine the year based on the month
    year = '2022' if month_abbr in ['Jan', 'Feb', 'Mar', 'Apr','pr', 'May', 'Jun'] else '2021'

    # Create the game ID
    gameid = year + month + day + time_24_hour + away_team_abbr + home_team_abbr

    # Convert gameid to string and remove the colon
    gameid = str(gameid)

    # Assign the game ID to the current row in the DataFrame
    games_df.at[index, 'gameid'] = gameid

db_password = os.environ.get('DB_PASSWORD')

# Establish connection to your SQL database
conn = psycopg2.connect(
    dbname="NBA", 
    user="postgres",  
    password=db_password,  
    host="localhost",  
    port="5432"
)
# Define a function to insert data into the database
def insert_data_into_db(games_df, conn):
    cur = conn.cursor()
    for index, row in games_df.iterrows():
        try:
            # Strip leading and trailing whitespaces from the gameid
            gameid = row['gameid'].strip()

            # Match gameid with the ones in your SQL database
            cur.execute("SELECT * FROM games WHERE gameid=%s", (row['gameid'],))

            match = cur.fetchone()
            if match:
                print("Inserting data into database:")
                print(f"gameid: {gameid}")
                print(f"visitorq1: {row['visitorq1']}, visitorq2: {row['visitorq2']}, visitorq3: {row['visitorq3']}, visitorq4: {row['visitorq4']}")
                print(f"visitorot1: {row['visitorot1']}, visitorot2: {row['visitorot2']}")
                print(f"homeq1: {row['homeq1']}, homeq2: {row['homeq2']}, homeq3: {row['homeq3']}, homeq4: {row['homeq4']}")
                print(f"homeot1: {row['homeot1']}, homeot2: {row['homeot2']}")
                # Insert corresponding quarter and OT scores into the database
                cur.execute("""UPDATE games SET visitorq1=%s, visitorq2=%s, visitorq3=%s, visitorq4=%s, visitorot1=%s,
                            visitorot2=%s, homeq1=%s, homeq2=%s, homeq3=%s, homeq4=%s, homeot1=%s, homeot2=%s WHERE gameid=%s""",
                            (row['visitorq1'], row['visitorq2'], row['visitorq3'], row['visitorq4'],
                             row['visitorot1'], row['visitorot2'], row['homeq1'],row['homeq2'], row['homeq3'], 
                             row['homeq4'], row['homeot1'], row['homeot2'], row['gameid']))
                conn.commit()
            else:
                print(f"No match found for gameid: {gameid}. Skipping insertion.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            print(f"Error occurred at row index {index}: {row}")
            print(traceback.format_exc())  # This prints the stack trace
            conn.rollback()  # Rollback the transaction on error
            continue

try:
    # Call the function to insert data into the database
    insert_data_into_db(games_df, conn)
finally:
    # Close the database connection
    conn.close()

# Output the DataFrame to a CSV file
csv_filename = "NBA21_22seasondata.csv"
games_df.to_csv(csv_filename, index=False)
#Open the CSV file with the default application
os.system(f'start {csv_filename}')
