import functions_framework
import os
import requests
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
import pytz
import numpy as np
import json
from bs4 import BeautifulSoup
import kenpompy.utils
import kenpompy.summary
import kenpompy.misc
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import traceback
from kenpompy.utils import login

# ODDS API MAP UPDATED FOR KENPOM
odds_api_team_mapping = {
    "Abilene Christian Wildcats": "Abilene Christian",
    "Air Force Falcons": "Air Force",
    "Akron Zips": "Akron",
    "Alabama A&M Bulldogs": "Alabama A&M",
    "Alabama Crimson Tide": "Alabama",
    "Alabama St Hornets": "Alabama St.",
    "Albany Great Danes": "Albany",
    "Alcorn St Braves": "Alcorn St.",
    "American Eagles": "American",
    "Appalachian St Mountaineers": "Appalachian St.",
    "Arizona St Sun Devils": "Arizona St.",
    "Arizona Wildcats": "Arizona",
    "Arkansas Razorbacks": "Arkansas",
    "Arkansas St Red Wolves": "Arkansas St.",
    "Arkansas-Little Rock Trojans": "Little Rock",
    "Arkansas-Pine Bluff Golden Lions": "Arkansas Pine Bluff",
    "Army Knights": "Army",
    "Auburn Tigers": "Auburn",
    "Austin Peay Governors": "Austin Peay",
    "Ball State Cardinals": "Ball St.",
    "Baylor Bears": "Baylor",
    "Belmont Bruins": "Belmont",
    "Bellarmine Knights": "Bellarmine",
    "Bethune-Cookman Wildcats": "Bethune Cookman",
    "Binghamton Bearcats": "Binghamton",
    "Boise State Broncos": "Boise St.",
    "Boston College Eagles": "Boston College",
    "Boston Univ. Terriers": "Boston University",
    "Bowling Green Falcons": "Bowling Green",
    "Bradley Braves": "Bradley",
    "Brown Bears": "Brown",
    "Bryant Bulldogs": "Bryant",
    "Bucknell Bison": "Bucknell",
    "Buffalo Bulls": "Buffalo",
    "Butler Bulldogs": "Butler",
    "BYU Cougars": "BYU",
    "Cal Baptist Lancers": "Cal Baptist",
    "Cal Poly Mustangs": "Cal Poly",
    "California Golden Bears": "California",
    "Campbell Fighting Camels": "Campbell",
    "Canisius Golden Griffins": "Canisius",
    "Central Arkansas Bears": "Central Arkansas",
    "Central Connecticut St Blue Devils": "Central Connecticut",
    "Central Michigan Chippewas": "Central Michigan",
    "Charleston Cougars": "Charleston",
    "Charleston Southern Buccaneers": "Charleston Southern",
    "Charlotte 49ers": "Charlotte",
    "Chattanooga Mocs": "Chattanooga",
    "Chicago St Cougars": "Chicago St.",
    "Cincinnati Bearcats": "Cincinnati",
    "Clemson Tigers": "Clemson",
    "Cleveland St Vikings": "Cleveland St.",
    "Coastal Carolina Chanticleers": "Coastal Carolina",
    "Colgate Raiders": "Colgate",
    "Colorado Buffaloes": "Colorado",
    "Colorado St Rams": "Colorado St.",
    "Columbia Lions": "Columbia",
    "Coppin St Eagles": "Coppin St.",
    "Cornell Big Red": "Cornell",
    "Creighton Bluejays": "Creighton",
    "CSU Bakersfield Roadrunners": "Cal St. Bakersfield",
    "CSU Fullerton Titans": "Cal St. Fullerton",
    "CSU Northridge Matadors": "Cal St. Northridge",
    "Dartmouth Big Green": "Dartmouth",
    "Davidson Wildcats": "Davidson",
    "Dayton Flyers": "Dayton",
    "Delaware Blue Hens": "Delaware",
    "Delaware St Hornets": "Delaware St.",
    "Denver Pioneers": "Denver",
    "DePaul Blue Demons": "DePaul",
    "Detroit Mercy Titans": "Detroit Mercy",
    "Drake Bulldogs": "Drake",
    "Drexel Dragons": "Drexel",
    "Duke Blue Devils": "Duke",
    "Duquesne Dukes": "Duquesne",
    "East Carolina Pirates": "East Carolina",
    "East Tennessee St Buccaneers": "East Tennessee St.",
    "Eastern Illinois Panthers": "Eastern Illinois",
    "Eastern Kentucky Colonels": "Eastern Kentucky",
    "Eastern Michigan Eagles": "Eastern Michigan",
    "Eastern Washington Eagles": "Eastern Washington",
    "Elon Phoenix": "Elon",
    "Evansville Purple Aces": "Evansville",
    "Fairfield Stags": "Fairfield",
    "Fairleigh Dickinson Knights": "Fairleigh Dickinson",
    "Florida A&M Rattlers": "Florida A&M",
    "Florida Atlantic Owls": "Florida Atlantic",
    "Florida Gators": "Florida",
    "Florida Gulf Coast Eagles": "Florida Gulf Coast",
    "Florida Int'l Golden Panthers": "FIU",
    "Florida St Seminoles": "Florida St.",
    "Fordham Rams": "Fordham",
    "Fort Wayne Mastodons": "Purdue Fort Wayne",
    "Fresno St Bulldogs": "Fresno St.",
    "Furman Paladins": "Furman",
    "Gardner-Webb Bulldogs": "Gardner Webb",
    "George Mason Patriots": "George Mason",
    "George Washington Colonials": "George Washington",
    "Georgetown Hoyas": "Georgetown",
    "Georgia Bulldogs": "Georgia",
    "Georgia Southern Eagles": "Georgia Southern",
    "Georgia St Panthers": "Georgia St.",
    "Georgia Tech Yellow Jackets": "Georgia Tech",
    "Gonzaga Bulldogs": "Gonzaga",
    "Grambling St Tigers": "Grambling St.",
    "Grand Canyon Antelopes": "Grand Canyon",
    "Green Bay Phoenix": "Green Bay",
    "Hampton Pirates": "Hampton",
    "Harvard Crimson": "Harvard",
    "Hawai'i Rainbow Warriors": "Hawaii",
    "High Point Panthers": "High Point",
    "Hofstra Pride": "Hofstra",
    "Holy Cross Crusaders": "Holy Cross",
    "Houston Baptist Huskies": "Houston Christian",
    "Houston Cougars": "Houston",
    "Howard Bison": "Howard",
    "Idaho State Bengals": "Idaho St.",
    "Idaho Vandals": "Idaho",
    "Illinois Fighting Illini": "Illinois",
    "Illinois St Redbirds": "Illinois St.",
    "Incarnate Word Cardinals": "Incarnate Word",
    "Indiana Hoosiers": "Indiana",
    "Indiana St Sycamores": "Indiana St.",
    "Iona Gaels": "Iona",
    "Iowa Hawkeyes": "Iowa",
    "Iowa State Cyclones": "Iowa St.",
    "IUPUI Jaguars": "IUPUI",
    "Jackson St Tigers": "Jackson St.",
    "Jacksonville Dolphins": "Jacksonville",
    "Jacksonville St Gamecocks": "Jacksonville St.",
    "James Madison Dukes": "James Madison",
    "Kansas Jayhawks": "Kansas",
    "Kansas St Wildcats": "Kansas St.",
    "Kennesaw St Owls": "Kennesaw St.",
    "Kent State Golden Flashes": "Kent St.",
    "Kentucky Wildcats": "Kentucky",
    "La Salle Explorers": "La Salle",
    "Lafayette Leopards": "Lafayette",
    "Lamar Cardinals": "Lamar",
    "Le Moyne Dolphins": "Le Moyne",
    "Lehigh Mountain Hawks": "Lehigh",
    "Liberty Flames": "Liberty",
    "Lindenwood Lions": "Lindenwood",
    "Lipscomb Bisons": "Lipscomb",
    "LIU Sharks": "LIU",
    "Long Beach St 49ers": "Long Beach St.",
    "Longwood Lancers": "Longwood",
    "Louisiana Ragin' Cajuns": "Louisiana",
    "Louisiana Tech Bulldogs": "Louisiana Tech",
    "Louisville Cardinals": "Louisville",
    "Loyola (Chi) Ramblers": "Loyola Chicago",
    "Loyola (MD) Greyhounds": "Loyola MD",
    "Loyola Marymount Lions": "Loyola Marymount",
    "LSU Tigers": "LSU",
    "Maine Black Bears": "Maine",
    "Manhattan Jaspers": "Manhattan",
    "Marist Red Foxes": "Marist",
    "Marquette Golden Eagles": "Marquette",
    "Marshall Thundering Herd": "Marshall",
    "Maryland Terrapins": "Maryland",
    "Maryland-Eastern Shore Hawks": "Maryland Eastern Shore",
    "Massachusetts Minutemen": "Massachusetts",
    "McNeese Cowboys": "McNeese St.",
    "Memphis Tigers": "Memphis",
    "Mercer Bears": "Mercer",
    "Merrimack Warriors": "Merrimack",
    "Miami (OH) RedHawks": "Miami OH",
    "Miami Hurricanes": "Miami FL",
    "Michigan St Spartans": "Michigan St.",
    "Michigan Wolverines": "Michigan",
    "Middle Tennessee Blue Raiders": "Middle Tennessee",
    "Milwaukee Panthers": "Milwaukee",
    "Minnesota Golden Gophers": "Minnesota",
    "Miss Valley St Delta Devils": "Mississippi Valley St.",
    "Mississippi St Bulldogs": "Mississippi St.",
    "Missouri St Bears": "Missouri St.",
    "Missouri Tigers": "Missouri",
    "Monmouth Hawks": "Monmouth",
    "Montana Grizzlies": "Montana",
    "Montana St Bobcats": "Montana St.",
    "Morehead St Eagles": "Morehead St.",
    "Morgan St Bears": "Morgan St.",
    "Mt. St. Mary's Mountaineers": "Mount St. Mary's",
    "Murray St Racers": "Murray St.",
    "N Colorado Bears": "Northern Colorado",
    "Navy Midshipmen": "Navy",
    "NC State Wolfpack": "N.C. State",
    "NJIT Highlanders": "NJIT",
    "Nebraska Cornhuskers": "Nebraska",
    "Nevada Wolf Pack": "Nevada",
    "New Hampshire Wildcats": "New Hampshire",
    "New Mexico Lobos": "New Mexico",
    "New Mexico St Aggies": "New Mexico St.",
    "New Orleans Privateers": "New Orleans",
    "Niagara Purple Eagles": "Niagara",
    "Nicholls St Colonels": "Nicholls St.",
    "Norfolk St Spartans": "Norfolk St.",
    "North Alabama Lions": "North Alabama",
    "North Carolina A&T Aggies": "North Carolina A&T",
    "North Carolina Central Eagles": "North Carolina Central",
    "North Carolina Tar Heels": "North Carolina",
    "North Dakota Fighting Hawks": "North Dakota",
    "North Dakota St Bison": "North Dakota St.",
    "North Florida Ospreys": "North Florida",
    "North Texas Mean Green": "North Texas",
    "Northeastern Huskies": "Northeastern",
    "Northern Arizona Lumberjacks": "Northern Arizona",
    "Northern Illinois Huskies": "Northern Illinois",
    "Northern Iowa Panthers": "Northern Iowa",
    "Northern Kentucky Norse": "Northern Kentucky",
    "Northwestern St Demons": "Northwestern St.",
    "Northwestern Wildcats": "Northwestern",
    "Notre Dame Fighting Irish": "Notre Dame",
    "Oakland Golden Grizzlies": "Oakland",
    "Ohio Bobcats": "Ohio",
    "Ohio State Buckeyes": "Ohio St.",
    "Oklahoma Sooners": "Oklahoma",
    "Oklahoma St Cowboys": "Oklahoma St.",
    "Old Dominion Monarchs": "Old Dominion",
    "Ole Miss Rebels": "Mississippi",
    "Omaha Mavericks": "Nebraska Omaha",
    "Oral Roberts Golden Eagles": "Oral Roberts",
    "Oregon Ducks": "Oregon",
    "Oregon St Beavers": "Oregon St.",
    "Pacific Tigers": "Pacific",
    "Penn State Nittany Lions": "Penn St.",
    "Pennsylvania Quakers": "Penn",
    "Pepperdine Waves": "Pepperdine",
    "Pittsburgh Panthers": "Pittsburgh",
    "Portland Pilots": "Portland",
    "Portland St Vikings": "Portland St.",
    "Prairie View Panthers": "Prairie View A&M",
    "Presbyterian Blue Hose": "Presbyterian",
    "Princeton Tigers": "Princeton",
    "Providence Friars": "Providence",
    "Purdue Boilermakers": "Purdue",
    "Queens University Royals": "Queens",
    "Quinnipiac Bobcats": "Quinnipiac",
    "Radford Highlanders": "Radford",
    "Rhode Island Rams": "Rhode Island",
    "Rice Owls": "Rice",
    "Richmond Spiders": "Richmond",
    "Rider Broncs": "Rider",
    "Robert Morris Colonials": "Robert Morris",
    "Rutgers Scarlet Knights": "Rutgers",
    "Sacramento St Hornets": "Sacramento St.",
    "Sacred Heart Pioneers": "Sacred Heart",
    "Saint Joseph's Hawks": "Saint Joseph's",
    "Saint Louis Billikens": "Saint Louis",
    "Saint Mary's Gaels": "Saint Mary's",
    "Saint Peter's Peacocks": "Saint Peter's",
    "Sam Houston St Bearkats": "Sam Houston St.",
    "Samford Bulldogs": "Samford",
    "San Diego St Aztecs": "San Diego St.",
    "San Diego Toreros": "San Diego",
    "San Francisco Dons": "San Francisco",
    "San José St Spartans": "San Jose St.",
    "Santa Clara Broncos": "Santa Clara",
    "SE Louisiana Lions": "Southeastern Louisiana",
    "SE Missouri St Redhawks": "Southeast Missouri St.",
    "Seattle Redhawks": "Seattle",
    "Seton Hall Pirates": "Seton Hall",
    "Siena Saints": "Siena",
    "SIU-Edwardsville Cougars": "SIU Edwardsville",
    "SMU Mustangs": "SMU",
    "South Alabama Jaguars": "South Alabama",
    "South Carolina Gamecocks": "South Carolina",
    "South Carolina St Bulldogs": "South Carolina St.",
    "South Carolina Upstate Spartans": "USC Upstate",
    "South Dakota Coyotes": "South Dakota",
    "South Dakota St Jackrabbits": "South Dakota St.",
    "South Florida Bulls": "South Florida",
    "Southern Illinois Salukis": "Southern Illinois",
    "Southern Indiana Screaming Eagles": "Southern Indiana",
    "Southern Jaguars": "Southern",
    "Southern Miss Golden Eagles": "Southern Miss",
    "Southern Utah Thunderbirds": "Southern Utah",
    "St. Bonaventure Bonnies": "St. Bonaventure",
    "St. Francis (PA) Red Flash": "Saint Francis",
    "St. John's Red Storm": "St. John's",
    "St. Thomas (MN) Tommies": "St. Thomas",
    "Stanford Cardinal": "Stanford",
    "Stephen F. Austin Lumberjacks": "Stephen F. Austin",
    "Stetson Hatters": "Stetson",
    "Stonehill Skyhawks": "Stonehill",
    "Stony Brook Seawolves": "Stony Brook",
    "Syracuse Orange": "Syracuse",
    "Tarleton State Texans": "Tarleton St.",
    "TCU Horned Frogs": "TCU",
    "Temple Owls": "Temple",
    "Tenn-Martin Skyhawks": "Tennessee Martin",
    "Tennessee St Tigers": "Tennessee St.",
    "Tennessee Tech Golden Eagles": "Tennessee Tech",
    "Tennessee Volunteers": "Tennessee",
    "Texas A&M Aggies": "Texas A&M",
    "Texas A&M-CC Islanders": "Texas A&M Corpus Chris",
    "Texas A&M-Commerce Lions": "Texas A&M Commerce",
    "Texas Longhorns": "Texas",
    "Texas Southern Tigers": "Texas Southern",
    "Texas State Bobcats": "Texas St.",
    "Texas Tech Red Raiders": "Texas Tech",
    "The Citadel Bulldogs": "The Citadel",
    "Toledo Rockets": "Toledo",
    "Towson Tigers": "Towson",
    "Troy Trojans": "Troy",
    "Tulane Green Wave": "Tulane",
    "Tulsa Golden Hurricane": "Tulsa",
    "UAB Blazers": "UAB",
    "UC Davis Aggies": "UC Davis",
    "UC Irvine Anteaters": "UC Irvine",
    "UC Riverside Highlanders": "UC Riverside",
    "UC Santa Barbara Gauchos": "UC Santa Barbara",
    "UC San Diego Tritons": "UC San Diego",
    "UCF Knights": "UCF",
    "UCLA Bruins": "UCLA",
    "UConn Huskies": "Connecticut",
    "UIC Flames": "Illinois Chicago",
    "UL Monroe Warhawks": "Louisiana Monroe",
    "UMass Lowell River Hawks": "UMass Lowell",
    "UMBC Retrievers": "UMBC",
    "UMKC Kangaroos": "UMKC",
    "UNC Asheville Bulldogs": "UNC Asheville",
    "UNC Greensboro Spartans": "UNC Greensboro",
    "UNC Wilmington Seahawks": "UNC Wilmington",
    "UNLV Rebels": "UNLV",
    "USC Trojans": "USC",
    "UT Rio Grande Valley Vaqueros": "UT Rio Grande Valley",
    "UT-Arlington Mavericks": "UT Arlington",
    "Utah State Aggies": "Utah St.",
    "Utah Utes": "Utah",
    "Utah Tech Trailblazers": "Utah Tech",
    "Utah Valley Wolverines": "Utah Valley",
    "UTEP Miners": "UTEP",
    "UTSA Roadrunners": "UTSA",
    "Valparaiso Beacons": "Valparaiso",
    "Vanderbilt Commodores": "Vanderbilt",
    "VCU Rams": "VCU",
    "Vermont Catamounts": "Vermont",
    "Villanova Wildcats": "Villanova",
    "Virginia Cavaliers": "Virginia",
    "Virginia Tech Hokies": "Virginia Tech",
    "VMI Keydets": "VMI",
    "Wagner Seahawks": "Wagner",
    "Wake Forest Demon Deacons": "Wake Forest",
    "Washington Huskies": "Washington",
    "Washington St Cougars": "Washington St.",
    "Weber State Wildcats": "Weber St.",
    "West Virginia Mountaineers": "West Virginia",
    "Western Carolina Catamounts": "Western Carolina",
    "Western Illinois Leathernecks": "Western Illinois",
    "Western Kentucky Hilltoppers": "Western Kentucky",
    "Western Michigan Broncos": "Western Michigan",
    "Wichita St Shockers": "Wichita St.",
    "William & Mary Tribe": "William & Mary",
    "Winthrop Eagles": "Winthrop",
    "Wisconsin Badgers": "Wisconsin",
    "Wofford Terriers": "Wofford",
    "Wright St Raiders": "Wright St.",
    "Wyoming Cowboys": "Wyoming",
    "Xavier Musketeers": "Xavier",
    "Yale Bulldogs": "Yale",
    "Youngstown St Penguins": "Youngstown St.",
}

# Function to be triggered by Cloud Functions
@functions_framework.http
def main(request):
    # Function to retrieve secret
    def get_secret(secret_id):
        project_id = "92962462962"
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    # Retrieve environment variables using Secret Manager
    api_key = get_secret('api_key')
    your_email = get_secret('ken_pom_ID')
    your_password = get_secret('ken_pom_pw')
    function_service_key_str = get_secret('secret_key')

    current_year = datetime.now().year



    # Returns an authenticated browser that can then be used to scrape pages that require authorization.
    browser = login(your_email, your_password)


    def fetch_odds_data(api_key):
        today = datetime.now(pytz.timezone('US/Eastern'))
        start_date = (today + timedelta(days=0)).strftime('%Y-%m-%d')  # start from today
        end_date = (today + timedelta(days=6)).strftime('%Y-%m-%d')    # end after a week

        # API endpoint for Odds API
        url = 'https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds'
        params = {
        'api_key': api_key,
        'regions': 'us',
        'markets': 'h2h,spreads',
        'oddsFormat': 'american',
        'dateFormat': 'iso',
        'startDate': start_date,
        'endDate': end_date
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f'Failed to get odds: status_code {response.status_code}, response body {response.text}')
            return []  # Return an empty list in case of failure

        odds_json = response.json()

        # Print useful information
        print('Number of events:', len(odds_json))
        print('Remaining requests', response.headers.get('x-requests-remaining'))
        print('Used requests', response.headers.get('x-requests-used'))

        return odds_json  # Return the JSON data

    def process_odds_data(odds_json, team_mapping):
        processed_data = []  # Initialize a new list to hold the processed data

        for game in odds_json:
            # Convert commence_time to datetime object
            utc_time = datetime.fromisoformat(game["commence_time"].rstrip('Z'))

            # Convert from UTC to EST
            eastern = pytz.timezone('US/Eastern')
            est_time = utc_time.astimezone(eastern)

            # Use team mapping for Home and Away teams
            home_team_mapped = team_mapping.get(game["home_team"], game["home_team"])
            away_team_mapped = team_mapping.get(game["away_team"], game["away_team"])

            game_info = {
                "Game_Date": est_time.strftime('%Y-%m-%d'),
                "Game_Time": est_time.strftime('%H:%M:%S'),
                "Home_Team": home_team_mapped,
                "Away_Team": away_team_mapped
            }

            for bookmaker in game["bookmakers"]:
                if bookmaker["key"] in ["fanduel", "bovada"]:
                    for market in bookmaker["markets"]:
                        if market["key"] == "h2h":
                            odds = {outcome["name"]: outcome["price"] for outcome in market["outcomes"]}
                            game_info[f"{bookmaker['title']} Home_Odds"] = odds.get(game["home_team"])
                            game_info[f"{bookmaker['title']} Away_Odds"] = odds.get(game["away_team"])
                        elif market["key"] == "spreads":
                            spreads = {outcome["name"]: outcome["point"] for outcome in market["outcomes"]}
                            game_info[f"{bookmaker['title']} Home_Spread"] = spreads.get(game["home_team"])
                            game_info[f"{bookmaker['title']} Away_Spread"] = spreads.get(game["away_team"])

            processed_data.append(game_info)

        # Return statement should be outside of the loop
        return pd.DataFrame(processed_data)

    # Fetch and store odds data in a variable
    odds_json = fetch_odds_data(api_key)
    processed_odds_data = process_odds_data(odds_json, odds_api_team_mapping)  # Pass the fetched data to process

    # Export to CSV
    processed_odds_data.to_csv('ncaam_matches.csv', index=False)

    print(processed_odds_data.head())

    import kenpompy.summary as kp
    import kenpompy.misc as kpm

    # Fetch team stats data using kenpompy
    team_stats_df = kp.get_teamstats(browser)
    team_stats_misc_df = kpm.get_pomeroy_ratings(browser)
    team_stats_four_factors_df = kp.get_fourfactors(browser)

    # Merge the first two DataFrames on the 'Team' column
    combined_df_1 = pd.merge(team_stats_df, team_stats_misc_df, on='Team', how='left')

    # Then, merge the third DataFrame with the result of the first merge
    combined_df_final = pd.merge(combined_df_1, team_stats_four_factors_df, on='Team', how='left')

    # Display the head of the combined DataFrame
    print(combined_df_final.head())

    # Export to CSV
    combined_df_final.to_csv('kenpom_combined_team_stats.csv', index=False)


    # Load the data
    basketball_reference_data = pd.read_csv('kenpom_combined_team_stats.csv')
    odds_api_data = pd.read_csv('ncaam_matches.csv')

    # Use the existing odds_api_team_mapping dictionary for team mapping
    team_mapping = {odds_team: odds_api_team_mapping.get(odds_team) for odds_team in odds_api_data}

    def merge_data(odds_data, stats_data, team_mapping):
        merged_data = odds_data.copy()

        for index, row in merged_data.iterrows():
            # Map team names
            home_team_mapped = team_mapping.get(row['Home_Team'], row['Home_Team'])
            away_team_mapped = team_mapping.get(row['Away_Team'], row['Away_Team'])

            # Check if mapped teams are in stats_data
            if any(stats_data['Team'] == home_team_mapped):
                home_stats = stats_data[stats_data['Team'] == home_team_mapped].iloc[0]
                merged_data.at[index, 'School_Home'] = home_team_mapped
                merged_data.at[index, 'Home_Team_Record'] = home_stats['W-L']
            else:
                merged_data.at[index, 'School_Home'] = 'Unknown'
                merged_data.at[index, 'Home_Team_Record'] = 'N/A'

            if any(stats_data['Team'] == away_team_mapped):
                away_stats = stats_data[stats_data['Team'] == away_team_mapped].iloc[0]
                merged_data.at[index, 'School_Away'] = away_team_mapped
                merged_data.at[index, 'Away_Team_Record'] = away_stats['W-L']
            else:
                merged_data.at[index, 'School_Away'] = 'Unknown'
                merged_data.at[index, 'Away_Team_Record'] = 'N/A'

        return merged_data

    # Function to convert and combine date and time into a single datetime column
    def combine_date_time(row):
        game_date = datetime.strptime(row['Game_Date'], '%Y-%m-%d')
        game_time = datetime.strptime(row['Game_Time'], '%H:%M:%S').time()
        combined_datetime = datetime.combine(game_date, game_time)
        return combined_datetime.strftime('%Y-%m-%d %H:%M:%S')

    # Merge the data
    final_data = merge_data(odds_api_data, basketball_reference_data, team_mapping)

    # Apply the combine_date_time function to each row
    final_data['Date_and_Time'] = final_data.apply(combine_date_time, axis=1)

    # Reordering columns for better scanning
    column_order = ['Date_and_Time', 'Home_Team', 'School_Home', 'Home_Team_Record', 'Away_Team', 'School_Away', 'Away_Team_Record',
                    'FanDuel Home_Odds', 'FanDuel Away_Odds', 'Bovada Home_Odds', 'Bovada Away_Odds', 'FanDuel Home_Spread', 'FanDuel Away_Spread', 'Bovada Home_Spread', 'Bovada Away_Spread']

    # Reorganize the DataFrame using the specified column order
    final_data = final_data[column_order]

    # Save the reorganized data to a CSV file
    final_data.to_csv('ncaam_combined_reordered.csv', index=False)

    print("Data reorganized and saved to 'ncaam_combined_reordered.csv'.")

    # Load the data
    odds_api_data_reordered = pd.read_csv('ncaam_combined_reordered.csv')
    kenpom_data = pd.read_csv('kenpom_combined_team_stats.csv')

    # Function to add statistics to each game row
    def add_team_statistics(row, kenpom_data):

        # Find the team stats
        home_team_stats = kenpom_data[kenpom_data['Team'] == row['School_Home']]
        away_team_stats = kenpom_data[kenpom_data['Team'] == row['School_Away']]

        # Check if statistics for both teams are available
        if not home_team_stats.empty and not away_team_stats.empty:
            home_team_stats = home_team_stats.iloc[0]
            away_team_stats = away_team_stats.iloc[0]

            # Add team statistics to the row
            for stat in kenpom_data.columns.drop('Team'):
                row[f'Home Team {stat}'] = home_team_stats[stat]
                row[f'Away Team {stat}'] = away_team_stats[stat]
        else:
            # If stats for one or both teams are missing, fill with None
            for stat in kenpom_data.columns.drop('Team'):
                row[f'Home Team {stat}'] = None
                row[f'Away Team {stat}'] = None

        return row

    # Apply the function to each row
    enriched_data = odds_api_data_reordered.apply(lambda row: add_team_statistics(row, kenpom_data), axis=1)

    # Save the enriched data to a new CSV file
    enriched_data.to_csv('ncaam_enriched_stats.csv', index=False)
    print("Data enriched and saved to 'ncaam_enriched_stats.csv'.")

    def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    # Check if the current time is close to 5pm EST
    current_time = datetime.now(pytz.timezone('US/Eastern'))
    if current_time.hour == 17:  # 17:00 in 24-hour format is 5pm
        # Timestamped filename for the 5pm export
        timestamped_blob_name = f"ncaam_enriched_stats_{current_time.strftime('%Y%m%d%H')}.csv"
        upload_to_gcs("kenpom-storage", "ncaam_enriched_stats.csv", timestamped_blob_name)

    # Constant filename for the blob for BigQuery
    bigquery_blob_name = "for-bigquery.csv"
    upload_to_gcs("kenpom-storage", "ncaam_enriched_stats.csv", bigquery_blob_name)

    # Function to format column names for BigQuery
    def format_bigquery_column_name(column_name):
        formatted_name = column_name.replace(" ", "_").replace("%", "_").replace(".", "_").replace("-", "_")
        return formatted_name[:300]  # Trim to 300 characters if necessary

    # Convert the string to a dictionary
    function_service_key = json.loads(function_service_key_str)

    # Now use the dictionary to create credentials
    credentials = service_account.Credentials.from_service_account_info(function_service_key)

    # Apply the formatting function to each column name
    enriched_data.columns = [format_bigquery_column_name(col) for col in enriched_data.columns]

    # BigQuery dataset and table
    dataset_id = 'kenpom'
    table_id = 'ncaam_new_table'  # Replace with your desired new table name
    table_full_path = f'{dataset_id}.{table_id}'
    
    try:
        pandas_gbq.to_gbq(enriched_data, destination_table=table_full_path, project_id="betlabs-analysis", credentials=credentials, if_exists='replace')
        print(f"Data uploaded to BigQuery table {table_full_path}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

    # ##### NEW ##### # 
    # Let's remap now home teams and away teams to underdogs and favorites

    def transform_data(input_file, output_file):
        # Read the data from the input file
        data = pd.read_csv(input_file)

        # Apply the formatting function to each column name
        data.columns = [format_bigquery_column_name(col) for col in data.columns]

        # Create new columns for Underdog and Favorite based on FanDuel odds
        data['Underdog'] = data.apply(lambda row: row['Home_Team'] if row['FanDuel_Home_Odds'] > row['FanDuel_Away_Odds'] else row['Away_Team'], axis=1)
        data['Favorite'] = data.apply(lambda row: row['Home_Team'] if row['Underdog'] == row['Away_Team'] else row['Away_Team'], axis=1)

        # Substitute Bovada odds when FanDuel odds are empty
        data['Underdog_Odds'] = data.apply(lambda row: row['Bovada_Home_Odds'] if row['Underdog'] == row['Home_Team'] else row['Bovada_Away_Odds'], axis=1)
        data['Favorite_Odds'] = data.apply(lambda row: row['Bovada_Home_Odds'] if row['Favorite'] == row['Home_Team'] else row['Bovada_Away_Odds'], axis=1)

        # Create a column to indicate whether the underdog is home or away
        data['Underdog_Location'] = data.apply(lambda row: 'Home' if row['Underdog'] == row['Home_Team'] else 'Away', axis=1)

        # Append key statistics columns at the end of the file, mapping to Underdog and Favorite
        relevant_columns = [
            '3P_', '2P_', 'FT_', 'Blk_', 'Stl_', 'A_', '3PA_', 'AdjOE_x', 'Rk', 'AdjEM', 'AdjO', 'AdjD', 'AdjT',
            'Luck', 'SOS_AdjEM', 'SOS_OppO', 'SOS_OppD', 'SOS_AdjEM_Rank', 'Seed', 'AdjTempo', 'AdjOE_y', 'Off_eFG_', 'Off_TO_',
            'Off_OR_', 'Off_FTRate', 'AdjDE', 'Def_eFG_', 'Def_TO_', 'Def_OR_', 'Def_FTRate'
        ]

        for stat in relevant_columns:
            data[f'Underdog_{stat}'] = data.apply(lambda row: row[f'Home_Team_{stat}'] if row['Underdog_Location'] == 'Home' else row[f'Away_Team_{stat}'], axis=1)
            data[f'Favorite_{stat}'] = data.apply(lambda row: row[f'Home_Team_{stat}'] if row['Underdog_Location'] == 'Away' else row[f'Away_Team_{stat}'], axis=1)

        # Drop columns related to Home Team and Away Team
        data = data.drop(columns=['Home_Team', 'Away_Team'])

        # Save the transformed data to the output file
        data.to_csv(output_file, index=False)

    # Specify the input and output file paths
    input_file_path = 'ncaam_enriched_stats.csv'
    output_file_path = 'transformed_data.csv'

    # Call the transform_data function
    transform_data(input_file_path, output_file_path)

    # Now let's perform the wizardry to try getting instances where underdog has an advantage over the favorite in the columns we care about. 
    
    # ##### NEW ##### #
    
    # Reload the data due to reset
    file_path = 'transformed_data.csv'
    data = pd.read_csv(file_path)

    # Define relevant columns and thresholds
    relevant_columns = {
        "Underdog_AdjT": "Favorite_AdjT",
        "Underdog_Off_eFG_": "Favorite_Off_eFG_",
        "Underdog_FT_": "Favorite_FT_",
        "Underdog_2P_": "Favorite_2P_",
        "Underdog_3P_": "Favorite_3P_",
        "Underdog_Off_OR_": "Favorite_Off_OR_",
        "Underdog_Off_TO_": "Favorite_Off_TO_",
        "Underdog_Rk": "Favorite_Rk",
        "Underdog_SOS_AdjEM_Rank": "Favorite_SOS_AdjEM_Rank"
    }

    # Define individual thresholds for each statistic / NOT CURRENTLY BEING USED 
    thresholds = {
        "AdjT": 3,
        "Off_eFG_": 5,
        "FT_": 3,
        "2P_": 3,
        "3P_": 2,
        "Off_OR_": 3,
        "Off_TO_": 1.5,
        "Rk": 30,
        "SOS_AdjEM_Rank": 30,
    }

    # Create a new DataFrame to store the selected rows
    selected_rows = []

    # Loop through each relevant column and apply the revised thresholds/exceeds logic
    for underdog_col, favorite_col in relevant_columns.items():
        threshold = thresholds.get(underdog_col.split('_')[-1], 0)

        # Determine if a lower value is considered "better"
        lower_is_better = underdog_col in ["Underdog_Rk", "Underdog_SOS_AdjEM_Rank", "Underdog_Off_TO_"]

        # THIS DOES NOT WORK SO IGNORE NOW Define the upper and lower bounds for "Thresholds Met"
        # lower_bound = data.apply(lambda row: min(row[favorite_col], row[underdog_col]), axis=1) - threshold
        # upper_bound = data.apply(lambda row: max(row[favorite_col], row[underdog_col]), axis=1) + threshold

        # THIS NEVER WORKED! Check if underdog value is within the range defined by favorite's value ± threshold
        # threshold_met_condition = (
        #    (data[underdog_col] >= lower_bound) & (data[underdog_col] <= upper_bound)
        # ) | (
        #   (data[underdog_col] <= lower_bound) & (data[underdog_col] >= upper_bound)
        # )

        # Check if underdog value exceeds the favorite
        # Inside the loop, modify the exceed_condition assignment:
        exceed_condition = np.where(
            (data[underdog_col] < data[favorite_col]) if lower_is_better else (data[underdog_col] > data[favorite_col]),
            True,  # If condition is True, output True
            False  # Otherwise, output False
        )

        # Append new columns using np.where to convert booleans:
        # DOESNT WORK data[f"{underdog_col}_Met_Threshold"] = threshold_met_condition
        data[f"{underdog_col}_Exceeds_Favorite"] = np.where(exceed_condition, 1, 0)

    # Create columns for totals
    # DOESNT WORK data['Thresholds_Met_Underdog'] = data.filter(like='_Met_Threshold').sum(axis=1)
    data['Exceeds_Favorite_Underdog'] = data.filter(like='_Exceeds_Favorite').sum(axis=1)

    # Weighted values for each stat
    weights_underdog = {
        'Underdog_AdjT_Exceeds_Favorite': 10,
        'Underdog_Off_eFG__Exceeds_Favorite': 15,
        'Underdog_FT__Exceeds_Favorite': 15,
        'Underdog_2P__Exceeds_Favorite': 5,
        'Underdog_3P__Exceeds_Favorite': 15,
        'Underdog_Off_OR__Exceeds_Favorite': 15,
        'Underdog_Off_TO__Exceeds_Favorite': 5,
        'Underdog_Rk_Exceeds_Favorite': 7.5,
        'Underdog_SOS_AdjEM_Rank_Exceeds_Favorite': 7.5
    }

    # Calculate weighted value and add a new column
    weighted_underdog_col = 'weighted_underdog_advantage'
    data[weighted_underdog_col] = data.apply(lambda row: sum(row[stat] * weights_underdog[stat] for stat in weights_underdog.keys()) / 100, axis=1)

    # Save the updated data to a new CSV file
    data.to_csv('all_data_file.csv', index=False)

    # Load the data from the CSV file
    file_path = 'all_data_file.csv'
    data = pd.read_csv(file_path)

    # Identify columns starting with "Home_Team" and "Away_Team"
    columns_to_remove = [col for col in data.columns if col.startswith(('Home_Team', 'Away_Team'))]

    # Drop the identified columns
    data = data.drop(columns=columns_to_remove)

    # Save the updated data to a new CSV file
    data.to_csv('underdog_advantages.csv', index=False)
    print("finished creating underdog_advantages.csv'.")
    
    # Now we will store this in the same bucket, but a different table in BigQuery

    # Reload the data 
    file_path = 'underdog_advantages.csv'
    data = pd.read_csv(file_path)

    def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    # Check if the current time is close to 5pm EST
    current_time = datetime.now(pytz.timezone('US/Eastern'))
    if current_time.hour == 17:  # 17:00 in 24-hour format is 5pm
        # Timestamped filename for the 5pm export
        timestamped_blob_name = f"underdog_advantages_{current_time.strftime('%Y%m%d%H')}.csv"
        upload_to_gcs("kenpom-storage", "underdog_advantages.csv", timestamped_blob_name)

    # Constant filename for the blob for BigQuery
    bigquery_blob_name = "advantages-for-bigquery.csv"
    upload_to_gcs("kenpom-storage", "underdog_advantages.csv", bigquery_blob_name)

    # Function to format column names for BigQuery
    def format_bigquery_column_name(column_name):
        formatted_name = column_name.replace(" ", "_").replace("%", "_").replace(".", "_").replace("-", "_")
        return formatted_name[:300]  # Trim to 300 characters if necessary

    # Convert the string to a dictionary
    function_service_key = json.loads(function_service_key_str)

    # Now use the dictionary to create credentials
    credentials = service_account.Credentials.from_service_account_info(function_service_key)

    # Apply the formatting function to each column name
    data.columns = [format_bigquery_column_name(col) for col in data.columns]

    # BigQuery dataset and table
    dataset_id = 'kenpom_ud_advantages'
    table_id = 'ncaam_ud_advantages'  # Replace with your desired new table name
    table_full_path = f'{dataset_id}.{table_id}'
    
    try:
        pandas_gbq.to_gbq(data, destination_table=table_full_path, project_id="betlabs-analysis", credentials=credentials, if_exists='replace')
        print(f"Data uploaded to BigQuery table {table_full_path}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")


    # Return statement should be at the end of the main function
    return 'Process completed successfully'
