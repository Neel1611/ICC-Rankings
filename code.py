import requests
import pandas as pd
import pyodbc
import re
from datetime import datetime
from datetime import date, datetime, timedelta, timezone

# Database connection
try:
    conn = pyodbc.connect(r"DRIVER={SQL Server};SERVER=LAPTOP-KVGOC1PQ\SQLEXPRESS;DATABASE=;UID=;PWD=;")
    cursor = conn.cursor()
    print("\nConnected to database successfully!")
except pyodbc.Error as e:
    print("\nError connecting to database:", e)
    exit()

# Base URL
base_url = "https://assets-icc.sportz.io/cricket/v1/ranking"
client_id = "tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&"

# Formats and Categories
formats = ["test", "odi", "t20", "odiw", "t20w"]
categories = ["bat", "bowl", "allrounder"]

# Track Record Counts
inserted_counts = {
    "countries_master": 0,
    "teams_master": 0,
    "players_master": 0,
    "player_rankings": 0
}

#today_date = datetime.today().strftime('%Y-%m-%d')
#yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Convert to string after calculation
today_date = date.today().strftime('%Y-%m-%d')
yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Helper function to safely convert values
def safe_int(value, default=0):
    """Convert to int safely, return default if conversion fails."""
    try:
        return int(value) if value is not None and str(value).isdigit() else default
    except ValueError:
        return default

# Fetch and process rankings
for comp_type in formats:
    for category in categories:
        url = f"{base_url}?client_id={client_id}&comp_type={comp_type}&feed_format=json&lang=en&type={category}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json().get("data", {}).get("bat-rank", {}).get("rank", [])

            if data:
                for player in data:
                    try:
                        # Extract Player Data
                        player_id = safe_int(player.get("Player_id"))
                        player_name = player.get("Player-name")
                        country_id = safe_int(player.get("Country_id"))
                        country_name = player.get("Country_name")
                        country_short = player.get("Country")
                        team_id = safe_int(player.get("team_id"))
                        team_name = player.get("team_name")
                        rating = safe_int(player.get("Points"))
                        career_best_rating = player.get("careerbest")  # Keep as string
                        rank_date = player.get("rankdate")
                        player_url = player.get("Player_url")

                        # Generate Image URLs
                        country_flag = f"https://images.icc-cricket.com/image/upload/t_team-logo/v1/{country_id}.png"
                        player_image = f"https://images.icc-cricket.com/image/upload/t_player-headshot-portrait-lg/prd/assets/players/generic/colored/{player_id}.png"

                        # Extract Position & Change
                        pos = safe_int(player.get("no"))

                        # Extract Numeric Change Value
                        change_raw = player.get("change", "( - )")  # Default to "( - )"
                        match = re.search(r"([-+]?\d+)", change_raw)
                        change = match.group(0) if match else "-"

                        # Insert Country if Not Exists
                        cursor.execute("""
                            MERGE INTO countries_master AS target
                            USING (SELECT ? AS country_id, ? AS country_name, ? AS country_short, ? AS country_flag) AS source
                            ON target.country_id = source.country_id
                            WHEN NOT MATCHED THEN 
                                INSERT (country_id, country_name, country_short, country_flag, entry_date)
                                VALUES (source.country_id, source.country_name, source.country_short, source.country_flag, ?);
                        """, country_id, country_name, country_short, country_flag, today_date)
                        if cursor.rowcount > 0:
                            inserted_counts["countries_master"] += 1

                        # Insert Team if Not Exists
                        cursor.execute("""
                            MERGE INTO teams_master AS target
                            USING (SELECT ? AS team_id, ? AS team, ? AS country_id, ? AS short_name) AS source
                            ON target.team_id = source.team_id
                            WHEN NOT MATCHED THEN 
                                INSERT (team_id, team, country_id, short_name, entry_date)
                                VALUES (source.team_id, source.team, source.country_id, source.short_name, ?);
                        """, team_id, team_name, country_id, country_short, today_date)
                        if cursor.rowcount > 0:
                            inserted_counts["teams_master"] += 1

                        # Insert Player if Not Exists
                        cursor.execute("""
                            MERGE INTO players_master AS target
                            USING (SELECT ? AS player_id, ? AS player, ? AS country_id, ? AS team_id, ? AS player_url, ? AS player_image) AS source
                            ON target.player_id = source.player_id
                            WHEN NOT MATCHED THEN 
                                INSERT (player_id, player, country_id, team_id, player_url, player_image, entry_date)
                                VALUES (source.player_id, source.player, source.country_id, source.team_id, source.player_url, source.player_image, ?);
                        """, player_id, player_name, country_id, team_id, player_url, player_image, today_date)
                        if cursor.rowcount > 0:
                            inserted_counts["players_master"] += 1

                        # Insert Player Rankings
                        cursor.execute("""
                            INSERT INTO player_rankings (player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, player_id, comp_type.upper(), category.lower(), pos, change, rating, career_best_rating, rank_date, today_date)
                        if cursor.rowcount > 0:
                            inserted_counts["player_rankings"] += 1
                            
                        

                    except pyodbc.Error as e:
                        print("SQL Error:", e)
                        continue  # Skip this player and move to the next

# ✅ Insert Logs into Transactions Table
for table, count in inserted_counts.items():
        cursor.execute("""
            INSERT INTO transactions (table_name, records, type, entry_date) 
            VALUES (?, ?, 'INSERT', ?)
        """, table, count, today_date)

# Print Summary of Inserted Records
print("\nRecords Inserted:")
for table, count in inserted_counts.items():
    print(f"{table}: {count} records inserted.")


#---------------------------------------------------------------------------------------------------------------
# Formats
formats = ["test", "odi", "t20", "odiw", "t20w"]

inserted_counts = {
    "countries_master": 0,
    "teams_master": 0,
    "team_rankings": 0
}

# Helper function to safely convert values
def safe_int2(value, default=0):
    """Convert to int safely, return default if conversion fails."""
    try:
        return int(value) if value is not None and str(value).isdigit() else default
    except ValueError:
        return default

# Function to clean the "change" field
def clean_change2(change_value):
    """Extracts numeric values from change field and removes parentheses."""
    match = re.search(r"([-+]?\d+)", change_value)
    return match.group(0) if match else "-"

# Fetch and process rankings
for comp_type in formats:
    url = f"{base_url}?client_id={client_id}&comp_type={comp_type}&feed_format=json&lang=en&type=team"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json().get("data", {}).get("bat-rank", {}).get("rank", [])

        if data:
            for team in data:
                # Extract and Validate Data
                country_id = safe_int2(team.get("Country_id"))
                country_name = team.get("Country")
                short_name = team.get("shortname")
                team_id = safe_int2(team.get("team_id"))
                team_name = team.get("team_name")
                pos = safe_int2(team.get("no"))
                change_raw = team.get("change", "( - )")  # Default to "( - )"
                change = clean_change2(change_raw)  # Removes parentheses
                matches = safe_int2(team.get("Matches"))
                points = safe_int2(team.get("Points"))
                rating = safe_int2(team.get("Rating"))
                rank_date = team.get("rankdate")
                country_flag = f"https://images.icc-cricket.com/image/upload/t_team-logo/v1/{country_id}.png"

                # Ensure required fields are not None
                if not country_id or not country_name or not short_name:
                    print(f"Skipping team due to missing data: {team_name}")
                    continue  # Skip this iteration if essential data is missing

                # Insert Country if Not Exists
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM countries_master WHERE country_id = ?)
                    BEGIN
                        INSERT INTO countries_master (country_id, country_name, country_short, country_flag, entry_date)
                        VALUES (?, ?, ?, ?, ?)
                    END
                """, country_id, country_id, country_name, short_name, country_flag, today_date)
                if cursor.rowcount > 0:
                            inserted_counts["countries_master"] += 1

                # Ensure required fields for team are not None
                if not team_id or not team_name:
                    print(f"Skipping ranking entry due to missing team data: {team_id}")
                    continue  # Skip this iteration if essential data is missing

                # Insert Team if Not Exists
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM teams_master WHERE team_id = ?)
                    BEGIN
                        INSERT INTO teams_master (team_id, team, country_id, short_name, entry_date)
                        VALUES (?, ?, ?, ?, ?)
                    END
                """, team_id, team_id, team_name, country_id, short_name, today_date)
                if cursor.rowcount > 0:
                            inserted_counts["teams_master"] += 1

                # Insert Team Rankings
                cursor.execute("""
                    INSERT INTO team_rankings (team_id, format, pos, change, matches, points, rating, rank_date, entry_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, team_id, comp_type.upper(), pos, change, matches, points, rating, rank_date, today_date)
                if cursor.rowcount > 0:
                            inserted_counts["team_rankings"] += 1

# ✅ Insert Logs into Transactions Table
for table, count in inserted_counts.items():
        cursor.execute("""
            INSERT INTO transactions (table_name, records, type, entry_date) 
            VALUES (?, ?, 'INSERT', ?)
        """, table, count, today_date)

from datetime import datetime, timedelta

#------------------------------------------------------------------------------------------------------------------------------------

# Calculate yesterday's date
yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Track record counts
moved_counts = {
    "team_rankings_old": 0,
    "player_rankings_old": 0,
    "team_rankings": 0,
    "player_rankings": 0
}

# Move Data from team_rankings to team_rankings_old
cursor.execute(f"""
    INSERT INTO team_rankings_old (team_id, format, pos, change, matches, points, rating, rank_date, entry_date)
    SELECT team_id, format, pos, change, matches, points, rating, rank_date, entry_date 
    FROM team_rankings 
    WHERE entry_date < ?
""", today_date)
moved_counts["team_rankings_old"] = cursor.rowcount

# Delete Moved Records from team_rankings
cursor.execute(f"""
    DELETE FROM team_rankings WHERE entry_date < ?
""", today_date)
moved_counts["team_rankings"] = cursor.rowcount

# Move Data from player_rankings to player_rankings_old
cursor.execute(f"""
    INSERT INTO player_rankings_old (player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date)
    SELECT player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date
    FROM player_rankings 
    WHERE entry_date < ?
""", today_date)
moved_counts["player_rankings_old"] = cursor.rowcount

# Delete Moved Records from player_rankings
cursor.execute(f"""
    DELETE FROM player_rankings WHERE entry_date < ?
""", today_date)
moved_counts["player_rankings"] = cursor.rowcount

# ✅ Insert Logs into Transactions Table
for table, count in moved_counts.items():
    action_type = "INSERT" if "old" in table else "DELETE"
    cursor.execute("""
        INSERT INTO transactions (table_name, records, type, entry_date) 
        VALUES (?, ?, ?, ?)
    """, table, count, action_type, yesterday_date)

# Print Summary of Inserted Records
for table, count in inserted_counts.items():
    print(f"{table}: {count} records inserted.")

# Print Summary
for table, count in moved_counts.items():
    if table == 'player_rankings'  or table == 'team_rankings':
        print(f"{table}: {count} old records deleted.")
    else:
        print(f"{table}: {count} old records inserted.")
print("\n📌 Data moved or inserted or deleted successfully:")
print("\n📌 Transaction logs stored in the `transactions` table.\n")
# Commit and Close Connection
conn.commit()
cursor.close()
conn.close()
