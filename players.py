import requests
import pandas as pd
import pyodbc
import re

# Database connection
try:
    conn = pyodbc.connect(r"DRIVER={SQL Server};SERVER=;DATABASE=;UID=;PWD=;")
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
                                INSERT (country_id, country_name, country_short, country_flag)
                                VALUES (source.country_id, source.country_name, source.country_short, source.country_flag);
                        """, country_id, country_name, country_short, country_flag)
                        if cursor.rowcount > 0:
                            inserted_counts["countries_master"] += 1

                        # Insert Team if Not Exists
                        cursor.execute("""
                            MERGE INTO teams_master AS target
                            USING (SELECT ? AS team_id, ? AS team, ? AS country_id, ? AS short_name) AS source
                            ON target.team_id = source.team_id
                            WHEN NOT MATCHED THEN 
                                INSERT (team_id, team, country_id, short_name)
                                VALUES (source.team_id, source.team, source.country_id, source.short_name);
                        """, team_id, team_name, country_id, country_short)
                        if cursor.rowcount > 0:
                            inserted_counts["teams_master"] += 1

                        # Insert Player if Not Exists
                        cursor.execute("""
                            MERGE INTO players_master AS target
                            USING (SELECT ? AS player_id, ? AS player, ? AS country_id, ? AS team_id, ? AS player_url, ? AS player_image) AS source
                            ON target.player_id = source.player_id
                            WHEN NOT MATCHED THEN 
                                INSERT (player_id, player, country_id, team_id, player_url, player_image)
                                VALUES (source.player_id, source.player, source.country_id, source.team_id, source.player_url, source.player_image);
                        """, player_id, player_name, country_id, team_id, player_url, player_image)
                        if cursor.rowcount > 0:
                            inserted_counts["players_master"] += 1

                        # Insert Player Rankings
                        cursor.execute("""
                            INSERT INTO player_rankings (player_id, format, category, pos, change, rating, career_best_rating, rank_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, player_id, comp_type.upper(), category.lower(), pos, change, rating, career_best_rating, rank_date)
                        if cursor.rowcount > 0:
                            inserted_counts["player_rankings"] += 1

                    except pyodbc.Error as e:
                        print("SQL Error:", e)
                        continue  # Skip this player and move to the next

# Commit and Close Connection
conn.commit()
cursor.close()
conn.close()

# Print Summary of Inserted Records
print("\nRecords Inserted:")
for table, count in inserted_counts.items():
    print(f"{table}: {count} records inserted.")
print("\nData inserted successfully!\n")