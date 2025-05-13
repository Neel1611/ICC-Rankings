import requests
import pandas as pd
import pyodbc
import re
from datetime import datetime, date, timedelta

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

# Inserted counts
inserted_counts = {
    "countries_master": 0,
    "teams_master": 0,
    "players_master": 0,
    "player_rankings": 0
}

# Dates
today_date = date.today().strftime('%Y-%m-%d')
#yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Helper function
def safe_int(value, default=0):
    try:
        return int(value) if value is not None and str(value).isdigit() else default
    except ValueError:
        return default

# Process Player Rankings
for comp_type in formats:
    for category in categories:
        url = f"{base_url}?client_id={client_id}&comp_type={comp_type}&feed_format=json&lang=en&type={category}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json().get("data", {}).get("bat-rank", {}).get("rank", [])

            if data:
                # Fix "=" in ranking
                actual_rank = 0
                for player in data:
                    no = player.get("no", "").strip()
                    if no == "=":
                        player["no"] = str(actual_rank)
                    else:
                        actual_rank = safe_int(no)

                for player in data:
                    try:
                        player_id = safe_int(player.get("Player_id"))
                        player_name = player.get("Player-name")
                        country_id = safe_int(player.get("Country_id"))
                        country_name = player.get("Country_name")
                        country_short = player.get("Country")
                        team_id = safe_int(player.get("team_id"))
                        team_name = player.get("team_name")
                        rating = safe_int(player.get("Points"))
                        career_best_rating = player.get("careerbest")
                        rank_date = player.get("rankdate")
                        player_url = player.get("Player_url")
                        country_flag = f"https://assets-icc.sportz.io/static-assets/buildv3-stg/images/teams/{country_id}.png?v=8"
                        player_image = f"https://images.icc-cricket.com/image/upload/t_player-headshot-portrait-lg/prd/assets/players/generic/colored/{player_id}.png"
                        pos = safe_int(player.get("no"))

                        change_raw = player.get("change", "( - )")
                        match = re.search(r"([-+]?\d+)", change_raw)
                        change = match.group(0) if match else "-"

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

                        cursor.execute("""
                            INSERT INTO player_rankings (player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, player_id, comp_type.upper(), category.lower(), pos, change, rating, career_best_rating, rank_date, today_date)
                        if cursor.rowcount > 0:
                            inserted_counts["player_rankings"] += 1

                    except pyodbc.Error as e:
                        print("SQL Error:", e)
                        continue

# Insert Logs
for table, count in inserted_counts.items():
    cursor.execute("""
        INSERT INTO transactions (table_name, records, type, entry_date) 
        VALUES (?, ?, 'INSERT', ?)
    """, table, count, today_date)

# ------------------ TEAM RANKINGS ------------------

inserted_counts = {
    "countries_master": 0,
    "teams_master": 0,
    "team_rankings": 0
}

def clean_change2(change_value):
    match = re.search(r"([-+]?\d+)", change_value)
    return match.group(0) if match else "-"

for comp_type in formats:
    url = f"{base_url}?client_id={client_id}&comp_type={comp_type}&feed_format=json&lang=en&type=team"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json().get("data", {}).get("bat-rank", {}).get("rank", [])

        if data:
            # Fix "=" ranks
            actual_rank = 0
            for team in data:
                no = team.get("no", "").strip()
                if no == "=":
                    team["no"] = str(actual_rank)
                else:
                    actual_rank = safe_int(no)

            for team in data:
                country_id = safe_int(team.get("Country_id"))
                country_name = team.get("Country")
                short_name = team.get("shortname")
                team_id = safe_int(team.get("team_id"))
                team_name = team.get("team_name")
                pos = safe_int(team.get("no"))
                change = clean_change2(team.get("change", "( - )"))
                matches = safe_int(team.get("Matches"))
                points = safe_int(team.get("Points"))
                rating = safe_int(team.get("Rating"))
                rank_date = team.get("rankdate")
                country_flag = f"https://images.icc-cricket.com/image/upload/t_team-logo/v1/{country_id}.png"

                if not country_id or not country_name or not short_name:
                    continue

                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM countries_master WHERE country_id = ?)
                    BEGIN
                        INSERT INTO countries_master (country_id, country_name, country_short, country_flag, entry_date)
                        VALUES (?, ?, ?, ?, ?)
                    END
                """, country_id, country_id, country_name, short_name, country_flag, today_date)
                if cursor.rowcount > 0:
                    inserted_counts["countries_master"] += 1

                if not team_id or not team_name:
                    continue

                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM teams_master WHERE team_id = ?)
                    BEGIN
                        INSERT INTO teams_master (team_id, team, country_id, short_name, entry_date)
                        VALUES (?, ?, ?, ?, ?)
                    END
                """, team_id, team_id, team_name, country_id, short_name, today_date)
                if cursor.rowcount > 0:
                    inserted_counts["teams_master"] += 1

                cursor.execute("""
                    INSERT INTO team_rankings (team_id, format, pos, change, matches, points, rating, rank_date, entry_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, team_id, comp_type.upper(), pos, change, matches, points, rating, rank_date, today_date)
                if cursor.rowcount > 0:
                    inserted_counts["team_rankings"] += 1

# Insert Logs
for table, count in inserted_counts.items():
    cursor.execute("""
        INSERT INTO transactions (table_name, records, type, entry_date) 
        VALUES (?, ?, 'INSERT', ?)
    """, table, count, today_date)

# ------------------ MOVE OLD RECORDS ------------------

moved_counts = {
    "team_rankings_old": 0,
    "player_rankings_old": 0,
    "team_rankings": 0,
    "player_rankings": 0
}

cursor.execute("""
    INSERT INTO team_rankings_old (team_id, format, pos, change, matches, points, rating, rank_date, entry_date)
    SELECT team_id, format, pos, change, matches, points, rating, rank_date, entry_date 
    FROM team_rankings 
    WHERE entry_date < ?
""", today_date)
moved_counts["team_rankings_old"] = cursor.rowcount

cursor.execute("""
    DELETE FROM team_rankings WHERE entry_date < ?
""", today_date)
moved_counts["team_rankings"] = cursor.rowcount

cursor.execute("""
    INSERT INTO player_rankings_old (player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date)
    SELECT player_id, format, category, pos, change, rating, career_best_rating, rank_date, entry_date
    FROM player_rankings 
    WHERE entry_date < ?
""", today_date)
moved_counts["player_rankings_old"] = cursor.rowcount

cursor.execute("""
    DELETE FROM player_rankings WHERE entry_date < ?
""", today_date)
moved_counts["player_rankings"] = cursor.rowcount

for table, count in moved_counts.items():
    action_type = "INSERT" if "old" in table else "DELETE"
    cursor.execute("""
        INSERT INTO transactions (table_name, records, type, entry_date) 
        VALUES (?, ?, ?, ?)
    """, table, count, action_type, today_date)

# Summary
# print("\n✅ Summary of inserted records:")
# for table, count in inserted_counts.items():
#     print(f"{table}: {count} records inserted.")

# print("\n✅ Summary of moved/deleted records:")
# for table, count in moved_counts.items():
#     print(f"{table}: {count} records {'inserted' if 'old' in table else 'deleted'}.")

# print("\n📌 Transaction logs stored in the `transactions` table.\n")
print("Data Inserted.....!\n")

# Commit and close
conn.commit()
cursor.close()
conn.close()
