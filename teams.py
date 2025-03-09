import requests
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

# Formats
formats = ["test", "odi", "t20", "odiw", "t20w"]

inserted_counts = {
    "countries_master": 0,
    "teams_master": 0,
    "team_rankings": 0
}

# Helper function to safely convert values
def safe_int(value, default=0):
    """Convert to int safely, return default if conversion fails."""
    try:
        return int(value) if value is not None and str(value).isdigit() else default
    except ValueError:
        return default

# Function to clean the "change" field
def clean_change(change_value):
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
                country_id = safe_int(team.get("Country_id"))
                country_name = team.get("Country")
                short_name = team.get("shortname")
                team_id = safe_int(team.get("team_id"))
                team_name = team.get("team_name")
                pos = safe_int(team.get("no"))
                change_raw = team.get("change", "( - )")  # Default to "( - )"
                change = clean_change(change_raw)  # Removes parentheses
                matches = safe_int(team.get("Matches"))
                points = safe_int(team.get("Points"))
                rating = safe_int(team.get("Rating"))
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
                        INSERT INTO countries_master (country_id, country_name, country_short, country_flag)
                        VALUES (?, ?, ?, ?)
                    END
                """, country_id, country_id, country_name, short_name, country_flag)
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
                        INSERT INTO teams_master (team_id, team, country_id, short_name)
                        VALUES (?, ?, ?, ?)
                    END
                """, team_id, team_id, team_name, country_id, short_name)
                if cursor.rowcount > 0:
                            inserted_counts["teams_master"] += 1

                # Insert Team Rankings
                cursor.execute("""
                    INSERT INTO team_rankings (team_id, format, pos, change, matches, points, rating, rank_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, team_id, comp_type.upper(), pos, change, matches, points, rating, rank_date)
                if cursor.rowcount > 0:
                            inserted_counts["team_rankings"] += 1

# Commit and Close Connection
conn.commit()
cursor.close()
conn.close()

# Print Summary of Inserted Records
print("\nRecords Inserted:")
for table, count in inserted_counts.items():
    print(f"{table}: {count} records inserted.")
print("\nData inserted successfully!\n")