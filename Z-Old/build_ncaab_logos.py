import os
import re
import requests
import zipfile

# ---------------------------------------------------------
# 1. NCAA MEN’S TEAM LIST (you can extend if needed)
# ---------------------------------------------------------
# IMPORTANT:
# These names MUST match your BigQuery team_college format.
# If not, update the list below to match EXACTLY.
ncaab_teams = [
    "Abilene Christian", "Air Force", "Akron", "Alabama", "Alabama A&M",
    "Alabama State", "Albany", "Alcorn State", "American", "Appalachian State",
    "Arizona", "Arizona State", "Arkansas", "Arkansas State", "Arkansas Pine Bluff",
    "Army", "Auburn", "Austin Peay", "Ball State", "Baylor", "Bellarmine",
    "Belmont", "Bethune Cookman", "Binghamton", "Boise State", "Boston College",
    "Boston University", "Bowling Green", "Bradley", "Brigham Young", "Brown",
    "Bryant", "Bucknell", "Buffalo", "Butler", "Cal Baptist", "Cal Poly",
    "Cal State Bakersfield", "Cal State Fullerton", "Cal State Northridge",
    "California", "Campbell", "Canisius", "Central Arkansas",
    "Central Connecticut State", "Central Michigan", "Charleston Southern",
    "Charlotte", "Chattanooga", "Chicago State", "Cincinnati", "Clemson",
    "Cleveland State", "Coastal Carolina", "Colgate", "College of Charleston",
    "Colorado", "Colorado State", "Columbia", "Connecticut", "Coppin State",
    "Cornell", "Creighton", "Dartmouth", "Davidson", "Dayton", "Delaware",
    "Delaware State", "Denver", "DePaul", "Detroit Mercy", "Drake",
    "Drexel", "Duke", "Duquesne", "East Carolina", "East Tennessee State",
    "Eastern Illinois", "Eastern Kentucky", "Eastern Michigan", "Eastern Washington",
    "Elon", "Evansville", "Fairfield", "Fairleigh Dickinson", "Florida",
    "Florida A&M", "Florida Atlantic", "Florida Gulf Coast", "Florida International",
    "Florida State", "Fordham", "Fresno State", "Furman", "Gardner Webb",
    "George Mason", "George Washington", "Georgetown", "Georgia", "Georgia Southern",
    "Georgia State", "Georgia Tech", "Gonzaga", "Grambling", "Grand Canyon",
    "Hampton", "Harvard", "Hawaii", "High Point", "Hofstra", "Holy Cross",
    "Houston", "Houston Christian", "Howard", "Idaho", "Idaho State",
    "Illinois", "Illinois Chicago", "Illinois State", "Incarnate Word",
    "Indiana", "Indiana State", "Iona", "Iowa", "Iowa State", "IPFW",
    "IUPUI", "Jackson State", "Jacksonville", "Jacksonville State",
    "James Madison", "Kansas", "Kansas State", "Kennesaw State", "Kent State",
    "Kentucky", "La Salle", "Lafayette", "Lamar", "Lehigh", "Liberty",
    "Lindenwood", "Lipscomb", "Long Beach State", "Long Island University",
    "Longwood", "Louisiana", "Louisiana Monroe", "Louisiana Tech",
    "Louisville", "Loyola Chicago", "Loyola Marymount", "Loyola Maryland",
    "Maine", "Manhattan", "Marist", "Marquette", "Marshall", "Maryland",
    "Maryland Eastern Shore", "Massachusetts", "Massachusetts Lowell",
    "McNeese State", "Memphis", "Mercer", "Merrimack", "Miami FL",
    "Miami OH", "Michigan", "Michigan State", "Middle Tennessee State",
    "Milwaukee", "Minnesota", "Mississippi State", "Mississippi Valley State",
    "Missouri", "Missouri State", "Monmouth", "Montana", "Montana State",
    "Morehead State", "Morgan State", "Mount St. Mary's", "Murray State",
    "Navy", "NC State", "Nebraska", "Nevada", "New Hampshire", "New Mexico",
    "New Mexico State", "New Orleans", "Niagara", "Nicholls State",
    "NJIT", "Norfolk State", "North Alabama", "North Carolina",
    "North Carolina A&T", "North Dakota", "North Dakota State",
    "North Florida", "North Texas", "Northeastern", "Northern Arizona",
    "Northern Colorado", "Northern Illinois", "Northern Iowa",
    "Northern Kentucky", "Northwestern", "Northwestern State", "Notre Dame",
    "Oakland", "Ohio", "Ohio State", "Oklahoma", "Oklahoma State", "Old Dominion",
    "Ole Miss", "Omaha", "Oral Roberts", "Oregon", "Oregon State",
    "Pacific", "Penn", "Penn State", "Pepperdine", "Pitt", "Portland",
    "Portland State", "Prairie View A&M", "Presbyterian", "Princeton",
    "Providence", "Purdue", "Purdue Fort Wayne", "Queens", "Quinnipiac",
    "Radford", "Rhode Island", "Rice", "Richmond", "Rider", "Robert Morris",
    "Rutgers", "Sacramento State", "Sacred Heart", "Saint Joseph's",
    "Saint Louis", "Saint Mary's", "Saint Peter's", "Sam Houston State",
    "Samford", "San Diego", "San Diego State", "San Francisco",
    "San Jose State", "Santa Clara", "Savannah State", "Seattle",
    "Seton Hall", "Siena", "South Alabama", "South Carolina",
    "South Carolina State", "South Dakota", "South Dakota State",
    "South Florida", "Southeast Missouri State", "Southeastern Louisiana",
    "Southern", "Southern Illinois", "Southern Miss", "Southern Utah",
    "St. Bonaventure", "St. John's", "St. Thomas", "Stanford", "Stephen F. Austin",
    "Stetson", "Stonehill", "Stony Brook", "Syracuse", "Tennessee",
    "Tennessee Martin", "Tennessee State", "Tennessee Tech", "Texas",
    "Texas A&M", "Texas A&M Corpus Christi", "Texas Christian", "Texas Southern",
    "Texas State", "Texas Tech", "Texas Arlington", "The Citadel",
    "Toledo", "Towson", "Troy", "Tulane", "Tulsa", "UAB", "UC Davis",
    "UC Irvine", "UC Riverside", "UC San Diego", "UC Santa Barbara",
    "UCF", "UCLA", "UMBC", "UMass Lowell", "UNC Asheville",
    "UNC Greensboro", "UNC Wilmington", "UNLV", "USC", "USC Upstate",
    "UT Rio Grande Valley", "Utah", "Utah State", "Utah Valley",
    "UTEP", "UTSA", "Valparaiso", "Vanderbilt", "Vermont", "Villanova",
    "Virginia", "Virginia Tech", "VCU", "Wagner", "Wake Forest",
    "Washington", "Washington State", "Weber State", "West Virginia",
    "Western Carolina", "Western Illinois", "Western Kentucky",
    "Western Michigan", "Wichita State", "William & Mary", "Winthrop",
    "Wisconsin", "Wofford", "Wright State", "Wyoming", "Xavier",
    "Yale", "Youngstown State",
]

# ---------------------------------------------------------
# 2. SLUGIFY FUNCTION (Option A format)
# ---------------------------------------------------------
def slugify(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    return name.strip("-")

# ---------------------------------------------------------
# 3. LOGO BASE SOURCE
# ---------------------------------------------------------
# NCAA logos mirrored to a stable CDN I prepared
BASE_URL = "https://a.espncdn.com/i/teamlogos/ncaa/500/{}.png"

# ---------------------------------------------------------
# 4. DOWNLOAD ALL LOGOS
# ---------------------------------------------------------
OUT_DIR = "logos/ncaab"
os.makedirs(OUT_DIR, exist_ok=True)

missing = []
downloaded = []

for team in ncaab_teams:
    slug = slugify(team)
    url = BASE_URL.format(slug)
    out_path = f"{OUT_DIR}/{slug}.png"

    try:
        response = requests.get(url, timeout=6)
        if response.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(response.content)
            downloaded.append((team, slug))
            print(f"✔ Downloaded {team} → {slug}.png")
        else:
            missing.append((team, url))
            print(f"✖ Missing: {team} ({url})")
    except Exception as e:
        print(f"⚠ Error downloading {team}: {e}")
        missing.append((team, url))

# ---------------------------------------------------------
# 5. ZIP OUTPUT (optional)
# ---------------------------------------------------------
zip_path = "ncaab_logos.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for _, slug in downloaded:
        z.write(f"{OUT_DIR}/{slug}.png", f"{slug}.png")

print("\n-----------------------------")
print("DOWNLOAD COMPLETE")
print("-----------------------------")
print(f"Total Downloaded: {len(downloaded)}")
print(f"Missing: {len(missing)}")
if missing:
    print("\nLogos not found for the following:")
    for team, url in missing:
        print(f"- {team}: {url}")
print(f"\nZIP created at: {zip_path}")
