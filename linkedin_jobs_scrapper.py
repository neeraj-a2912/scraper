import os
import pandas as pd
from datetime import datetime, timedelta
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters, ExperienceLevelFilters

# ===== CONFIG =====
ROLES = [
    "Data Engineer",
    "Software Engineer",
    "Data Scientist",
    "AI Engineer",
    "ML Engineer"
]
LOCATION = "India"
BASE_DIR = "data"

today = datetime.today()
year_folder = os.path.join(BASE_DIR, str(today.year))
month_folder = os.path.join(year_folder, f"{today.month:02d}")
os.makedirs(month_folder, exist_ok=True)

file_name = f"{today.strftime('%d')}.csv"
file_path = os.path.join(month_folder, file_name)

# ===== SCRAPER =====
scraper = LinkedinScraper(
    headless=True,           # run in background on GitHub Actions
    max_workers=1,           # sequential scraping
    slow_mo=2,               # 2-second delay between actions
    page_load_timeout=60     # wait max 60s for page to load
)

all_jobs = []

# ===== EVENT HANDLERS =====
def on_data(data: EventData):
    city = data.place.split(",")[0].strip() if data.place else None
    all_jobs.append({
        "title": data.title,
        "company": data.company,
        "location": data.place,
        "city": city,
        "date": data.date,
        "link": data.link,
        "description": data.description
    })

def on_error(error):
    print(f"Error: {error}")

def on_end():
    # Remove duplicates from yesterday
    yesterday = today - timedelta(days=1)
    yesterday_file = os.path.join(
        year_folder if yesterday.year == today.year else os.path.join(BASE_DIR, str(yesterday.year)),
        f"{yesterday.month:02d}",
        f"{yesterday.strftime('%d')}.csv"
    )

    df_today = pd.DataFrame(all_jobs)
    if os.path.exists(yesterday_file):
        df_yesterday = pd.read_csv(yesterday_file)
        df_today = df_today[~df_today["link"].isin(df_yesterday["link"])]

    df_today.to_csv(file_path, index=False)
    print(f"Saved {len(df_today)} new jobs to {file_path}")

# Attach events
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

# ===== QUERIES =====
queries = []
for role in ROLES:
    queries.append(
        Query(
            query=role,
            options=QueryOptions(
                locations=[LOCATION],
                filters=QueryFilters(
                    experience=[
                        ExperienceLevelFilters.ENTRY_LEVEL,
                        ExperienceLevelFilters.ASSOCIATE
                    ]
                ),
                limit=50
            )
        )
    )

# Run scraper
scraper.run(queries)
