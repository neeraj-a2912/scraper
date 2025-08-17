import os
import csv
import glob
from datetime import datetime
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.events import Events

# ==============================
# CONFIG
# ==============================
ROLES = [
    "Data Engineer",
    "Software Engineer",
    "Data Scientist",
    "AI Engineer",
    "ML Engineer"
]
LOCATION = "India"
RESULTS_PER_ROLE = 200  # Number of jobs per role

# ==============================
# SCRAPER SETUP
# ==============================
results = []

def on_data(data):
    results.append(data)

def on_error(error):
    print("[Error]", error)

def on_end():
    print("[End] Finished scraping")

scraper = LinkedinScraper(
    headless=True,      # run browser in headless mode
    max_workers=1,      # number of concurrent browser tabs
    slow_mo=0.5,        # slow down scraping (avoid blocks)
    page_load_timeout=60
)

scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

# ==============================
# QUERIES
# ==============================
queries = []
for role in ROLES:
    queries.append(Query(
        query=role,
        options=QueryOptions(
            locations=[LOCATION],
            filters=QueryFilters(),
            limit=RESULTS_PER_ROLE
        )
    ))

scraper.run(queries)

# ==============================
# DEDUPLICATION (MONTH-BY-MONTH)
# ==============================
now = datetime.now()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

month_folder = os.path.join("data", year, month)
os.makedirs(month_folder, exist_ok=True)

# Collect all previously seen job links this month
seen_links = set()
for file in glob.glob(os.path.join(month_folder, "*.csv")):
    with open(file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            seen_links.add(row["link"])

# Filter out duplicates
unique_results = [job for job in results if job.link not in seen_links]

# ==============================
# SAVE TODAY'S FILE
# ==============================
file_path = os.path.join(month_folder, f"{day}.csv")

with open(file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["title", "company", "location", "date", "link"])
    writer.writeheader()
    for job in unique_results:
        writer.writerow({
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "date": job.date,
            "link": job.link
        })

print(f"✅ Saved {len(unique_results)} new jobs to {file_path}")
