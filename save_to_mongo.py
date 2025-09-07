import os
import json
import hashlib
from datetime import datetime, timezone
import time
from dotenv import load_dotenv
from pymongo import MongoClient

# Import your fetchers and processors
from gnews_fetching import collect_news   # returns (articles, logs)
from rss_feed_outof_india import fetch_rss_news  # returns (articles, logs)
from filter_update_news import process_news_file  # your scoring/deduplication
from combine_stage import combine_news  # your combine module

# --- Load .env ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "newsdb"

# --- Mongo Connection ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
news_col = db["news"]
newsmap_col = db["newsmap"]
gnews_logs_col = db["gnews_logs"]
rss_logs_col = db["rss_logs"]

# --- Utility: Generate Unique Hash for Article ---
# def get_hash(article: dict) -> str:
#     base = f"{article.get('url','')}_{article.get('source','')}_{article.get('published_at','')}_{article.get('title','')}"
#     return hashlib.md5(base.encode("utf-8")).hexdigest()


# --- Save Articles ---
def save_articles(articles: list):
    inserted, updated = 0, 0
    for article in articles:
        # hash_id = get_hash(article)

        published_at = article.get("published_at")
        fetched_at = article.get("fetched_at")

        date_str = published_at.split("T")[0] if published_at else None

        doc = {
            "articleId": article.get("article_id") ,
            "title": article.get("title"),
            "description": article.get("description"),
            "author": article.get("author"),
            "source": article.get("source"),
            "url": article.get("url"),
            "imageUrl": article.get("image_url"),
            "category": article.get("category"),
            "tags": article.get("tags", []),

            "publishedAt": datetime.fromisoformat(published_at.replace("Z", "+00:00")) if published_at else None,
            "fetchedAt": datetime.fromisoformat(fetched_at.replace("Z", "+00:00")) if fetched_at else datetime.now(timezone.utc),
            "date": date_str,

            "score": article.get("score"),
            "hotness": article.get("hotness"),
            "impactScore": article.get("impact_score"),
            "popularityScore": article.get("popularity_score", 0),

            "viewsCount": 0,
            "likesCount": 0,
            "aiGenerationsCount": 0,

            "updatedAt": datetime.now(timezone.utc)
        }

        existing = news_col.find_one({"articleId": doc["articleId"]})
        if existing:
            doc["viewsCount"] = existing.get("viewsCount", 0)
            doc["likesCount"] = existing.get("likesCount", 0)
            doc["aiGenerationsCount"] = existing.get("aiGenerationsCount", 0)

            news_col.update_one({"articleId": doc["articleId"]}, {"$set": doc})
            updated += 1
        else:
            doc["createdAt"] = datetime.now(timezone.utc)
            news_col.insert_one(doc)
            inserted += 1

    print(f"✅ News Saved — Inserted: {inserted}, Updated: {updated}")

# --- Save NewsMap ---
def save_newsmap(map_data: dict):
    inserted, updated = 0, 0
    for md5_key, entry in map_data.items():
        doc = {
            "md5": entry.get("md5", md5_key),
            "text": entry.get("text"),
            "sources": entry.get("sources", []),
            "articleIds": entry.get("article_ids", []),
            "firstSeen": datetime.fromisoformat(entry["first_seen"].replace("Z", "+00:00")) if entry.get("first_seen") else None,
            "lastSeen": datetime.fromisoformat(entry["last_seen"].replace("Z", "+00:00")) if entry.get("last_seen") else None,
            "count": entry.get("count", 0),
            "updatedAt": datetime.now(timezone.utc)
        }

        existing = newsmap_col.find_one({"md5": doc["md5"]})
        if existing:
            newsmap_col.update_one({"md5": doc["md5"]}, {"$set": doc})
            updated += 1
        else:
            doc["createdAt"] = datetime.now(timezone.utc)
            newsmap_col.insert_one(doc)
            inserted += 1

    print(f"✅ NewsMap Saved — Inserted: {inserted}, Updated: {updated}")

# --- Debug Utility: Save JSON to file ---
def dump_to_file(data, filename):
    os.makedirs("debug_output", exist_ok=True)
    path = os.path.join("debug_output", filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"📂 Dumped {filename} → {path}")

# --- Save Logs ---
def save_logs(logs: list, log_collection):
    if not logs:
        return
    log_collection.insert_many(logs)


# --- Indexes ---
def create_indexes():
    news_col.create_index("articleId", unique=True)
    news_col.create_index("date")
    news_col.create_index("category")
    news_col.create_index([("tags", 1)])
    news_col.create_index([("publishedAt", -1)])


# --- Main Pipeline ---
if __name__ == "__main__":
    print("📡 Fetching GNews...")
    gnews_data, gnews_logs = collect_news()
    # dump_to_file(gnews_data, "01_gnews_data.json")

    print("📡 Fetching RSS...")
    for attempt in range(3):
        rss_data, rss_logs = fetch_rss_news()
        if rss_data:
            break
        print(f"⚠️ RSS attempt {attempt+1} failed, retrying...")
        time.sleep(5)
    # dump_to_file(rss_data, "02_rss_data.json")

    print("🔄 Combining...")
    combined_data = combine_news(gnews_data, rss_data)
    # dump_to_file(combined_data, "03_combined.json")

    print("⚡ Processing...")
    updated_articles, news_map = process_news_file(combined_data)
    # dump_to_file(updated_articles, "04_processed_articles.json")
    # dump_to_file(news_map, "05_newsmap.json")

    print("💾 Saving Articles...")
    stats = save_articles(updated_articles)
    # dump_to_file(stats, "06_save_stats.json")

    print("📝 Saving news_map...")
    save_newsmap(news_map)
  
  
    print("📝 Saving Logs...")
    save_logs(gnews_logs, gnews_logs_col)
    save_logs(rss_logs, rss_logs_col)

    print("🎯 Pipeline completed successfully!")