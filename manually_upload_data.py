import os
import json
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient

# --- Load .env ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "newsdb"

# --- Mongo Connection ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
news_col = db["news"]

# --- Utility: Generate Unique Hash for Article ---
def get_hash(article: dict) -> str:
    base = f"{article.get('url','')}_{article.get('source','')}_{article.get('published_at','')}_{article.get('title','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

# --- Save Articles ---
def save_articles(articles: list):
    inserted, updated = 0, 0
    for article in articles:
        hash_id = get_hash(article)

        published_at = article.get("published_at")
        fetched_at = article.get("fetched_at")

        date_str = published_at.split("T")[0] if published_at else None

        doc = {
            "articleId": hash_id,
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
            # preserve engagement metrics
            doc["viewsCount"] = existing.get("viewsCount", 0)
            doc["likesCount"] = existing.get("likesCount", 0)
            doc["aiGenerationsCount"] = existing.get("aiGenerationsCount", 0)

            news_col.update_one({"articleId": doc["articleId"]}, {"$set": doc})
            updated += 1
        else:
            doc["createdAt"] = datetime.now(timezone.utc)
            news_col.insert_one(doc)
            inserted += 1

    print(f"✅ Finished: Inserted {inserted}, Updated {updated}")

# --- Main ---
if __name__ == "__main__":
    # load your combined JSON (from file)
    with open("updated_data/updated_combined_news_20250901_202429.json", "r", encoding="utf-8") as f:
        combined_data = json.load(f)

    # process each article list (combined_data is a dict)
    all_articles = []
    for source, articles in combined_data.items():
        all_articles.extend(articles)

    print(f"📊 Total articles from combined file: {len(all_articles)}")

    save_articles(all_articles)
