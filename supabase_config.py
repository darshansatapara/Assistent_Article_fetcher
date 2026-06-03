import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Load .env ---
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# --- Supabase Client Initialization ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# --- Table Names ---
NEWS_TABLE = "news_articles"

# --- Save Articles to Supabase ---
def save_articles_to_supabase(articles: list):
    """
    Insert or update articles in Supabase news table
    Returns: dict with inserted and updated count
    """
    inserted, updated = 0, 0
    
    for article in articles:
        published_at = article.get("published_at")

        doc = {
            "article_id": article.get("article_id"),
            "title": article.get("title"),
            "description": article.get("description"),
            "author": article.get("author"),
            "source": article.get("source"),
            "url": article.get("url"),
            "image_url": article.get("image_url"),
            "category": article.get("category"),
            "tags": article.get("tags", []),
            "published_at": published_at,
            "score": article.get("score"),
            "hotness": article.get("hotness"),
            "impact_score": article.get("impact_score"),
            "popularity_score": article.get("popularity_score", 0),
            "views_count": 0,
            "likes_count": 0,
            "ai_generations_count": 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            #one column is not here comic_image which is null by default, when we need, will add it.
        }

        try:
            # Check if article exists
            response = supabase.table(NEWS_TABLE).select("article_id").eq(
                "article_id", doc["article_id"]
            ).execute()

            if response.data:
                # Update existing article
                supabase.table(NEWS_TABLE).update(doc).eq(
                    "article_id", doc["article_id"]
                ).execute()
                updated += 1
            else:
                # Insert new article
                doc["created_at"] = datetime.now(timezone.utc).isoformat()
                supabase.table(NEWS_TABLE).insert(doc).execute()
                inserted += 1

        except Exception as e:
            print(f"❌ Error saving article {doc['article_id']}: {e}")

    print(f"✅ Supabase Articles Saved — Inserted: {inserted}, Updated: {updated}")
    return {"inserted": inserted, "updated": updated}
