import requests
import math
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import time
# --- Load env ---
load_dotenv()
API_KEY = os.getenv("GNEWS_API_KEY")

# Categories available in GNews
CATEGORIES = ["business", "technology", "politics", "entertainment",
              "sports", "science", "health", "nature", "education", "general"]

PRIMARY_COUNTRY = "in"
GLOBAL_COUNTRIES = ["us", "jp"]

IND_PER_REQUEST_RATIO = 0.8
DAILY_MAX_REQUESTS = 10
ARTICLES_PER_REQUEST = 10

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_news(endpoint, params, category, country, logs):
    try:
        response = requests.get(endpoint, params=params, timeout=10)
        data = response.json()
        if response.status_code != 200 or "articles" not in data:
            logs.append({
                "source": "gnews",
                "category": category,
                "country": country,
                "articles_count": 0,
                "params": params,
                "error": str(data),
                "timestamp": datetime.now(timezone.utc)
            })
            return []

        articles = data.get("articles", [])
        logs.append({
            "source": "gnews",
            "category": category,
            "country": country or "global",
            "articles_count": len(articles),
            "params": params,
            "error": None,
            "timestamp": datetime.now(timezone.utc)
        })

        news_items = []
        for entry in articles:
            description = entry.get("description")
            source = entry.get("source", {}).get("name") if entry.get("source") else None
            image_url = entry.get("image")

            news_item = {
                "title": entry.get("title") or None,
                "description": description or None,
                "author": entry.get("author"),
                "source": source,
                "url": entry.get("url"),
                "image_url": image_url,
                "category": category,
                "tags": [category.capitalize()],
                "impact_score": None,
                "popularity_score": None,
                "published_at": entry.get("publishedAt"),
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
            news_items.append(news_item)

        return news_items

    except Exception as e:
        logs.append({
            "source": "gnews",
            "category": category,
            "country": country,
            "articles_count": 0,
            "params": params,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        })
        return []


def fetch_category_news(category, country=None, is_top=False, logs=None):
    endpoint = "https://gnews.io/api/v4/top-headlines" if is_top else "https://gnews.io/api/v4/search"
    params = {
        "token": API_KEY,
        "lang": "en",
        "max": ARTICLES_PER_REQUEST
    }
    if is_top:
        params["category"] = category
        if country:
            params["country"] = country
    else:
        params["q"] = category
        params["from"] = TODAY
        params["to"] = TODAY
        if country:
            params["country"] = country
    return fetch_news(endpoint, params, category, country, logs)


def collect_news():
    all_news = {cat: [] for cat in CATEGORIES}
    logs = []

    india_requests = math.floor(DAILY_MAX_REQUESTS * IND_PER_REQUEST_RATIO)
    global_requests = DAILY_MAX_REQUESTS - india_requests

    for i in range(india_requests):
        cat = CATEGORIES[i % len(CATEGORIES)]
        all_news[cat].extend(fetch_category_news(cat, PRIMARY_COUNTRY, is_top=False, logs=logs))
        time.sleep(1) 

    for i in range(global_requests):
        cat = CATEGORIES[i % len(CATEGORIES)]
        country = GLOBAL_COUNTRIES[i % len(GLOBAL_COUNTRIES)]
        all_news[cat].extend(fetch_category_news(cat, country, is_top=False, logs=logs))
        time.sleep(1)

    return all_news, logs


if __name__ == "__main__":
    news_data, logs = collect_news()
    print(f"✅ Collected GNews articles: {sum(len(v) for v in news_data.values())}")
    print(f"📝 Logs: {len(logs)}")
