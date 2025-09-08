# import requests
# import json
# import math
# import random
# import os
# from datetime import datetime, timezone
# from dotenv import load_dotenv

# load_dotenv()

# API_KEY = os.getenv("GNEWS_API_KEY")
# OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
# # Categories available in GNews
# CATEGORIES = ["business", "technology", "politics", "entertainment",
#               "sports", "science", "health", "nature", "education", "general"]

# # Country split
# PRIMARY_COUNTRY = "in"
# GLOBAL_COUNTRIES = ["us", "jp"]

# # 80% ratio
# IND_PER_REQUEST_RATIO = 0.8

# # Free plan limits
# DAILY_MAX_REQUESTS = 10   # total requests per run
# ARTICLES_PER_REQUEST = 10

# TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# # Tracking logs
# LOG_FILE = "gnews_log.txt"
# request_log = []
# stats = {
#     "total_requests": 0,
#     "total_articles": 0,
#     "categories": {cat: {} for cat in CATEGORIES}
# }


# def log_request(category, country, articles_count, params):
#     stats["total_requests"] += 1
#     stats["total_articles"] += articles_count
#     if country not in stats["categories"][category]:
#         stats["categories"][category][country] = {
#             "requests": 0,
#             "articles": 0
#         }
#     stats["categories"][category][country]["requests"] += 1
#     stats["categories"][category][country]["articles"] += articles_count

#     entry = f"[{datetime.now()}] Category={category}, Country={country}, " \
#             f"Articles={articles_count}, Params={params}"
#     request_log.append(entry)


# def log_error(message):
#     entry = f"[ERROR {datetime.now()}] {message}"
#     request_log.append(entry)


# def fetch_news(endpoint, params, category, country):
#     try:
#         response = requests.get(endpoint, params=params, timeout=10)
#         data = response.json()

#         # Handle API errors (like quota reached)
#         if response.status_code != 200 or "articles" not in data:
#             log_error(f"API Error for {category} ({country}): {data}")
#             return []

#         articles = data.get("articles", [])
#         log_request(category, country or "global", len(articles), params)

#         news_items = []
#         for entry in articles:
#             description = entry.get("description")
#             source = entry.get("source", {}).get("name") if entry.get("source") else None
#             image_url = entry.get("image")

#             news_item = {
#                 "title": entry.get("title") or None,
#                 "description": description or None,
#                 "author": entry.get("author") or None,
#                 "source": source or None,
#                 "url": entry.get("url") or None,
#                 "image_url": image_url or None,
#                 "category": category,
#                 "tags": [category.capitalize()],
#                 "impact_score": None,
#                 "popularity_score": None,
#                 "published_at": entry.get("publishedAt") or None,
#                 "fetched_at": datetime.now().isoformat()
#             }
#             news_items.append(news_item)

#         return news_items

#     except Exception as e:
#         log_error(f"{endpoint} params={params} => {e}")
#         return []


# def fetch_category_news(category, country=None, is_top=False):
#     endpoint = "https://gnews.io/api/v4/top-headlines" if is_top else "https://gnews.io/api/v4/search"
#     params = {
#         "token": API_KEY,
#         "lang": "en",
#         "max": ARTICLES_PER_REQUEST
#     }
#     if is_top:
#         params["category"] = category
#         if country:
#             params["country"] = country
#     else:
#         params["q"] = category
#         params["from"] = TODAY
#         params["to"] = TODAY
#         if country:
#             params["country"] = country
#     return fetch_news(endpoint, params, category, country)


# def collect_news():
#     all_news = {cat: [] for cat in CATEGORIES}

#     # Split requests into 80% India, 20% Global
#     india_requests = math.floor(DAILY_MAX_REQUESTS * IND_PER_REQUEST_RATIO)
#     global_requests = DAILY_MAX_REQUESTS - india_requests

#     # India requests round-robin categories
#     for i in range(india_requests):
#         cat = CATEGORIES[i % len(CATEGORIES)]
#         all_news[cat].extend(fetch_category_news(cat, PRIMARY_COUNTRY, is_top=False))

#     # Global requests round-robin categories + countries
#     for i in range(global_requests):
#         cat = CATEGORIES[i % len(CATEGORIES)]
#         country = GLOBAL_COUNTRIES[i % len(GLOBAL_COUNTRIES)]
#         all_news[cat].extend(fetch_category_news(cat, country, is_top=False))

#     return all_news


# def save_logs():
#     with open(LOG_FILE, "a", encoding="utf-8") as f:
#         f.write("\n" + "="*60 + "\n")
#         f.write(f"Run at {datetime.now()}\n")
#         f.write(f"Total Requests: {stats['total_requests']}\n")
#         f.write(f"Total Articles: {stats['total_articles']}\n\n")

#         for cat, countries in stats["categories"].items():
#             f.write(f"Category: {cat}\n")
#             for country, data in countries.items():
#                 f.write(f"  {country} -> Requests={data['requests']}, Articles={data['articles']}\n")
#             f.write("\n")

#         f.write("Detailed Requests:\n")
#         for entry in request_log:
#             f.write(entry + "\n")
#         f.write("="*60 + "\n")


# if __name__ == "__main__":
#     data = collect_news()
#     filename = f"{OUTPUT_DIR}/gnews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4, ensure_ascii=False)

#     save_logs()
#     print(f"✅ Saved {filename} with category-wise news")
#     print(f"📄 Log written to {LOG_FILE}")



import requests
import math
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

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

    for i in range(global_requests):
        cat = CATEGORIES[i % len(CATEGORIES)]
        country = GLOBAL_COUNTRIES[i % len(GLOBAL_COUNTRIES)]
        all_news[cat].extend(fetch_category_news(cat, country, is_top=False, logs=logs))

    return all_news, logs


if __name__ == "__main__":
    news_data, logs = collect_news()
    print(f"✅ Collected GNews articles: {sum(len(v) for v in news_data.values())}")
    print(f"📝 Logs: {len(logs)}")
