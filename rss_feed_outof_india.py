

import feedparser
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

# --- Load env ---
load_dotenv()

RSS_FEEDS = {
    "entertainment": [
        "https://variety.com/feed/",
        "https://www.hollywoodreporter.com/feed/",
        "https://www.hindustantimes.com/feeds/rss/entertainment/bollywood/rssfeed.xml",
        "https://indianexpress.com/section/entertainment/bollywood/feed/"
    ],
    "sports": [
        "https://www.espn.com/espn/rss/news",
        "https://www.sbnation.com/rss/index.xml",
        "https://www.hindustantimes.com/feeds/rss/sports/rssfeed.xml",
        "https://indianexpress.com/section/sports/feed/"
    ],
    "politics": [
        "https://feeds.washingtonpost.com/rss/politics",
        "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "https://indianexpress.com/section/politics/feed/"
    ],
    "science": [
        "https://www.sciencedaily.com/rss/top/science.xml",
        "https://www.nature.com/nature.rss",
        "https://www.hindustantimes.com/feeds/rss/tech/science/rssfeed.xml"
    ],
    "space": [
        "https://www.nasa.gov/rss/dyn/breaking_news.rss",
        "https://www.sciencedaily.com/rss/top/science.xml",
        "https://www.space.com/feeds/all"
    ],
    "technology": [
        "https://www.theverge.com/rss/index.xml",
        "https://techcrunch.com/feed/",
        "https://www.wired.com/feed/rss",
        "https://feeds.feedburner.com/gadgets360-latest"
    ],
    "startups": [
        "https://techcrunch.com/startups/feed/",
        "https://yourstory.com/feed"
    ],
    "general": [
        "https://www.bbc.com/news/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
        "https://feeds.washingtonpost.com/rss/world",
        "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "https://indianexpress.com/feed/"
    ]
}


def fetch_rss_news():
    news_list = []
    rss_logs = []

    for category, feeds in RSS_FEEDS.items():
        for feed_url in feeds:
            feed_log = {
                "source": "rss",
                "category": category,
                "url": feed_url,
                "articles_count": 0,
                "error": None,
                "timestamp": datetime.now(timezone.utc)
            }
            try:
                feed = feedparser.parse(feed_url)
                source = feed.feed.get("title", "Unknown Source")
                count = 0

                for entry in feed.entries[:10]:
                    title = entry.get("title")
                    description = entry.get("summary", "")
                    author = entry.get("author", "Unknown")
                    url = entry.get("link", "")
                    image_url = None

                    if "media_content" in entry:
                        image_url = entry.media_content[0].get("url", None)
                    elif "media_thumbnail" in entry:
                        image_url = entry.media_thumbnail[0].get("url", None)

                    published_at = None
                    if "published_parsed" in entry:
                        published_at = datetime(*entry.published_parsed[:6]).isoformat()

                    tags = [category.capitalize(), "Breaking"]

                    news_item = {
                        "title": title,
                        "description": description,
                        "author": author,
                        "source": source,
                        "url": url,
                        "image_url": image_url,
                        "category": category,
                        "tags": tags,
                        "popularity_score": None,
                        "published_at": published_at,
                        "fetched_at": datetime.now(timezone.utc).isoformat()
                    }

                    news_list.append(news_item)
                    count += 1

                feed_log["articles_count"] = count

            except Exception as e:
                feed_log["error"] = str(e)

            rss_logs.append(feed_log)

    return news_list, rss_logs


if __name__ == "__main__":
    news_data, logs = fetch_rss_news()
    print(f"✅ Collected {len(news_data)} RSS news items")
    print(f"📝 Logs: {len(logs)}")
