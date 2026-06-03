import os
import json
import hashlib
import re
import random
from datetime import datetime, timezone

# ---------- Utility Functions ----------

def normalize_text(text: str) -> str:
    """Normalize text for hashing and comparison."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return " ".join(text.split())

def md5_hash(text: str) -> str:
    """Generate MD5 hash of normalized text."""
    return hashlib.md5(normalize_text(text).encode()).hexdigest()

def jaccard_similarity(text1: str, text2: str) -> float:
    """Compute Jaccard similarity between two texts."""
    set1, set2 = set(normalize_text(text1).split()), set(normalize_text(text2).split())
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)

# ---------- Data Normalization ----------

def normalize_article(article):
    """Preserve all original keys and ensure required fields exist."""
    normalized = dict(article)  # copy everything

    normalized.setdefault("title", "")
    normalized.setdefault("description", article.get("summary", ""))
    normalized.setdefault("source", article.get("source", "") or article.get("publisher", ""))
    normalized.setdefault("published_at", article.get("published", ""))
    if "fetched_at" not in normalized or not normalized["fetched_at"]:
        normalized["fetched_at"] = article.get("created_at", "")

    return normalized

# ---------- Keywords ----------

KEYWORDS_HIGH = {
    "war","crisis","scandal","ban","protest","violence","terror","attack",
    "modi","bjp","congress","election","supreme court","parliament","policy",
    "inflation","recession","merger","acquisition","default","collapse",
    "isro","satellite","rocket","launch","breakthrough","discovery",
    "world cup","ipl","record","victory","defeat"
}

KEYWORDS_MED = {
    "million","billion","rupee","gdp","sensex","nifty","rbi","startup","unicorn",
    "investment","funding","crypto","stock","gold","market","oil",
    "ai","quantum","drone","5g","cyber","innovation",
    "covid","vaccine","pollution","flood","drought","cyclone","earthquake",
    "china","pakistan","usa","trade","summit","sanction","alliance"
}

BIG_SOURCES = ["reuters","bbc","times of india","cnn","the hindu","ndtv","indian express","hindustan times"]

# ---------- Scoring ----------

def calculate_score(article, entry):
    score = 0
    source = article.get("source", "").lower()
    score += 20 if any(big in source for big in BIG_SOURCES) else 10

    text = normalize_text(article.get("title","") + " " + article.get("description",""))
    if any(k in text for k in KEYWORDS_HIGH):
        score += 20
    if any(k in text for k in KEYWORDS_MED):
        score += 10

    pub_time = article.get("published_at")
    if pub_time:
        try:
            pub_dt = datetime.strptime(pub_time.split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            diff_hours = (now - pub_dt).total_seconds() / 3600
            if diff_hours < 3:
                score += 30
            elif diff_hours < 12:
                score += 15
            elif diff_hours > 24:
                score -= 10
        except:
            pass

    num_sources = len(entry["sources"])
    if num_sources > 1:
        score += min((num_sources - 1) * 15, 50)

    if entry["first_seen"] and entry["last_seen"]:
        try:
            fmt = "%Y-%m-%dT%H:%M:%S"
            first_dt = datetime.strptime(entry["first_seen"].split(".")[0], fmt)
            last_dt = datetime.strptime(entry["last_seen"].split(".")[0], fmt)
            diff_min = (last_dt - first_dt).total_seconds() / 60
            if diff_min >= 30:
                score += 10
        except:
            pass

    return min(100, int(score / 140 * 100))

def classify_hotness(score):
    if score >= 70:
        return "Hot"
    elif score >= 40:
        return "Medium"
    else:
        return "Low"

# ---------- Main Processing ----------

# ---------- Main Processing ----------

def get_hash(article: dict) -> str:
    base = f"{article.get('url','')}_{article.get('source','')}_{article.get('published_at','')}_{article.get('title','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def process_news_file(articles, jaccard_threshold: float = 0.80):
    """
    Takes list/dict of articles, returns (updated_articles, news_map) in memory.
    """
    news_map = {}
    updated_articles = []

    # Handle dict of categories vs flat list
    if isinstance(articles, dict):
        categories = articles.items()
    else:
        categories = [("general", articles)]

    for category, items in categories:
        for idx, raw_article in enumerate(items):
            article = normalize_article(raw_article)
            article_id = get_hash(article)
            article["article_id"] = article_id

            title, desc = article["title"], article["description"]
            core_text = f"{title} {desc}"
            strict_id = md5_hash(core_text)

            matched_id = None
            for nid, entry in news_map.items():
                if strict_id == entry["md5"] or jaccard_similarity(core_text, entry["text"]) >= jaccard_threshold:
                    matched_id = nid
                    break

            if matched_id:  # duplicate
                entry = news_map[matched_id]
                entry["sources"].add(article.get("source", ""))
                entry["article_ids"].append(article_id)
                last_seen = article.get("fetched_at")
                if last_seen and last_seen > entry["last_seen"]:
                    entry["last_seen"] = last_seen
                score = calculate_score(article, entry)
            else:  # new
                news_map[strict_id] = {
                    "md5": strict_id,
                    "text": core_text,
                    "sources": {article.get("source", "")},
                    "article_ids": [article_id],
                    "first_seen": article.get("fetched_at", ""),
                    "last_seen": article.get("fetched_at", ""),
                }
                entry = news_map[strict_id]
                score = calculate_score(article, entry)

            article["score"] = score
            article["hotness"] = classify_hotness(score)
            article["popularity_score"] = random.randint(1, 10)

            updated_articles.append(article)

    # Convert sets → lists in news_map
    news_map_clean = {
        k: {
            **v,
            "sources": list(v["sources"]),
            "count": len(v["article_ids"])
        }
        for k, v in news_map.items()
    }

    return updated_articles, news_map_clean
