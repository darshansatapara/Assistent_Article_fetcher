# Supabase Integration Setup Guide

## Overview
The project now saves articles and newsmap data to both MongoDB and Supabase. This provides redundancy and flexibility in data storage.

## Setup Steps

### 1. Environment Variables
Add the following to your `.env` file:

```
# MongoDB
MONGO_URI=your_mongodb_connection_string

# Supabase
SUPABASE_URL=your_supabase_project_url
SUPABASE_API_KEY=your_supabase_api_key
```

### 2. Supabase Table Schema
Create the following tables in your Supabase project:

#### Table: `news`
```sql
CREATE TABLE news (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  article_id TEXT NOT NULL UNIQUE,
  title TEXT,
  description TEXT,
  author TEXT,
  source TEXT,
  url TEXT,
  image_url TEXT,
  category TEXT,
  tags JSONB DEFAULT '[]'::jsonb,
  published_at TIMESTAMP,
  fetched_at TIMESTAMP,
  date TEXT,
  score FLOAT,
  hotness FLOAT,
  impact_score FLOAT,
  popularity_score FLOAT DEFAULT 0,
  views_count INT DEFAULT 0,
  likes_count INT DEFAULT 0,
  ai_generations_count INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_news_article_id ON news(article_id);
CREATE INDEX idx_news_date ON news(date);
CREATE INDEX idx_news_category ON news(category);
CREATE INDEX idx_news_published_at ON news(published_at DESC);
```

#### Table: `newsmap`
```sql
CREATE TABLE newsmap (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  md5 TEXT NOT NULL UNIQUE,
  text TEXT,
  sources JSONB DEFAULT '[]'::jsonb,
  article_ids JSONB DEFAULT '[]'::jsonb,
  first_seen TIMESTAMP,
  last_seen TIMESTAMP,
  count INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_newsmap_md5 ON newsmap(md5);
```

### 3. Install Dependencies
```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # macOS/Linux

# Install requirements
pip install -r requirements.txt
```

### 4. Run the Pipeline
```bash
python save_to_mongo.py
```

## Files Added/Modified

### New Files
- `supabase_config.py` - Supabase configuration and save functions

### Modified Files
- `save_to_mongo.py` - Added Supabase import and save calls
- `requirements.txt` - Added `supabase` package

## Functions Available

### From `supabase_config.py`

**`save_articles_to_supabase(articles: list) -> dict`**
- Saves or updates articles in Supabase
- Returns: `{"inserted": int, "updated": int}`

**`save_newsmap_to_supabase(map_data: dict) -> dict`**
- Saves or updates newsmap entries in Supabase
- Returns: `{"inserted": int, "updated": int}`

## How It Works

1. Fetches news from GNews and RSS feeds
2. Combines and processes articles
3. Saves to **MongoDB** (original behavior)
4. Saves to **Supabase** (new behavior)
5. Logs are saved to MongoDB

Both MongoDB and Supabase will now contain the same article data, providing data redundancy and backup.
