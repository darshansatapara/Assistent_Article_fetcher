import os
import json
from datetime import datetime, timezone

# Import your existing fetchers
from gnews_fetching import  collect_news  # your GNews script
from rss_feed_outof_india import fetch_rss_news     # your RSS script


def combine_news(gnews_data, rss_data):
    """
    Combine GNews structured data + RSS flat data into a unified flat list.
    Preserves all keys from original articles.
    """
    combined = []

    # Flatten GNews data (dict of categories)
    if isinstance(gnews_data, dict):
        for category, articles in gnews_data.items():
            for article in articles:
                # Ensure category field exists
                article.setdefault("category", category)
                combined.append(article)

    # Add RSS data (already flat list)
    if isinstance(rss_data, list):
        for article in rss_data:
            combined.append(article)

    return combined


# if __name__ == "__main__":
#     # Step 1: Fetch from GNews
#     print("📡 Fetching GNews data...")
#     gnews_data = collect_news()

#     # Step 2: Fetch from RSS
#     print("📡 Fetching RSS data...")
#     rss_data, rss_logs = fetch_rss_news()

#     # Step 3: Combine
#     print("🔄 Combining sources...")
#     combined_data = combine_news(gnews_data, rss_data)

#     # Step 4: Save combined (optional for testing; remove in production DB pipeline)
#     output_dir = "combined"
#     os.makedirs(output_dir, exist_ok=True)
#     filename = f"{output_dir}/combined_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(combined_data, f, ensure_ascii=False, indent=4)

#     print(f"✅ Combined {len(combined_data)} articles into {filename}")
