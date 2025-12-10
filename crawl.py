# ==================== CRAWLER FINAL V8 ====================
# Hỗ trợ lấy ảnh theo ưu tiên:
# 1. media:content
# 2. enclosure
# 3. <img src> trong description
# ===========================================================

import feedparser
import logging
import time
import requests
import re
import html
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from pybloom_live import ScalableBloomFilter
import atexit
from typing import Dict, List, Optional
import pickle

# ==================== CONFIG ====================
API_BASE = "http://62.146.237.219:8188/api/v1"
LOGIN_URL = f"{API_BASE}/auth/login"
SOURCES_URL = f"{API_BASE}/sources/all-with-topics"

USERNAME = "admin"
PASSWORD = "News@2025"

POLL_INTERVAL = 60
MAX_WORKERS = 8
BLOOM_FILE = "multi_source_seen.bloom"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# ==================== BLOOM FILTER ====================
try:
    with open(BLOOM_FILE, "rb") as f:
        bloom = pickle.load(f)
    logging.info(f"Đã load Bloom Filter: ~{len(bloom)} items")
except:
    bloom = ScalableBloomFilter(initial_capacity=20_000_000, error_rate=0.001)
    logging.info("Tạo Bloom Filter mới")

def save_bloom():
    try:
        with open(BLOOM_FILE, "wb") as f:
            pickle.dump(bloom, f)
        logging.info("Đã lưu Bloom Filter")
    except Exception as e:
        logging.error(f"Lưu bloom lỗi: {e}")

atexit.register(save_bloom)

# ==================== DATABASE ====================
try:
    conn = psycopg2.connect(
    host="172.17.0.1",
    database="news_db",
    user="postgres",
    password="postgres",
    port=54123
)

    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id BIGSERIAL PRIMARY KEY,
        title VARCHAR(1024),
        link VARCHAR(1024) UNIQUE,
        guid VARCHAR(1024),
        description TEXT,
        pub_date BIGINT,
        image_link TEXT,
        topic_id BIGINT,
        deleted BOOLEAN DEFAULT FALSE,
        last_updated_by VARCHAR(255),
        last_updated_at BIGINT,
        created_by VARCHAR(255),
        created_at BIGINT,
        CONSTRAINT fk_article_topic_id_topic_id
            FOREIGN KEY (topic_id) REFERENCES topics(id)
    );
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_articles_pub_date ON articles(pub_date DESC);
    CREATE INDEX IF NOT EXISTS idx_articles_topic_id ON articles(topic_id);
    """)
    conn.commit()

    logging.info("PostgreSQL kết nối OK!")
except Exception as e:
    logging.error(f"Không kết nối được DB: {e}")
    exit(1)

# ==================== AUTH & SOURCES ====================
session = requests.Session()
token: Optional[str] = None
sources_with_topics: List[Dict] = []

def login():
    global token
    resp = session.post(LOGIN_URL, json={"username": USERNAME, "password": PASSWORD}, timeout=10)
    resp.raise_for_status()
    token = resp.json()["data"]["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    logging.info("Đăng nhập thành công!")

def load_sources():
    global sources_with_topics
    resp = session.get(SOURCES_URL, timeout=15)
    resp.raise_for_status()
    raw = resp.json()["data"]

    topics = []
    for source in raw:
        for topic in source.get("topics", []):
            if topic.get("rss_url"):
                topics.append({
                    "source_id": source["id"],
                    "source_name": source["name"],
                    "topic_id": topic["id"],
                    "topic_name": topic["name"],
                    "rss_url": topic["rss_url"]
                })

    sources_with_topics = topics
    logging.info(f"Đã tải {len(topics)} RSS topic")


login()
load_sources()

# ==================== HELPERS ====================
def extract_image_from_entry(entry) -> Optional[str]:
    """
    Lấy ảnh theo ưu tiên:
    1. media:content -> url
    2. enclosure -> url
    3. <img src> trong description
    """

    # 1) media:content (MRSS)
    media = entry.get("media_content")
    if media and isinstance(media, list):
        for m in media:
            if "url" in m:
                return m["url"].strip()

    # 2) enclosure
    enclosures = entry.get("enclosures")
    if enclosures and isinstance(enclosures, list):
        for e in enclosures:
            if "url" in e:
                return e["url"].strip()

    # 3) fallback: ảnh trong description
    desc = entry.get("description") or entry.get("summary") or ""
    img = re.search(
        r'<img[^>]*src=["\']([^"\']+)["\']',
        desc,
        re.IGNORECASE | re.DOTALL
    )
    if img:
        return img.group(1).strip()

    return None


def get_pub_date_ms(entry) -> int:
    """Trả về pub_date dạng milliseconds UTC"""
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
        dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    return int(dt.timestamp() * 1000)


def get_key(entry):
    return entry.get('guid') or entry.get('link') or entry.title


def clean_title(title: str) -> str:
    if not title:
        return ""
    title = html.unescape(title)
    replacements = {
        '&rsquo;': "'", '&ldquo;': '"', '&rdquo;': '"', '&nbsp;': ' ',
        '&quot;': '"', '&#39;': "'", '&#34;': '"',
        '&ndash;': '-', '&mdash;': '—'
    }
    for k, v in replacements.items():
        title = title.replace(k, v)
    return re.sub(r'\s+', ' ', title).strip()

# ==================== CRAWL ONE RSS ====================
def crawl_rss_topic(topic_info: Dict):
    rss_url = topic_info["rss_url"]
    topic_id = topic_info["topic_id"]
    topic_name = topic_info["topic_name"]
    source_name = topic_info["source_name"]

    try:
        feed = feedparser.parse(rss_url, request_headers={'User-Agent': 'NewsCrawler/1.0'})
        if feed.bozo:
            logging.warning(f"[{source_name} - {topic_name}] RSS lỗi: {feed.bozo_exception}")
            return 0

        new = 0
        now_ms = int(time.time() * 1000)

        for entry in feed.entries:
            key = get_key(entry)
            if key in bloom:
                continue

            image_link = extract_image_from_entry(entry)
            pub_date_ms = get_pub_date_ms(entry)

            try:
                cur.execute("""
                    INSERT INTO articles 
                        (title, link, guid, description, pub_date, image_link, topic_id,
                         created_by, created_at, last_updated_by, last_updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            'admin', %s, 'admin', %s)
                    ON CONFLICT(link) DO NOTHING
                """, (
                    clean_title(entry.title)[:1024],
                    entry.get('link'),
                    entry.get('guid'),
                    (entry.get('description') or entry.get('summary') or '')[:100000],
                    pub_date_ms,
                    image_link,
                    topic_id,
                    now_ms,
                    now_ms
                ))

                if cur.rowcount > 0:
                    bloom.add(key)
                    conn.commit()
                    logging.info(f"[{source_name} - {topic_name}] MỚI - {clean_title(entry.title)[:70]}")
                    new += 1

            except Exception as e:
                conn.rollback()
                logging.error(f"Lưu DB lỗi [{source_name}-{topic_name}]: {e}")

        return new

    except Exception as e:
        logging.error(f"Crawl lỗi {rss_url}: {e}")
        return 0

# ==================== MAIN LOOP ====================
def crawl_once():
    logging.info(f"=== Bắt đầu crawl {len(sources_with_topics)} RSS ===")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        total_new = sum(executor.map(crawl_rss_topic, sources_with_topics))
    logging.info(f"=== HOÀN TẤT: {total_new} bài mới ===" if total_new else "Không có bài mới")

# ==================== START ====================
if __name__ == "__main__":
    logging.info("=== RSS CRAWLER V8 - Final Stable ===")
    crawl_once()

    try:
        while True:
            time.sleep(POLL_INTERVAL)
            crawl_once()
    except KeyboardInterrupt:
        logging.info("Dừng bởi người dùng")
    finally:
        save_bloom()
        cur.close()
        conn.close()
        logging.info("Đã đóng kết nối. Bye!")
