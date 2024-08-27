import asyncpraw
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import nest_asyncio
import streamlit as st
from textblob import TextBlob

# Cài đặt nest_asyncio để cho phép vòng lặp sự kiện đã chạy tiếp tục hoạt động
nest_asyncio.apply()

# Cấu hình và kết nối với Reddit API
reddit = asyncpraw.Reddit(
    client_id='FvUOzRsVAyqjhPwwURAQNA',
    client_secret='etpictk0iK7NoNvEjTjovK-lXQjZRg',
    user_agent='My user agent description',
    check_for_updates=False
)

# Khởi tạo DataFrame
posts_list = []
df = pd.DataFrame(posts_list)

def format_time(utc_timestamp):
    """Chuyển đổi thời gian UTC sang định dạng ngày giờ khu vực Việt Nam (UTC+7)."""
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    vietnam_time = utc_time + timedelta(hours=7)
    return vietnam_time.strftime('%Y-%m-%d %H:%M:%S')

def analyze_sentiment(text):
    """Phân tích cảm xúc của văn bản."""
    analysis = TextBlob(text)
    return 'Positive' if analysis.sentiment.polarity > 0 else 'Negative' if analysis.sentiment.polarity < 0 else 'Neutral'

async def fetch_latest_posts():
    """Lấy bài viết mới nhất từ toàn bộ Reddit và chỉ lưu bài viết mới."""
    global df
    seen_submission_ids = set()
    subreddit = await reddit.subreddit('all')

    while True:
        async for submission in subreddit.new(limit=10):
            if submission.id not in seen_submission_ids:
                seen_submission_ids.add(submission.id)

                sentiment = analyze_sentiment(submission.title)

                # Cập nhật DataFrame
                global df
                new_entry = {
                    "Title": submission.title,
                    "Created Time (VN)": format_time(submission.created_utc),
                    "Sentiment": sentiment
                }
                df = pd.concat([pd.DataFrame([new_entry]), df], ignore_index=True)
                df = df.sort_values(by="Created Time (VN)", ascending=False).reset_index(drop=True)

                # Hiển thị bảng dữ liệu với thanh cuộn
                st.dataframe(df, use_container_width=True)

            await asyncio.sleep(1)  # Thay đổi thời gian chờ tùy theo nhu cầu

# Chạy ứng dụng Streamlit
if st.button('Start Fetching'):
    asyncio.run(fetch_latest_posts())
