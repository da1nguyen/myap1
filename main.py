import streamlit as st
import asyncpraw
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import nest_asyncio
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

# Danh sách để lưu bài viết
posts_list = []

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
    global posts_list

    seen_submission_ids = set()

    subreddit = await reddit.subreddit('all')

    async for submission in subreddit.new(limit=10):  # Lấy nhiều bài viết hơn để kiểm tra
        if submission.id not in seen_submission_ids:
            seen_submission_ids.add(submission.id)

            sentiment = analyze_sentiment(submission.title)

            # Thêm bài viết vào danh sách
            posts_list.append({
                "Title": submission.title,
                "Created Time (VN)": format_time(submission.created_utc),
                "Sentiment": sentiment
            })

            # Chuyển dữ liệu vào DataFrame
            df = pd.DataFrame(posts_list)

            # Hiển thị DataFrame mới nhất
            st.write(df.tail(1))  # Hiển thị bài viết mới nhất

        # Chờ 1 giây trước khi lấy dữ liệu mới
        await asyncio.sleep(0.5)

# Chạy hàm chính
async def main():
    while True:
        await fetch_latest_posts()

# Chạy Streamlit với vòng lặp sự kiện
if __name__ == "__main__":
    st.title("Real-Time Reddit Post Sentiment Analysis")
    st.write("Fetching latest posts from Reddit and analyzing sentiment in real-time...")

    asyncio.run(main())
