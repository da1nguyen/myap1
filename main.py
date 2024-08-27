import asyncpraw
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import nest_asyncio
import streamlit as st
from textblob import TextBlob
import numpy as np

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

    async for submission in subreddit.new(limit=10):
        if submission.id not in seen_submission_ids:
            seen_submission_ids.add(submission.id)

            sentiment = analyze_sentiment(submission.title)

            # Thêm bài viết vào danh sách
            posts_list.append({
                "Title": submission.title,
                "Created Time (VN)": format_time(submission.created_utc),
                "Sentiment": sentiment,
                "Created Time UTC": submission.created_utc  # Thêm thời gian để sắp xếp
            })

            # Cập nhật DataFrame và xử lý lỗi
            df = pd.DataFrame(posts_list)
            
            if "Created Time UTC" in df.columns:
                # Xử lý lỗi nếu có giá trị không hợp lệ trong cột 'Created Time UTC'
                df['Created Time UTC'] = pd.to_numeric(df['Created Time UTC'], errors='coerce')
                df = df.dropna(subset=['Created Time UTC'])
                
                try:
                    df = df.sort_values(by="Created Time UTC", ascending=False).drop(columns=["Created Time UTC"])
                except Exception as e:
                    st.error(f"Error sorting DataFrame: {e}")
            
            # Hiển thị bảng dữ liệu với cột "Title" được mở rộng
            dataframe_placeholder.dataframe(df, height=400, use_container_width=True)

        await asyncio.sleep(0.5)

async def main(max_iterations=10):
    """Chạy vòng lặp chính để lấy và hiển thị bài viết."""
    iteration = 0
    while iteration < max_iterations:
        await fetch_latest_posts()
        iteration += 1

# Khởi tạo placeholder cho bảng dữ liệu
dataframe_placeholder = st.empty()

# Nút để bắt đầu quá trình lấy bài viết
if st.button('Start Fetching'):
    asyncio.run(main())
