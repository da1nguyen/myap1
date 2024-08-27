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


df = pd.DataFrame(columns=["Title", "Created Time (VN)", "Sentiment"])

def format_time(utc_timestamp):
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    vietnam_time = utc_time + timedelta(hours=7)
    return vietnam_time.strftime('%Y-%m-%d %H:%M:%S')

def analyze_sentiment(text):
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity >= 0.6:
        return 'Very Positive'
    elif polarity >= 0.2:
        return 'Positive'
    elif polarity >= -0.2:
        return 'Neutral'
    elif polarity >= -0.6:
        return 'Negative'
    else:
        return 'Very Negative'

async def fetch_latest_posts():
    global df
    seen_submission_ids = set()
    subreddit = await reddit.subreddit('all')

    # Tạo một không gian để cập nhật bảng dữ liệu
    st.write("### Real-time Text Mining Analysis")
    data_placeholder = st.empty()

    while True:
        async for submission in subreddit.new(limit=10):
            if submission.id not in seen_submission_ids:
                seen_submission_ids.add(submission.id)

                sentiment = analyze_sentiment(submission.title)

                new_entry = {
                    "Content": submission.title,
                    "Created Time (VN)": format_time(submission.created_utc),
                    "Sentiment": sentiment
                }
                df = pd.concat([pd.DataFrame([new_entry]), df], ignore_index=True)
                df = df.sort_values(by="Created Time (VN)", ascending=False).reset_index(drop=True)

                with data_placeholder:
                    st.dataframe(df, use_container_width=True)

            await asyncio.sleep(1)  # Thay đổi thời gian chờ tùy theo nhu cầu

if st.button('Execute the program'):
    asyncio.run(fetch_latest_posts())
