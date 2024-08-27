import streamlit as st
import asyncpraw
from kafka import KafkaProducer
import json
import asyncio
from datetime import datetime, timedelta
import nest_asyncio

# Cài đặt nest_asyncio để cho phép vòng lặp sự kiện đã chạy tiếp tục hoạt động
nest_asyncio.apply()

# Cấu hình và kết nối với Reddit API
reddit = asyncpraw.Reddit(
    client_id='FvUOzRsVAyqjhPwwURAQNA',
    client_secret='etpictk0iK7NoNvEjTjovK-lXQjZRg',
    user_agent='My user agent description',
    check_for_updates=False
)

# Cấu hình Kafka
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Thay đổi theo cấu hình Kafka của bạn
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def format_time(utc_timestamp):
    """Chuyển đổi thời gian UTC sang định dạng ngày giờ khu vực Việt Nam (UTC+7)."""
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    vietnam_time = utc_time + timedelta(hours=7)
    return vietnam_time.strftime('%Y-%m-%d %H:%M:%S')


async def fetch_latest_post():
    """Lấy bài viết mới nhất từ toàn bộ Reddit và gửi vào Kafka."""
    subreddit = await reddit.subreddit('all')  # Await the subreddit coroutine

    async for submission in subreddit.new(limit=1):
        post_data = {
            "Title": submission.title,
            "Link": submission.url,
            "Created Time (VN)": format_time(submission.created_utc)
        }

        # Gửi dữ liệu vào Kafka
        kafka_producer.send('your_topic_name', post_data)
        kafka_producer.flush()

        return post_data


def main():
    st.title('Reddit Latest Post Fetcher')

    if st.button('Fetch Latest Post'):
        st.write('Fetching the latest post...')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        post_data = loop.run_until_complete(fetch_latest_post())

        # Hiển thị bài viết mới nhất
        st.write(post_data)


if __name__ == "__main__":
    main()
