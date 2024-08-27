import streamlit as st
import asyncpraw
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import nest_asyncio
import json
from confluent_kafka import Producer, Consumer, KafkaException
from transformers import pipeline

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
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['your_topic_name'])  # Thay đổi theo tên topic Kafka của bạn

# Khởi tạo mô hình dự đoán cảm xúc
sentiment_model = pipeline('sentiment-analysis')

# Danh sách để lưu bài viết
posts_list = []

def format_time(utc_timestamp):
    """Chuyển đổi thời gian UTC sang định dạng ngày giờ khu vực Việt Nam (UTC+7)."""
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    vietnam_time = utc_time + timedelta(hours=7)
    return vietnam_time.strftime('%Y-%m-%d %H:%M:%S')

async def fetch_latest_posts():
    """Lấy bài viết mới nhất từ toàn bộ Reddit và chỉ lưu bài viết mới."""
    global posts_list

    seen_submission_ids = set()

    subreddit = await reddit.subreddit('all')  # Await the subreddit coroutine

    async for submission in subreddit.new(limit=10):  # Lấy nhiều bài viết hơn để kiểm tra
        if submission.id not in seen_submission_ids:
            seen_submission_ids.add(submission.id)

            # Dự đoán cảm xúc cho bài viết
            sentiment = sentiment_model(submission.title)[0]
            sentiment_label = sentiment['label']
            sentiment_score = sentiment['score']

            # Tạo dữ liệu bài viết cùng với dự đoán cảm xúc
            post_data = {
                "Title": submission.title,
                "Created Time (VN)": format_time(submission.created_utc),
                "Sentiment Label": sentiment_label,
                "Sentiment Score": sentiment_score
            }

            # Thêm bài viết vào danh sách
            posts_list.append(post_data)

            # Chuyển dữ liệu vào DataFrame
            df = pd.DataFrame(posts_list)

            # Gửi dữ liệu vào Kafka
            producer.produce('your_topic_name', value=json.dumps(post_data))
            producer.flush()

        # Chờ 1 giây trước khi lấy dữ liệu mới
        await asyncio.sleep(0.5)

def consume_messages():
    """Tiêu thụ và trả về tin nhắn từ Kafka."""
    try:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())
        return msg.value().decode('utf-8')
    except KafkaException as e:
        st.error(f"Kafka error: {str(e)}")
        return None

def main():
    st.title('Reddit to Kafka Streamlit App')

    if st.button('Fetch Latest Post'):
        st.write('Fetching the latest post...')
        # Chạy hàm fetch_latest_posts trong môi trường đồng bộ
        loop = asyncio.get_event_loop()
        loop.run_until_complete(fetch_latest_posts())
        st.write(pd.DataFrame(posts_list).tail(1))

    st.write('Consuming messages from Kafka:')
    # Hiển thị tin nhắn từ Kafka
    try:
        while True:
            message = consume_messages()
            if message:
                st.write(message)
            else:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
