import streamlit as st
import asyncpraw
from confluent_kafka import Consumer, Producer, KafkaException
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
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['your_topic_name'])  # Thay đổi theo tên topic Kafka của bạn

def format_time(utc_timestamp):
    """Chuyển đổi thời gian UTC sang định dạng ngày giờ khu vực Việt Nam (UTC+7)."""
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    vietnam_time = utc_time + timedelta(hours=7)
    return vietnam_time.strftime('%Y-%m-%d %H:%M:%S')

async def fetch_latest_post():
    """Lấy bài viết mới nhất từ toàn bộ Reddit và gửi vào Kafka."""
    subreddit = await reddit.subreddit('all')

    async for submission in subreddit.new(limit=1):
        post_data = {
            "Title": submission.title,
            "Link": submission.url,
            "Created Time (VN)": format_time(submission.created_utc)
        }

        # Gửi dữ liệu vào Kafka
        producer.produce('your_topic_name', value=json.dumps(post_data))
        producer.flush()

        return post_data

def consume_messages():
    """Tiêu thụ và trả về tin nhắn từ Kafka."""
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        return None
    if msg.error():
        raise KafkaException(msg.error())
    return msg.value().decode('utf-8')

def main():
    st.title('Reddit to Kafka Streamlit App')

    if st.button('Fetch Latest Post'):
        st.write('Fetching the latest post...')
        # Chạy hàm fetch_latest_post trong môi trường đồng bộ
        post_data = asyncio.run(fetch_latest_post())
        st.write(post_data)

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
