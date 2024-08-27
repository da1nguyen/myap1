import streamlit as st
from confluent_kafka import Consumer, KafkaException, KafkaError
import asyncio
import nest_asyncio

# Apply nest_asyncio to allow the event loop to continue running
nest_asyncio.apply()

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

# Initialize the Kafka Consumer
consumer = Consumer(conf)
topic = 'your_topic_name'  # Replace with your actual Kafka topic
consumer.subscribe([topic])

# Function to fetch messages from Kafka
async def fetch_messages():
    try:
        msg = consumer.poll(timeout=1.0)  # Poll for a message
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                st.write("End of partition reached.")
            else:
                raise KafkaException(msg.error())
        else:
            return msg.value().decode('utf-8')  # Decode the message
    except Exception as e:
        st.error(f"Error fetching message: {e}")
        return None

# Main function to continuously fetch messages
async def main():
    while True:
        message = await fetch_messages()
        if message:
            st.write(f"Received message: {message}")
        await asyncio.sleep(1)  # Wait for 1 second before fetching the next message

# Run the event loop
if __name__ == "__main__":
    asyncio.run(main())
