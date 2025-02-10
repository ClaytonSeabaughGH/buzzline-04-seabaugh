"""
json_consumer_seabaugh.py

Real-Time Kafka Consumer with Analytics and Visualization

Consumes JSON messages from a Kafka topic and visualizes sentiment analysis, message volume over time, and keyword frequencies in real-time.

Example JSON message:
{"message": "I love Python and Kafka!", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict
from datetime import datetime

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv
import matplotlib.pyplot as plt

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# NLTK Setup
#####################################

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    return os.getenv("BUZZ_TOPIC", "default_topic")

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    return os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")

#####################################
# Data Structures for Analytics
#####################################

sentiment_counts = defaultdict(int)
message_timestamps = []
keywords = ['Kafka', 'Python', 'data', 'real-time', 'analysis']
keyword_counts = defaultdict(int)

#####################################
# Set Up Live Visualization
#####################################

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 12))
plt.ion()

#####################################
# Visualization Update Function
#####################################

def update_chart():
    """Update all live charts with latest analytics data."""
    # Clear previous state
    ax1.clear()
    ax2.clear()
    ax3.clear()

    # Sentiment Pie Chart
    labels = ['Positive', 'Neutral', 'Negative']
    sizes = [sentiment_counts['positive'], 
             sentiment_counts['neutral'], 
             sentiment_counts['negative']]
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', 
            colors=['#4CAF50', '#FFEB3B', '#F44336'])
    ax1.set_title('Real-Time Sentiment Analysis')

    # Message Volume Line Chart
    if message_timestamps:
        minutes = [ts.replace(second=0, microsecond=0) for ts in message_timestamps]
        time_counts = defaultdict(int)
        for minute in minutes:
            time_counts[minute] += 1
        times = sorted(time_counts.keys())
        counts = [time_counts[t] for t in times]
        ax2.plot(times, counts, marker='o', linestyle='-', color='#2196F3')
        ax2.set_title('Message Volume Over Time')
        ax2.set_ylabel('Messages per Minute')
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

    # Keyword Frequency Bar Chart
    if keyword_counts:
        keywords = list(keyword_counts.keys())
        counts = list(keyword_counts.values())
        ax3.bar(keywords, counts, color='#9C27B0')
        ax3.set_title('Keyword Frequency Tracking')
        ax3.set_ylabel('Count')
        plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Message Processing Function
#####################################

def process_message(message: str) -> None:
    """Process message and update analytics data."""
    try:
        msg = json.loads(message)
        if isinstance(msg, dict):
            message_text = msg.get('message', '')
            
            # Sentiment Analysis
            scores = sia.polarity_scores(message_text)
            compound = scores['compound']
            if compound >= 0.05:
                sentiment = 'positive'
            elif compound <= -0.05:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'
            sentiment_counts[sentiment] += 1

            # Keyword Tracking
            clean_text = message_text.lower()
            for keyword in keywords:
                if keyword.lower() in clean_text:
                    keyword_counts[keyword] += 1

            # Message Volume Tracking
            message_timestamps.append(datetime.now())

            update_chart()
            logger.info(f"Processed message: {message_text[:50]}...")

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

#####################################
# Main Consumer Logic
#####################################

def main() -> None:
    """Main entry point for the Kafka consumer."""
    logger.info("Starting real-time analytics consumer")
    
    consumer = create_kafka_consumer(
        get_kafka_topic(),
        get_kafka_consumer_group_id()
    )

    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()