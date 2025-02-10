# project_consumer_seabaugh.py



#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict, Counter, deque  # data structure for counting author occurrences
import time
import datetime
import re

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import ntlk for tracking sentiment
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')


# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Sentiment Chart
#####################################

# Initialize Sentiment Analyzer
sia = SentimentIntensityAnalyzer()

# Data structure to store sentiment counts
sentiment_counts = Counter({"Positive": 0, "Neutral": 0, "Negative": 0})

# Initialize Matplotlib figure
fig_sentiment, ax_sentiment = plt.subplots()
plt.ion()  # Turn on interactive mode for real-time updates

def analyze_sentiment(text: str) -> str:
    """Determine sentiment category of a message."""
    score = sia.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

def update_sentiment_chart():
    """Update sentiment distribution pie chart."""
    ax_sentiment.clear()
    labels, sizes = zip(*sentiment_counts.items())
    ax_sentiment.pie(sizes, labels=labels, autopct="%1.1f%%", colors=["green", "gray", "red"])
    ax_sentiment.set_title("Real-Time Sentiment Analysis")
    plt.draw()
    plt.pause(0.01)

def process_sentiment(message_text: str):
    """Process message text for sentiment analysis."""
    sentiment = analyze_sentiment(message_text)
    sentiment_counts[sentiment] += 1
    update_sentiment_chart()

#####################################
# Time Series Line Chart
#####################################

# Data structure for message timestamps
message_timestamps = deque(maxlen=100)  # Stores last 100 timestamps

# Initialize Matplotlib figure
fig_volume, ax_volume = plt.subplots()

def update_message_volume():
    """Update time-series chart showing message volume per minute."""
    ax_volume.clear()
    
    # Convert timestamps to minute-based bins
    times = [datetime.datetime.fromtimestamp(ts).strftime('%H:%M') for ts in message_timestamps]
    time_counts = Counter(times)
    
    # Sort times for plotting
    sorted_times = sorted(time_counts.keys())
    sorted_counts = [time_counts[t] for t in sorted_times]

    ax_volume.plot(sorted_times, sorted_counts, marker="o", linestyle="-", color="blue")
    ax_volume.set_xlabel("Time (HH:MM)")
    ax_volume.set_ylabel("Messages per Minute")
    ax_volume.set_title("Real-Time Message Volume Over Time")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

def process_message_volume():
    """Log message timestamp and update chart."""
    message_timestamps.append(time.time())
    update_message_volume()


#####################################
# Time Series Line Chart
#####################################

# Keywords to track
keywords = ["Kafka", "Python", "JSON"]
keyword_counts = Counter({kw: 0 for kw in keywords})

# Initialize Matplotlib figure
fig_keywords, ax_keywords = plt.subplots()

def update_keyword_chart():
    """Update keyword frequency bar chart."""
    ax_keywords.clear()
    words, counts = zip(*keyword_counts.items())
    ax_keywords.bar(words, counts, color="orange")
    ax_keywords.set_xlabel("Keywords")
    ax_keywords.set_ylabel("Occurrences")
    ax_keywords.set_title("Keyword Frequency in Messages")
    plt.draw()
    plt.pause(0.01)

def process_keywords(message_text: str):
    """Track occurrences of keywords in message text."""
    for keyword in keywords:
        if re.search(rf"\b{keyword}\b", message_text, re.IGNORECASE):
            keyword_counts[keyword] += 1
    update_keyword_chart()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def update_chart():
    """Update the live chart with the latest author counts."""
    # Clear the previous chart
    ax.clear()

    # Get the authors and counts from the dictionary
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())

    # Create a bar chart using the bar() method.
    # Pass in the x list, the y list, and the color
    ax.bar(authors_list, counts_list, color="skyblue")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Authors")
    ax.set_ylabel("Message Counts")
    ax.set_title("Clayton's Real-Time Author Message Counts")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticklabels(authors_list, rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)


#####################################
# Function to process a single message
# #####################################


def process_message(message: dict) -> None:
    """
    Process a single message dictionary and update the charts.

    Args:
        message (dict): The message as a dictionary.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Extract fields from the message dictionary
        author = message.get("author", "unknown")
        message_text = message.get("message", "")

        # Update author counts
        author_counts[author] += 1
        logger.info(f"Updated author counts: {dict(author_counts)}")

        # Update charts
        update_chart()
        process_sentiment(message_text)
        process_message_volume()
        process_keywords(message_text)

    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
