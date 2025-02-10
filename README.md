# buzzline-04-seabaugh

# Real-Time Kafka Consumer with Analytics and Visualization

## Overview
This Kafka consumer processes real-time messages and implements multiple analytics features, including sentiment analysis, message volume tracking, and keyword tracking. It updates live visualizations using Matplotlib.

## Features
### 1. Real-Time Sentiment Analysis
- Uses **VADER Sentiment Analysis** to classify messages as **Positive, Neutral, or Negative**.
- Displays a **pie chart** showing the distribution of sentiments in received messages.

### 2. Message Volume Tracking Over Time
- Tracks the number of messages received **per minute**.
- Displays a **time-series chart** showing message volume trends.

### 3. Keyword Tracking
- Monitors specific keywords (e.g., **"Kafka"**, **"Python"**) in messages.
- Displays a **bar chart** showing the frequency of tracked keywords.

## Setup and Installation
### Prerequisites
Ensure you have the following installed:
- Python 3.8+
- Kafka server running
- Required Python libraries:
  ```sh
  pip install kafka-python matplotlib nltk
  ```
  
### Environment Variables
Set up the following environment variables in a `.env` file:
```
BUZZ_TOPIC=<your_kafka_topic>
BUZZ_CONSUMER_GROUP_ID=<your_consumer_group>
```

### Running the Consumer
Start the Kafka consumer by running:
```sh
python consumer.py
```

## Code Breakdown
### **Sentiment Analysis**
- Uses `nltk.sentiment.SentimentIntensityAnalyzer` to classify messages.
- Updates a **pie chart** with sentiment distribution.

### **Message Volume Tracking**
- Records timestamps of received messages.
- Updates a **time-series line chart** with messages per minute.

### **Keyword Tracking**
- Checks messages for specific keywords.
- Updates a **bar chart** with keyword frequencies.

## Integration
Modify `process_message()` in `consumer.py` to call the following functions:
```python
process_sentiment(message_text)
process_message_volume()
process_keywords(message_text)
```

## Notes
- The consumer automatically updates charts in real-time.
- Stop the consumer using `CTRL+C`.
- Ensure Kafka is running before starting the consumer.

## Future Enhancements
- Implement additional NLP techniques for better sentiment accuracy.
- Store processed data in a database for historical analysis.
- Add an interactive dashboard for enhanced data visualization.