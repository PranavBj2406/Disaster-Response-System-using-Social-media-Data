from faker import Faker
import random
import json
import time
import os
from datetime import datetime
import subprocess
import shutil

fake = Faker()

# Define disaster keywords and locations
disaster_keywords = ["flood", "earthquake", "hurricane", "wildfire", "tsunami", "tornado", "landslide"]
locations = ["Mumbai", "Tokyo", "California", "Jakarta", "Dhaka", "Lagos", "Manila"]

def generate_disaster_tweet():
    """Generate a fake disaster tweet with metadata"""
    disaster = random.choice(disaster_keywords)
    location = random.choice(locations)
    severity = random.randint(1, 5)  # On a scale of 1-5
    
    # Add some randomness to make tweets more realistic
    if random.random() < 0.7:  # 70% of tweets mention the disaster directly
        tweet = f"URGENT: {disaster.capitalize()} reported in {location}. {fake.sentence()}"
    else:  # 30% more subtle mentions
        tweet = f"Anyone else in {location} experiencing this? {fake.sentence()} #possible{disaster}"
    
    # Generate coordinates with some randomness around city centers (simplified)
    base_coords = {
        "Mumbai": (19.076, 72.877),
        "Tokyo": (35.682, 139.759),
        "California": (36.778, -119.417),
        "Jakarta": (-6.200, 106.816),
        "Dhaka": (23.810, 90.415),
        "Lagos": (6.524, 3.379),
        "Manila": (14.599, 120.984)
    }
    
    # Add some randomness to coordinates
    base_lat, base_lng = base_coords[location]
    lat = base_lat + (random.random() - 0.5) * 0.5  # +/- 0.25 degrees
    lng = base_lng + (random.random() - 0.5) * 0.5
    
    timestamp = datetime.now().isoformat()
    
    return {
        "text": tweet,
        "location": location,
        "disaster_type": disaster,
        "severity": severity,
        "coordinates": {"lat": lat, "lng": lng},
        "timestamp": timestamp,
        "user_id": fake.uuid4(),
        "retweet_count": random.randint(0, 1000),
        "verified_report": random.random() < 0.3  # 30% chance of being from verified source
    }

def check_hadoop_available():
    """Check if Hadoop is available in the system"""
    # Attempt with explicit Hadoop path for Windows
    hadoop_cmd = "hadoop"
    hadoop_path = "C:/hadoop/bin/hadoop.cmd"  # Windows hadoop command
    
    if os.path.exists(hadoop_path):
        hadoop_cmd = hadoop_path
    
    try:
        result = subprocess.run([hadoop_cmd, "version"], capture_output=True, text=True, check=False)
        return result.returncode == 0, hadoop_cmd
    except FileNotFoundError:
        return False, hadoop_cmd

def save_to_local_and_hdfs(tweets, batch_num):
    """Save tweets to local file and optionally to HDFS if available"""
    # Create directory if it doesn't exist
    if not os.path.exists("tweets"):
        os.makedirs("tweets")
    
    # Save to local file
    filename = f"tweets/disaster_tweets_batch_{batch_num}.json"
    with open(filename, 'w') as f:
        for tweet in tweets:
            f.write(json.dumps(tweet) + '\n')
    
    print(f"Saved batch {batch_num} with {len(tweets)} tweets to {filename}")
    
    # Check if Hadoop is available
    hadoop_available, hadoop_cmd = check_hadoop_available()
    
    if hadoop_available:
        # Move to HDFS
        hdfs_path = "/user/disaster_response/raw_tweets/"
        try:
            # Create HDFS directory if it doesn't exist
            subprocess.run([hadoop_cmd, "fs", "-mkdir", "-p", hdfs_path], check=False)
            
            # Copy file to HDFS
            subprocess.run([hadoop_cmd, "fs", "-put", "-f", filename, hdfs_path], check=True)
            print(f"Moved {filename} to HDFS path {hdfs_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error copying to HDFS: {e}")
    else:
        print(f"Hadoop not available using command '{hadoop_cmd}'. Skipping HDFS operations.")
        
        # If using for a disaster response system, we might want to save to a backup location
        backup_dir = "hdfs_backup"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        backup_file = os.path.join(backup_dir, os.path.basename(filename))
        shutil.copy2(filename, backup_file)
        print(f"Created backup copy at {backup_file}")

def main():
    batch_size = 50  # Number of tweets per batch
    batch_interval = 30  # Seconds between batches
    max_batches = 10  # Set to None for continuous operation
    
    print(f"Starting tweet generator. Press Ctrl+C to stop.")
    print(f"Generating {batch_size} tweets every {batch_interval} seconds.")
    
    # Check for Hadoop at startup
    hadoop_available, hadoop_cmd = check_hadoop_available()
    if hadoop_available:
        print(f"Hadoop detected using command: {hadoop_cmd}")
    else:
        print(f"WARNING: Hadoop not found using command: {hadoop_cmd}. Will save files locally only.")
    
    batch_num = 1
    
    try:
        while True:
            if max_batches and batch_num > max_batches:
                print(f"Reached maximum number of batches ({max_batches}). Exiting.")
                break
                
            # Generate a batch of tweets
            tweets = [generate_disaster_tweet() for _ in range(batch_size)]
            
            # Save the batch
            save_to_local_and_hdfs(tweets, batch_num)
            
            batch_num += 1
            time.sleep(batch_interval)
            
    except KeyboardInterrupt:
        print("Tweet generator stopped by user")

if __name__ == "__main__":
    main()