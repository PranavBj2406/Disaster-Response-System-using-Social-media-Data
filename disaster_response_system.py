
"""
Disaster Response System - Main Controller
This script coordinates the overall disaster response pipeline:
1. Tweet data collection
2. MapReduce processing
3. MongoDB storage
4. Spark streaming
5. Link analysis
"""

import os
import sys
import time
import subprocess
import logging
import argparse
import schedule
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("disaster_system.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("DisasterResponseSystem")

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HDFS_INPUT_PATH = "/user/disaster/tweets"
HDFS_OUTPUT_PATH = "/user/disaster/processed_tweets"
LOCAL_OUTPUT_DIR = os.path.join(BASE_DIR, "mapreduce_output")

def run_tweet_scraper():
    """Run the tweet scraper to collect disaster-related tweets"""
    logger.info("Starting tweet scraper...")
    try:
        subprocess.run(["python", os.path.join(BASE_DIR, "twitter_scrapper.py")], 
                      check=True)
        logger.info("Tweet scraping completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Tweet scraper failed with error: {e}")
        return False

def run_mapreduce_job():
    """Run the MapReduce job to process tweets"""
    logger.info("Starting MapReduce job...")
    try:
        # Clean up previous output directory if it exists
        subprocess.run(["hadoop", "fs", "-rm", "-r", "-skipTrash", HDFS_OUTPUT_PATH], 
                      stderr=subprocess.DEVNULL)
        
        # Run the MapReduce job
        subprocess.run([
            "hadoop", "jar", os.path.join(BASE_DIR, "disaster-filter.jar"),
            "DisasterTweetFilter", HDFS_INPUT_PATH, HDFS_OUTPUT_PATH
        ], check=True)
        
        logger.info("MapReduce job completed successfully")
        
        # Create local output directory if it doesn't exist
        os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
        
        # Get the current timestamp for unique directory name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(LOCAL_OUTPUT_DIR, f"output_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)
        
        # Copy the output files to local filesystem
        subprocess.run([
            "hadoop", "fs", "-get", f"{HDFS_OUTPUT_PATH}/part-*", output_dir
        ], check=True)
        
        logger.info(f"MapReduce output copied to {output_dir}")
        return output_dir
    
    except subprocess.CalledProcessError as e:
        logger.error(f"MapReduce job failed with error: {e}")
        return None

def import_to_mongodb(output_dir):
    """Import MapReduce output to MongoDB"""
    if not output_dir:
        logger.error("Cannot import to MongoDB: No output directory provided")
        return False
    
    logger.info(f"Importing data from {output_dir} to MongoDB...")
    try:
        subprocess.run([
            "python", os.path.join(BASE_DIR, "import_to_mongodb.py"), output_dir
        ], check=True)
        logger.info("MongoDB import completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"MongoDB import failed with error: {e}")
        return False

def start_spark_streaming():
    """Start Spark streaming for real-time processing"""
    logger.info("Starting Spark streaming application...")
    try:
        # Run in background
        process = subprocess.Popen([
            "spark-submit", 
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            os.path.join(BASE_DIR, "spark_streaming.py")
        ])
        logger.info(f"Spark streaming started with PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Failed to start Spark streaming: {e}")
        return None

def run_link_analysis():
    """Run link analysis on affected locations"""
    logger.info("Running link analysis...")
    try:
        subprocess.run([
            "python", os.path.join(BASE_DIR, "link_analysis.py")
        ], check=True)
        logger.info("Link analysis completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Link analysis failed with error: {e}")
        return False

def daily_batch_processing():
    """Run the daily batch processing pipeline"""
    logger.info("Starting daily batch processing")
    
    # 1. Collect new tweets
    run_tweet_scraper()
    
    # 2. Run MapReduce job
    output_dir = run_mapreduce_job()
    
    # 3. Import results to MongoDB
    if output_dir:
        import_to_mongodb(output_dir)
    
    # 4. Run link analysis
    run_link_analysis()
    
    logger.info("Daily batch processing completed")

def main():
    parser = argparse.ArgumentParser(description="Disaster Response System Controller")
    parser.add_argument("--batch", action="store_true", help="Run batch processing once and exit")
    parser.add_argument("--streaming", action="store_true", help="Start streaming processing")
    parser.add_argument("--schedule", action="store_true", help="Schedule regular processing")
    args = parser.parse_args()
    
    if args.batch:
        # Run batch processing once
        logger.info("Running one-time batch processing")
        daily_batch_processing()
    
    spark_process = None
    try:
        if args.streaming or args.schedule:
            # Start streaming
            spark_process = start_spark_streaming()
        
        if args.schedule:
            # Schedule daily batch processing
            logger.info("Scheduling daily batch processing at midnight")
            schedule.every().day.at("00:00").do(daily_batch_processing)
            
            # Schedule link analysis every 6 hours
            logger.info("Scheduling link analysis every 6 hours")
            schedule.every(6).hours.do(run_link_analysis)
            
            # Run the scheduler
            logger.info("Scheduler started")
            while True:
                schedule.run_pending()
                time.sleep(60)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Clean up
        if spark_process:
            logger.info(f"Terminating Spark streaming process (PID {spark_process.pid})")
            spark_process.terminate()

if __name__ == "__main__":
    main()