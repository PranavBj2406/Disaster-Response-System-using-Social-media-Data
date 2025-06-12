from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, max as spark_max, sum as spark_sum, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import pymongo
from pymongo import MongoClient
import os
import time
import json
import threading
from datetime import datetime

# Windows-compatible Spark session (no Hadoop required)
spark = SparkSession \
    .builder \
    .appName("DisasterTweetProcessor") \
    .config("spark.master", "local[*]") \
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session created successfully!")

# Define schema for your tweet data
tweet_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("location", StringType(), True),
    StructField("disaster_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("verified_report", BooleanType(), True),
    StructField("retweet_count", IntegerType(), True)
])

# Disaster keywords
disaster_keywords = ["flood", "earthquake", "hurricane", "wildfire", "tsunami", "tornado", "landslide", "emergency", "disaster", "crisis"]

def find_json_files():
    """Find your JSON files in the project directory"""
    json_files = []
    
    # Look for JSON files with disaster_tweets pattern
    for file in os.listdir("."):
        if file.startswith("disaster_tweets_batch_") and file.endswith(".json"):
            json_files.append(file)
    
    # Also check tweets_json folder if it exists
    if os.path.exists("tweets_json"):
        for file in os.listdir("tweets_json"):
            if file.startswith("disaster_tweets_batch_") and file.endswith(".json"):
                json_files.append(os.path.join("tweets_json", file))
    
    return sorted(json_files)

def process_json_file(file_path, batch_id):
    """Process a single JSON file"""
    try:
        print(f"\n🔄 Processing: {file_path}")
        
        # Read JSON file into Spark DataFrame
        df = spark.read.option("multiLine", "true").json(file_path)
        
        print(f"📊 Loaded {df.count()} tweets from {file_path}")
        
        # Show sample data structure
        print("📋 Sample data:")
        df.show(3, truncate=False)
        
        # Create temporary view
        df.createOrReplaceTempView("tweets")
        
        # Build disaster filter
        keyword_conditions = []
        for keyword in disaster_keywords:
            keyword_conditions.append(f"lower(text) LIKE '%{keyword}%'")
        
        keyword_filter = " OR ".join(keyword_conditions)
        
        # Filter disaster-related tweets
        filtered_df = spark.sql(f"""
            SELECT *
            FROM tweets
            WHERE (
                {keyword_filter}
                OR severity >= 4
                OR retweet_count > 100
                OR verified_report = true
            )
        """)
        
        filtered_count = filtered_df.count()
        print(f"🚨 Found {filtered_count} disaster-related tweets")
        
        if filtered_count == 0:
            print("ℹ️  No disaster alerts generated from this batch")
            return []
        
        # Show filtered tweets
        print("🔍 Disaster tweets:")
        filtered_df.select("text", "location", "disaster_type", "severity").show(5, truncate=False)
        
        # Aggregate by location and disaster type
        aggregated_df = spark.sql(f"""
            SELECT 
                location,
                disaster_type,
                COUNT(*) as count,
                AVG(severity) as avg_severity,
                SUM(CASE WHEN verified_report = true THEN 1 ELSE 0 END) as verified_reports,
                MAX(timestamp) as last_updated
            FROM tweets
            WHERE (
                {keyword_filter}
                OR severity >= 4
                OR retweet_count > 100
                OR verified_report = true
            )
            GROUP BY location, disaster_type
            ORDER BY count DESC, avg_severity DESC
        """)
        
        # Calculate alert levels
        processed_df = aggregated_df.withColumn(
            "alert_level",
            when((col("avg_severity") >= 4) | (col("count") > 5) | (col("verified_reports") >= 2), 3)
            .when((col("avg_severity") >= 3) | (col("count") > 2) | (col("verified_reports") >= 1), 2)
            .otherwise(1)
        )
        
        print("📈 Alert Summary:")
        processed_df.show(truncate=False)
        
        # Convert to Python objects
        results = processed_df.collect()
        alerts = []
        
        for row in results:
            alert = {
                "batch_id": batch_id,
                "file_source": file_path,
                "location": row["location"] if row["location"] else "Unknown",
                "disaster_type": row["disaster_type"] if row["disaster_type"] else "General",
                "count": row["count"],
                "avg_severity": float(row["avg_severity"]) if row["avg_severity"] else 0.0,
                "verified_reports": row["verified_reports"],
                "last_updated": row["last_updated"],
                "alert_level": row["alert_level"],
                "alert_level_name": ["", "LOW", "MEDIUM", "HIGH"][row["alert_level"]],
                "processing_time": datetime.now().isoformat()
            }
            alerts.append(alert)
        
        return alerts
        
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return []

def save_alerts_to_mongodb(alerts):
    """Save alerts to MongoDB"""
    if not alerts:
        return False
        
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["disaster_response"]
        collection = db["batch_alerts"]
        
        # Insert alerts
        result = collection.insert_many(alerts)
        print(f"✅ Saved {len(result.inserted_ids)} alerts to MongoDB")
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False

def print_alerts(alerts):
    """Print alerts to console with nice formatting"""
    if not alerts:
        print("ℹ️  No alerts to display")
        return
        
    print(f"\n🚨 DISASTER ALERTS SUMMARY ({len(alerts)} alerts)")
    print("=" * 80)
    
    # Group by alert level
    high_alerts = [a for a in alerts if a["alert_level"] == 3]
    medium_alerts = [a for a in alerts if a["alert_level"] == 2]
    low_alerts = [a for a in alerts if a["alert_level"] == 1]
    
    for alert_group, emoji, name in [
        (high_alerts, "🟥", "HIGH"),
        (medium_alerts, "🟧", "MEDIUM"), 
        (low_alerts, "🟨", "LOW")
    ]:
        if alert_group:
            print(f"\n{emoji} {name} PRIORITY ALERTS ({len(alert_group)})")
            print("-" * 50)
            for alert in alert_group:
                print(f"📍 Location: {alert['location']}")
                print(f"🌪️  Disaster: {alert['disaster_type']}")
                print(f"📊 Stats: {alert['count']} reports | {alert['verified_reports']} verified | Avg severity: {alert['avg_severity']:.1f}")
                print(f"🕒 Last updated: {alert['last_updated']}")
                print(f"📁 Source: {alert['file_source']}")
                print()

def main():
    """Main processing function"""
    print("🚀 Starting Disaster Tweet Processing...")
    print("🔍 Searching for JSON files...")
    
    # Find JSON files
    json_files = find_json_files()
    
    if not json_files:
        print("❌ No disaster tweet JSON files found!")
        print("Expected files: disaster_tweets_batch_*.json")
        print("Current directory files:")
        for file in os.listdir("."):
            if file.endswith(".json"):
                print(f"  - {file}")
        return
    
    print(f"✅ Found {len(json_files)} JSON files:")
    for file in json_files:
        print(f"  - {file}")
    
    all_alerts = []
    
    # Process each file
    for i, file_path in enumerate(json_files, 1):
        print(f"\n{'='*60}")
        print(f"📦 Processing Batch {i}/{len(json_files)}")
        print(f"{'='*60}")
        
        alerts = process_json_file(file_path, i)
        all_alerts.extend(alerts)
        
        # Add delay between files for demo effect
        if i < len(json_files):
            print("⏳ Waiting 3 seconds before next batch...")
            time.sleep(3)
    
    # Final summary
    print(f"\n{'='*80}")
    print("📊 FINAL PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"📁 Files processed: {len(json_files)}")
    print(f"🚨 Total alerts generated: {len(all_alerts)}")
    
    if all_alerts:
        # Print all alerts
        print_alerts(all_alerts)
        
        # Save to MongoDB
        print("\n💾 Saving to MongoDB...")
        if save_alerts_to_mongodb(all_alerts):
            print("✅ All alerts saved to MongoDB successfully!")
        else:
            print("⚠️  MongoDB save failed, but alerts are displayed above")
            
        # Save to JSON file as backup
        backup_file = f"disaster_alerts_{int(time.time())}.json"
        try:
            with open(backup_file, 'w') as f:
                json.dump(all_alerts, f, indent=2)
            print(f"📄 Backup saved to: {backup_file}")
        except Exception as e:
            print(f"❌ Backup save failed: {e}")
    
    print(f"\n🎉 Processing completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Processing interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("🧹 Cleaning up...")
        spark.stop()
        print("✅ Spark session closed")