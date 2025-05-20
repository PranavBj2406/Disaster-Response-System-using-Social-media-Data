import json
import os
import sys
from pymongo import MongoClient

def import_to_mongodb(input_dir, db_name='disaster_response', collection_name='disaster_alerts'):
    """
    Import MapReduce output to MongoDB
    
    Args:
        input_dir: Directory containing MapReduce output files
        db_name: MongoDB database name
        collection_name: MongoDB collection name
    """
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client[db_name]
        collection = db[collection_name]
        
        # Process all part-r-* files in the output directory
        count = 0
        for filename in os.listdir(input_dir):
            if filename.startswith('part-r-'):
                filepath = os.path.join(input_dir, filename)
                print(f"Processing file: {filepath}")
                
                with open(filepath, 'r') as f:
                    for line in f:
                        # Each line has format: location:disaster_type    {"json": "data"}
                        try:
                            key, value = line.strip().split('\t', 1)
                            
                            # Parse the JSON data
                            data = json.loads(value)
                            
                            # Add the key parts as fields
                            if ':' in key:
                                location, disaster_type = key.split(':', 1)
                                data['_location'] = location
                                data['_disaster_type'] = disaster_type
                            
                            # Insert into MongoDB
                            result = collection.insert_one(data)
                            count += 1
                            
                            if count % 100 == 0:
                                print(f"Imported {count} documents...")
                                
                        except Exception as e:
                            print(f"Error processing line: {line}")
                            print(f"Error details: {e}")
        
        print(f"Import complete. Total documents imported: {count}")
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_to_mongodb.py <mapreduce_output_dir>")
        sys.exit(1)
        
    input_dir = sys.argv[1]
    import_to_mongodb(input_dir)