import csv
import json
import os
import sys
import glob
from datetime import datetime

def convert_csv_to_json(input_path, output_path):
    """
    Convert CSV files in input_path to line-delimited JSON files in output_path
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(input_path, "*.csv"))
    
    for csv_file in csv_files:
        # Create output filename by replacing .csv with .json
        base_name = os.path.basename(csv_file)
        json_file = os.path.join(output_path, base_name.replace(".csv", ".json"))
        
        print(f"Converting {csv_file} to {json_file}")
        
        try:
            # Read CSV and write JSON
            with open(csv_file, 'r', encoding='utf-8') as f_in:
                reader = csv.DictReader(f_in)
                
                with open(json_file, 'w', encoding='utf-8') as f_out:
                    for row in reader:
                        # Convert string values to appropriate types
                        try:
                            if 'severity' in row:
                                row['severity'] = int(row['severity'])
                            if 'retweet_count' in row:
                                row['retweet_count'] = int(row['retweet_count'])
                            if 'verified_report' in row:
                                row['verified_report'] = row['verified_report'] == '1'
                            if 'lat' in row:
                                row['lat'] = float(row['lat'])
                            if 'lng' in row:
                                row['lng'] = float(row['lng'])
                        except (ValueError, KeyError) as e:
                            print(f"Warning: Error converting types in row {row}: {e}")
                            continue
                        
                        # Write as line-delimited JSON
                        f_out.write(json.dumps(row) + '\n')
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            continue
    
    return len(csv_files)

def convert_and_upload_to_hdfs(local_input_path, local_output_path, hdfs_output_path):
    """
    Convert CSV files to JSON and upload to HDFS
    """
    # First convert files locally
    num_files = convert_csv_to_json(local_input_path, local_output_path)
    
    # Then upload to HDFS if requested
    if hdfs_output_path:
        try:
            # Try to create HDFS directory
            result = os.system(f"hadoop fs -mkdir -p {hdfs_output_path}")
            if result != 0:
                raise RuntimeError("Failed to create HDFS directory")
            
            # Upload all JSON files
            json_files = glob.glob(os.path.join(local_output_path, "*.json"))
            if not json_files:
                print(f"Warning: No JSON files found in {local_output_path}")
                return num_files
            
            for json_file in json_files:
                if not os.path.exists(json_file):
                    print(f"Warning: JSON file {json_file} does not exist")
                    continue
                # Normalize path for Hadoop (replace \ with /)
                normalized_json_file = json_file.replace('\\', '/')
                cmd = f"hadoop fs -put -f {normalized_json_file} {hdfs_output_path}"
                print(f"Executing: {cmd}")
                result = os.system(cmd)
                if result != 0:
                    raise RuntimeError(f"Failed to upload {json_file}")
            
            print(f"Uploaded {len(json_files)} JSON files to HDFS path {hdfs_output_path}")
        except Exception as e:
            print(f"Error uploading to HDFS: {e}")
            print("Files were converted locally but not uploaded to HDFS")
    
    return num_files

if __name__ == "__main__":  # Fixed typo in _name_ to __name__
    if len(sys.argv) < 3:
        print("Usage: python csv_to_json_converter.py <input_dir> <output_dir> [hdfs_path]")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    hdfs_path = sys.argv[3] if len(sys.argv) > 3 else None
    
    files_converted = convert_and_upload_to_hdfs(input_dir, output_dir, hdfs_path)
    print(f"Converted {files_converted} CSV files to JSON format")