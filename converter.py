import json
import os

def convert_json_array_to_ndjson(input_filepath, output_filepath):
    with open(input_filepath, 'r') as f_in, open(output_filepath, 'w') as f_out:
        data = json.load(f_in)  # load full array
        for obj in data:
            f_out.write(json.dumps(obj) + '\n')

tweets_folder = "tweets"
for filename in os.listdir(tweets_folder):
    if filename.startswith("disaster_tweets_batch_") and filename.endswith(".json"):
        input_path = os.path.join(tweets_folder, filename)
        output_path = os.path.join(tweets_folder, "converted_" + filename)
        convert_json_array_to_ndjson(input_path, output_path)
        print(f"Converted {input_path} to {output_path}")
