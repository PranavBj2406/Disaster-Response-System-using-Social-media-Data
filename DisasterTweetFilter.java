import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONObject;

public class DisasterTweetFilter {

    public static class DisasterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> disasterKeywords = new HashSet<>(Arrays.asList(
                "flood", "earthquake", "hurricane", "wildfire", "tsunami", "tornado", "landslide"));
        
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                
                // Parse the JSON line
                JSONObject tweet = new JSONObject(line);
                String tweetText = tweet.getString("text").toLowerCase();
                String location = tweet.getString("location");
                String disasterType = tweet.getString("disaster_type");
                
                // Check if tweet contains disaster keywords
                boolean relevantTweet = false;
                for (String keyword : disasterKeywords) {
                    if (tweetText.contains(keyword)) {
                        relevantTweet = true;
                        break;
                    }
                }
                
                // If severity is high (4 or 5) or has high retweet count (>100), consider it relevant
                if (tweet.has("severity") && tweet.getInt("severity") >= 4) {
                    relevantTweet = true;
                }
                
                if (tweet.has("retweet_count") && tweet.getInt("retweet_count") > 100) {
                    relevantTweet = true;
                }
                
                // If tweet is from verified source, consider it relevant
                if (tweet.has("verified_report") && tweet.getBoolean("verified_report")) {
                    relevantTweet = true;
                }
                
                if (relevantTweet) {
                    // Use location and disaster type as key
                    outputKey.set(location + ":" + disasterType);
                    outputValue.set(tweet.toString());
                    context.write(outputKey, outputValue);
                }
            } catch (Exception e) {
                // Log error but continue processing
                System.err.println("Error processing tweet: " + e.getMessage());
            }
        }
    }

    public static class DisasterReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Calculate average severity and count for this location/disaster
            int count = 0;
            double severitySum = 0;
            int verified = 0;
            String mostRecent = null;
            long latestTimestamp = 0;
            
            for (Text val : values) {
                try {
                    JSONObject tweet = new JSONObject(val.toString());
                    count++;
                    
                    if (tweet.has("severity")) {
                        severitySum += tweet.getInt("severity");
                    }
                    
                    if (tweet.has("verified_report") && tweet.getBoolean("verified_report")) {
                        verified++;
                    }
                    
                    // Keep track of most recent tweet
                    String timestamp = tweet.getString("timestamp");
                    // Simple string comparison works for ISO format timestamps
                    if (mostRecent == null || timestamp.compareTo(mostRecent) > 0) {
                        mostRecent = timestamp;
                        latestTimestamp = System.currentTimeMillis(); // For alert threshold
                    }
                } catch (Exception e) {
                    System.err.println("Error in reducer: " + e.getMessage());
                }
            }
            
            double avgSeverity = count > 0 ? severitySum / count : 0;
            
            // Create a summary object
            JSONObject summary = new JSONObject();
            String[] parts = key.toString().split(":");
            summary.put("location", parts[0]);
            summary.put("disaster_type", parts[1]);
            summary.put("count", count);
            summary.put("avg_severity", avgSeverity);
            summary.put("verified_reports", verified);
            summary.put("last_updated", mostRecent);
            
            // Calculate alert level (1-3) based on severity, count, and verified reports
            int alertLevel = 1; // Default is low
            if (avgSeverity >= 4 || count > 5 || verified >= 2) {
                alertLevel = 3; // High alert
            } else if (avgSeverity >= 3 || count > 2 || verified >= 1) {
                alertLevel = 2; // Medium alert
            }
            summary.put("alert_level", alertLevel);
            
            result.set(summary.toString());
            context.write(new Text(key), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: DisasterTweetFilter <input> <output>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "disaster tweet filter");
        job.setJarByClass(DisasterTweetFilter.class);
        
        job.setMapperClass(DisasterMapper.class);
        job.setReducerClass(DisasterReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}