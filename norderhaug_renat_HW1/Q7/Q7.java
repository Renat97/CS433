/*
Renat Norderhaug, Question #7 of Homework assignment #1 for class CS433
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;
import java.util.regex.*;
import java.io.IOException;
// setting up basing config
public class Q7 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Q7 <input_file_tweets> <input_file_users> <output_file>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Q7_CS433");
        job.setJarByClass(Q7.class);

        job.setReducerClass(Q7Reducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Q7MapperTweets.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Q7MapperUsers.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Q7MapperTweets extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String tweet = value.toString();
                if(!tweet.trim().equals("")) {
                    String[] tokens = tweet.split("\\t");
                    String user = tokens[0].trim();
                    context.write(new Text(user), new Text("1"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Q7MapperUsers extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String  tweet  =  value.toString();
                if(!tweet.trim().equals("")) {
                    String[] tokens = tweet.split("\\t");
                    String user = tokens[0].trim();
                    String city = tokens[1].split(",")[0].trim();
                    context.write(new Text(user), new Text(city));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Q7Reducer extends Reducer<Text, Text, Text, IntWritable> {
        private Map<Text, IntWritable> cityMap = new HashMap<>();

        public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();
// sorting the key entries part of the reducer tasks
            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
                @Override
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text city = new Text();

            for (Text val : values) {
                String value = val.toString();
                if (value.matches("\\d+")) {
                    sum += Integer.valueOf(value);
                } else {
                    city = new Text(value);
                }
            }

            if (cityMap.containsKey(city)) {
                sum += cityMap.get(city).get();
            }

            cityMap.put(city, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = sortByValues(cityMap);

            int counter = 0;
            for (Text key: sortedMap.keySet()) {
                if (counter ++ == 11) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }
}
