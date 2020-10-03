/*
Renat Norderhaug, CS 433 Assignment #1
4/8/20
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

public class Q6 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Q6 <input_file> <output_file>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Q6_CS433");
        job.setJarByClass(Q6.class);

        job.setMapperClass(Q6Mapper.class);
        job.setReducerClass(Q6Reducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Q6Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, Integer> dateCountMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String  tweet  =  value.toString();
                if(!tweet.trim().equals("")) {
					String[] tokens = tweet.split("\\t");
                    StringTokenizer itr = new StringTokenizer(tokens[3]);

                    while (itr.hasMoreTokens()) {
                        String word = itr.nextToken().trim();
                        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
                        Matcher matcher = pattern.matcher(word);

                        if (matcher.matches()) {
                            String date = matcher.group(0);
                            if (dateCountMap.containsKey(date)) {
                                dateCountMap.put(date, dateCountMap.get(date)+1);
                            }
                            else {
                                dateCountMap.put(date, 1);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String key: dateCountMap.keySet()) {
                context.write(new Text(key), new IntWritable(dateCountMap.get(key)));
            }
        }
    }

    public static class Q6Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Text, IntWritable> dateCountMap = new HashMap<>();

        public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();
// compares the values
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            dateCountMap.put(new Text(key), new IntWritable(sum));
        }
// overriding the cleanup functionality
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = sortByValues(dateCountMap);

            int counter = 0;
            // make sure its not past twenty
            for (Text key: sortedMap.keySet()) {
                if (counter ++ == 20) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }
}
