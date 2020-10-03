// hashtags program to find the most repeated 10 hashtags in the training_tweets file by Renat Norderhaug
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class topten_hashtags {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
        // while the line has tokens
      while (itr.hasMoreTokens()) {

        word.set(itr.nextToken());
        String datecheck = word.toString();

        // reads input of string to see if it matches a hashtag
        if(datecheck.matches("#(?!\\d)\\w*")) {
          // if it is valid then write it to context
          context.write(word, one);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    private TreeMap<Integer, String> top_keys;

  
    public void setup(Context context) throws IOException, InterruptedException  {
      // declare a new tree map
      top_keys = new TreeMap<Integer, String>();
    }

    // reduce function override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // sum the values that have the same key
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      // put them into
      result.set(sum);
      top_keys.put(sum, key.toString());
    }
    // cleanup function overridden
    public void cleanup(Context context) throws IOException, InterruptedException {

      while(top_keys.size() > 10) {
        top_keys.remove(top_keys.firstKey());
      }
      // returns tree map values
      for(Map.Entry<Integer, String> entry : top_keys.entrySet()) {
        int count = entry.getKey();
        String name = entry.getValue();
        // write the values to context
	result.set(count);
        context.write(new Text(name), result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(topten_hashtags.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // set number of reduce tasks that run
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
