import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLink {

  public static class ReverseMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word1 = new Text();
    private Text word2 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String[] splited = itr.nextToken().split(" ");
        if (splited.length != 2) {
        	continue;
        }
        word1.set(splited[1]);
        word2.set(splited[0]);
        context.write(word1, word2);
      }
    }
  }

  public static class IdentityReducer
       extends Reducer<Text, Text, Text, Text> {

    Hashtable<String, String> hashtable = 
              new Hashtable<String, String>();

    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
      	String currVal = val.toString();
      	if (hashtable.containsKey(currVal)) {
      		continue;
      	}
      	hashtable.put(currVal, "exist");
        context.write(key, val);
      }
      
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WebLink");
    job.setJarByClass(WebLink.class);
    job.setMapperClass(ReverseMapper.class);
    job.setCombinerClass(IdentityReducer.class);
    job.setReducerClass(IdentityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}