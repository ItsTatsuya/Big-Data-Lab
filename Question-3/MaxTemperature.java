import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
    
    // Mapper class
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            
            // Extract year and temperature
            String year = parts[0];
            int temperature = Integer.parseInt(parts[1]);
            
            context.write(new Text(year), new IntWritable(temperature));
        }
    }
    
    // Reducer class
    public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            
            // Find maximum temperature
            for (IntWritable value : values) {
                maxTemp = Math.max(maxTemp, value.get());
            }
            
            context.write(key, new IntWritable(maxTemp));
        }
    }
    
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temperature");
        
        job.setJarByClass(MaxTemperature.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}