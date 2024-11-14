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
    
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year = new Text();
        private IntWritable temperature = new IntWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                try {
                    String[] parts = line.split(",");
                    if (parts.length == 2) {
                        year.set(parts[0]);
                        temperature.set(Integer.parseInt(parts[1].trim()));
                        context.write(year, temperature);
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed lines
                    context.getCounter("Temperature", "Malformed_Lines").increment(1);
                }
            }
        }
    }
    
    public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            
            for (IntWritable value : values) {
                maxTemp = Math.max(maxTemp, value.get());
            }
            
            result.set(maxTemp);
            context.write(key, result);
        }
    }
    
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