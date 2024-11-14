import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class MatrixMultiplication {
    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return; // Skip empty lines
            }
            
            try {
                String[] tokens = line.split(",");
                if (tokens.length != 4) {
                    context.getCounter("Matrix", "Malformed_Lines").increment(1);
                    return;
                }
                
                String matrix = tokens[0];
                int row = Integer.parseInt(tokens[1]);
                int col = Integer.parseInt(tokens[2]);
                int val = Integer.parseInt(tokens[3]);

                Configuration conf = context.getConfiguration();
                int m = Integer.parseInt(conf.get("m", "2")); 

                if (matrix.equals("A")) {
                    for (int k = 0; k < m; k++) {
                        context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
                    }
                } else if (matrix.equals("B")) {
                    for (int i = 0; i < m; i++) {
                        context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
                    }
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                context.getCounter("Matrix", "Error_Lines").increment(1);
                return;
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] A = new int[10]; // Assuming max 10x10 matrices
            int[] B = new int[10];
            
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                int pos = Integer.parseInt(parts[1]);
                int value = Integer.parseInt(parts[2]);
                
                if (parts[0].equals("A")) {
                    A[pos] = value;
                } else {
                    B[pos] = value;
                }
            }

            int result = 0;
            for (int i = 0; i < 10; i++) {
                result += A[i] * B[i];
            }
            if (result != 0) {
                context.write(key, new IntWritable(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("m", "2"); // 2x2 matrices

        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}