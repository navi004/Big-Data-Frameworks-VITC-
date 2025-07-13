import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Reduce Side Join Example for Employee and Salary Data
 */
public class rsp {

    // Mapper Class
    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Ensure valid record
            if (fields.length >= 3) {
                String recordType = fields[0].trim(); // "A" for Employee, "B" for Salary
                String joinKey = fields[1].trim();    // Employee ID (Join Key)
                String details = fields[2].trim();    // Employee Name or Salary

                context.write(new Text(joinKey), new Text(recordType + "," + details));
            }
        }
    }

    // Reducer Class
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String employeeName = null;
            String salary = null;

            // Iterate over values
            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens.length == 2) {
                    if (tokens[0].equals("A")) {
                        employeeName = tokens[1];  // Employee Name
                    } else if (tokens[0].equals("B")) {
                        salary = tokens[1];  // Employee Salary
                    }
                }
            }

            // Output only if both values exist
            if (employeeName != null && salary != null) {
                context.write(key, new Text(employeeName + ", " + salary));
            }
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ReduceSideJoin <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(rsp.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
