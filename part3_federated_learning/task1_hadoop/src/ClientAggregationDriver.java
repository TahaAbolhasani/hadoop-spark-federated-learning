import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//This is the driver class, It sets up the Hadoop job configuration and kicks off the process.
public class ClientAggregationDriver {
    public static void main(String[] args) throws Exception {
        // Create a new Hadoop configuration
        Configuration conf = new Configuration();

        // Create a new job with a descriptive name
        Job job = Job.getInstance(conf, "Client Aggregation");

        // Set the main class for the job
        job.setJarByClass(ClientAggregationDriver.class);

        // Set the mapper and reducer classes that will be used
        job.setMapperClass(ClientAggregationMapper.class);
        job.setReducerClass(ClientAggregationReducer.class);

        // Specify the types of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Path to input data
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Path for output results

        // Submit the job and wait for it to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
