import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


//Reducer class that aggregates feature values and counts for each client.
public class ClientAggregationReducer extends Reducer<Text, Text, Text, Text> {
    // Reusable Text object
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;  // Sum of all feature values for this client (feature_value is decimal, e.g. 10.5)
        int count = 0;   // Total number of entries for this client

        // Iterate over all values for a given clientId
        for (Text val : values) {
            // Each value is in the format "feature_value,1"
            String[] valueParts = val.toString().split(",");

            // Add feature_value to sum (must use Double, values like 10.5 are not integers)
            sum += Double.parseDouble(valueParts[0]);

            // Increment count (always 1 for each record)
            count += Integer.parseInt(valueParts[1]);
        }

        // Prepare the final output as "Sum: X, Count: Y"
        result.set("Sum: " + sum + ", Count: " + count);

        // Write the result for the clientId
        context.write(key, result);
    }
}
