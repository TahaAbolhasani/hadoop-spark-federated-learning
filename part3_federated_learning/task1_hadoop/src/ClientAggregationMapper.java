import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


//Mapper class for processing each line of input data, Extracts the client ID and feature value, and prepares them for aggregation.
public class ClientAggregationMapper extends Mapper<Object, Text, Text, Text> {
    // Reusable objects
    private Text clientId = new Text();
    private Text featureValueWithCount = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input line to a String
        String line = value.toString();

        // Skip header line if present
        if (line.startsWith("client_id")) {
            return;
        }

        // Split the line based on commas
        String[] fields = line.split(",");

        // Check if the line has at least two fields (client_id and feature_value)
        if (fields.length >= 2) {
            // Set clientId to the first field (client_id)
            clientId.set(fields[0]);

            // Create a combined value of feature_value and a count of 1 (to be used in Reducer)
            featureValueWithCount.set(fields[1] + ",1");

            // Emit the clientId as key and the featureValue,1 as value
            context.write(clientId, featureValueWithCount);
        }
    }
}
