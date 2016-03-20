package k3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class AirportRankReducer extends Reducer<LongWritable, Text, Text, LongWritable> {


    private final int COUNT = 20;
    private int counter = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        if(counter <= COUNT){
            for (Text val: values){
                context.write(new Text(counter + ". " + val), key);
            }
            counter++;
        }
    }
}
