package k3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


public class AirportReducer extends Reducer<Text, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        HashMap<String, Integer> totals = new HashMap<String, Integer>();

        for (Text val : values){
            String[] data = val.toString().split("\t");
            if (totals.containsKey(data[0])){
                int total = totals.get(data[0]);
                totals.put(data[0], total + 1);
            } else{
                totals.put(data[0], 1);
            }
        }

        for (String keys: totals.keySet()){
            k.set(keys);
            v.set(totals.get(keys));
            context.write(k,v);
        }
    }
}
