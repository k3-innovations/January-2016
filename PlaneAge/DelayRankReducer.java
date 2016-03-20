package k3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayRankReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        for (Text val : values){
            context.write(val, key);
        }
    }
}
