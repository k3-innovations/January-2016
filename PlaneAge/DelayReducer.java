package k3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        int sum = 0;
        for (IntWritable val : values){
            sum += 1;
        }

        v.set(sum);
        context.write(key,v);
    }
}
