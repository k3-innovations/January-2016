package k3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();


    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        System.out.println("Start reducer");
        int sum = 0;

        for (IntWritable val : values){
            sum += val.get();
        }

        result.set(sum);
        context.write(key, result);

        System.out.println("Finish reduce");
    }
}