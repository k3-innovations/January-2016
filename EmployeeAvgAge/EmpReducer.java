package k3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EmpReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        System.out.println(key);
        float count = 0;
        float total = 0;
        for(IntWritable year : values){
            System.out.println("Year: " + year.get());
            total += 2016 - year.get();
            count++;

            System.out.println("Total: " + total + "\n" + "Count: " + count);
        }

        float average = total/count;
        System.out.println("Average for " + key + ": " + average);

        context.write(key, new FloatWritable(average));
    }
}

