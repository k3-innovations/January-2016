package k3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayRankMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    LongWritable k = new LongWritable();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String[] line = value.toString().split("\t");

        k.set(Integer.parseInt(line[1]));
        v.set(line[0]);
        context.write(k,v);
    }
}
