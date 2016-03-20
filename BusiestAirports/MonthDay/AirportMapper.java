package k3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();
    String airport1;
    String airport2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        String[] row = value.toString().split(",");
        k.set(row[3]);
        airport1 = row[16];
        v.set(airport1);
        context.write(k,v);
        airport2 =  row[17];
        v.set(airport2);
        context.write(k,v);
    }
}
