package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by faisal on 2/14/16.
 */
public class InfoMapper extends Mapper<LongWritable, Text, Text, Text>{

    private static String[] info;
    private static Text k = new Text();
    private static Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        Configuration conf = context.getConfiguration();
        String option = conf.get("option");

        System.out.println("in info map");
        info = value.toString().split(",");



            k.set(info[0]);

            v.set("gyear\t"+info[1]);
            context.write(k, v);

//            v.set("claims\t" + info[8]);
//            context.write(k, v);

            v.set("country\t" + info[4]);
            context.write(k, v);
    }
}
