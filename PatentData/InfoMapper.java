package k3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by faisal on 2/14/16.
 */
public class InfoMapper extends Mapper<LongWritable, Text, Text, Text>{

    private static String[] info;
    private static Text patent = new Text();
    private static Text data = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        System.out.println("in info map");
        info = value.toString().split(",");

        patent.set(info[0]);

        data.set("gyear\t"+info[1]);
        context.write(patent, data);

        data.set("gdate\t" + info[2]);
        context.write(patent, data);
    }
}
