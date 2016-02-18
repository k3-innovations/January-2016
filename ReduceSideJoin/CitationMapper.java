package k3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by faisal on 2/14/16.
 */
public class CitationMapper extends Mapper<LongWritable, Text, Text, Text> {

// For storing keys and values.
    private static String[] citation;
    private static Text patent = new Text();
    private static Text referrer = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        System.out.println("in citation map");

        citation = value.toString().split(",");

        System.out.println("Citation array size: " + citation.length);
        System.out.println("[0] " + citation[0]);
        System.out.println("[1] " + citation[1]);

        patent.set(citation[1]);
        referrer.set("cite\t"+citation[0]);

        context.write(patent, referrer);
    }
}
