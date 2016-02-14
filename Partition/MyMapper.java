package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String option = conf.get("options");

        System.out.println(option);

        StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[-+.^:,';’()?!]",""));

        if(option.equals("noisewords")){
            while(itr.hasMoreTokens()){
                String tmp = itr.nextToken();
                if(tmp.equals("and") || tmp.equals("or") || tmp.equals("the") || tmp.equals("to") || tmp.equals("for") || tmp.equals("this") || tmp.equals("that") || tmp.equals("a") ){
                    word.set("noisewords");
                } else{
                    word.set(tmp);
                }
                context.write(word, one);
            }
        } else if(option.equals("total")){
            while(itr.hasMoreTokens()){
                itr.nextToken();
                word.set("total");
                context.write(word, one);
            }
        } else {
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }

        System.out.println("Finish map");
    }

}
