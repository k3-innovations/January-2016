package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class AirportDailyCounter
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 3){
            System.err.println("3 arguments needed: <airport-data> <flight-data> <final-output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        /* Job 1 - Get total traffic for airports */
        Job job1 = Job.getInstance(conf, "Busiest Airports");

        job1.setJarByClass(AirportDailyCounter.class);
        job1.setMapperClass(DailyMapper.class);
        job1.setReducerClass(DailyReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

    public static class DailyMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);

            String[] data = value.toString().split("\t");
            k.set(data[1]);
            v.set(data[0]);
            context.write(k,v);

        }
    }


    public static class DailyReducer extends Reducer<Text, Text, Text, Text>{

//        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);

            int sumIn = 0;
            int sumOut = 0;
            String val;
            for (Text value : values){
                val = value.toString();
                if(val.equals("out")){
                    sumOut++;
                } else{
                    sumIn++;
                }
            }

            v.set("Inbound:" + sumIn + "\tOutbound:" + sumOut);
            context.write(key, v);

        }
    }
}
