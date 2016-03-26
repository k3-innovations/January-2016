package k3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;


public class AirportInOutDay
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 3){
            System.err.println("3 arguments needed: <airport-data> <flight-data> <final-output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        /* Job 1 - Get total traffic for airports */
        Job job1 = Job.getInstance(conf, "Busiest Airports");

        job1.setJarByClass(AirportInOutDay.class);
        job1.setMapperClass(DayMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(DayReducer.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);

        MultipleOutputs.addNamedOutput(job1, "sunday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "monday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "tuesday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "wednesday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "thursday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "friday", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "saturday", TextOutputFormat.class, Text.class, Text.class);


        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

    /* Day Mapper */
    public static class DayMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);

            String[] data = value.toString().split(",");

            k.set(data[3]);
            v.set("out\t" + data[16]);
            context.write(k,v);

            k.set(data[3]);
            v.set("in\t" + data[17]);
            context.write(k,v);

        }
    }

    /* Day Reducer */
    public static class DayReducer extends Reducer<Text, Text, Text, Text>{

        private Text k = new Text();
        private Text v = new Text();
        private HashMap<String, String> days = new HashMap<String, String>();

        MultipleOutputs<Text, Text> output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
            output = new MultipleOutputs(context);

            days.put("1","sunday");
            days.put("2","monday");
            days.put("3","tuesday");
            days.put("4","wednesday");
            days.put("5","thursday");
            days.put("6","friday");
            days.put("7","saturday");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);

            System.out.println("Key: " + key);
            System.out.println("days: " + days.get(key.toString()));

            if(!key.toString().equals("DayOfWeek")){
                String[] val;
                for (Text value : values){
                    val = value.toString().split("\t");
                    k.set(val[0]);
                    v.set(val[1]);
                    output.write(days.get(key.toString()), k, v);

                }
            }

        }

    }
}
