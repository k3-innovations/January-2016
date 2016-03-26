package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NgramRows
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2){
            System.err.println("2 arguments needed: <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Add rows to ngram file");

        job.setJarByClass(NgramRows.class);
        job.setMapperClass(RowMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class RowMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text k = new Text();
//        Text v = new Text();
        int row_num = 0;


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);

            row_num++;
            k.set("z_" + row_num);

            context.write(k,value);
        }
    }
}
