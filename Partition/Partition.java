package k3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Partition
{

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {

        Configuration conf = new Configuration();

        if(args.length == 3){
            conf.set("options", args[2]);
        } else {
            conf.set("options", "none");
        }

        Job job = Job.getInstance(conf, "Word Count example");

        job.setJarByClass(Partition.class);
        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
