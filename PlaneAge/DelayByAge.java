package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class DelayByAge extends Configured implements Tool {
    public int run(String[] args) throws Exception {


        Configuration conf = new Configuration();

        /* Job 1 - Get total traffic for airports */
        Job job1 = Job.getInstance(conf, "Delays by Plane Age");
        DistributedCache.addCacheFile(new URI(args[1]), job1.getConfiguration());


        job1.setJarByClass(DelayByAge.class);

        /* Map settings */
        job1.setMapperClass(DelayMapper.class);
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(IntWritable.class);

        /* Reduce settings */
        job1.setReducerClass(DelayReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);


        /* Job 2 for sorting and getting top 20 */
        Job job2 = new Job(conf, "Sort by total delays");
//        DistributedCache.addCacheFile(new URI(args[0]), job2.getConfiguration());

        job2.setJarByClass(DelayByAge.class);
        job2.setMapperClass(DelayRankMapper.class);
        job2.setReducerClass(DelayRankReducer.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[2]));

        if (fs.exists(new Path(args[3]))) {
            fs.delete(new Path(args[3]), true);
        }

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));


        return (job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("4 arguments needed: <airport-data> <plane-data> <tmp-output> <final-output>");
            System.exit(-1);
        }

        ToolRunner.run(new Configuration(), new DelayByAge(), args);
    }
}
