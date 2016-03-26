package k3;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
//import java.net.URI;
import java.net.URISyntaxException;

public class Airports
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if(args.length != 4){
            System.err.println("3 arguments needed: <airport-data> <flight-data> <job1-output> <final-output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        /* Job 1 - Get total traffic for airports */
        Job job1 = Job.getInstance(conf, "Busiest Airports");

        job1.setJarByClass(Airports.class);
        job1.setMapperClass(AirportMapper.class);
        job1.setReducerClass(AirportReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);


        /* Job 2 for sorting and getting top 20 */
        Job job2 = new Job(conf, "Sort top busiest airports 20");
//        DistributedCache.addCacheFile(new URI(args[0]), job2.getConfiguration());

        job2.setJarByClass(Airports.class);
        job2.setMapperClass(AirportRankMapper.class);
        job2.setReducerClass(AirportRankReducer.class);
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


        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
