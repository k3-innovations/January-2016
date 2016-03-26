package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class MapSideJoin {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // Check if all program arguments are passed.
        if(args.length != 3){
            System.err.println("3 arguments needed: <states> <data> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "State Data MSJ");

        // Distributed Cache File for MSJ
//        Path cacheFile = new Path(args[0]);
//        job.addCacheFile(cacheFile.toUri());
        DistributedCache.addCacheFile(new URI(args[0]), job.getConfiguration());


        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(MsjMapper.class);
        job.setReducerClass(MsjReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
