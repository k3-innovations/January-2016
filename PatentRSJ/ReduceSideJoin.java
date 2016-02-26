package k3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class ReduceSideJoin
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        // Check if all program arguments are passed.
        if(args.length != 3){
            System.err.println("3 arguments needed 1)citation-file 2)parent-info-file 3)output-folder");
            System.exit(-1);
        }

        // Get Reduce option from user
//        Scanner scan = new Scanner(System.in);
//        System.out.println("Menu: Select one...");
//        System.out.println("\t1 - Gather patent data with citations \n\t2 - Patent data based on countries (Returns Country, No. of Patents from) \n\t3 - Patents by grant year");
//        String option = scan.next();

        Configuration conf = new Configuration();
//        conf.set("option", option);
        Job job = Job.getInstance(conf, "Patent RSJ");

        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(PatentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CitationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, InfoMapper.class);


        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}