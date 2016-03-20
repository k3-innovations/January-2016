package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Scanner;


public class AirportsMD extends Configured implements Tool
{
    public int run(String[] args) throws Exception {

        /* Get user input from option menu */
        System.out.println("Menu: Get Top 20 by...\n 1: Month \n 2. Day ");
        System.out.print("Enter you selection: ");
        Scanner scan = new Scanner(System.in);
        String option = scan.next();

        Configuration conf = new Configuration();
        conf.set("option", option); // Save user option for use in mapper

        /* Job 1 - Get total traffic for airports */
        Job job1 = Job.getInstance(conf, "Busiest Airports by Month/Day");
        job1.setJarByClass(AirportsMD.class);

        /* Map settings */
        job1.setMapperClass(AirportMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        /* Reduce settings */
        job1.setReducerClass(AirportReducer.class);

        /* Option 2 - Day */
        if(option.equals("2")){
            job1.setPartitionerClass(DayPartioner.class);
            job1.setNumReduceTasks(7);
        }
        /* Option 1 - Month */
        if (option.equals("1")){
            job1.setPartitionerClass(MonthPartioner.class);
            job1.setNumReduceTasks(12);
        }
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        return job1.waitForCompletion(true) ? 0 : 1;
    }


    public static void main( String[] args ) throws Exception {
        if(args.length != 4){
            System.err.println("3 arguments needed: <airport-data> <flight-data> <job1-output> <final-output>");
            System.exit(-1);
        }

        ToolRunner.run(new Configuration(), new AirportsMD(), args);
    }

    /* Partition by Day value */
    public static class DayPartioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {


            String k = key.toString();
            if(k.equals("1")){
                return 0; //% numPartitions;
            } else if (k.equals("2")){
                return 1; //% numPartitions;
            }else if (k.equals("3")){
                return 2; //% numPartitions;
            }else if (k.equals("4")){
                return 3; //% numPartitions;
            }else if (k.equals("5")){
                return 4; //% numPartitions;
            }else if (k.equals("6")){
                return 5; //% numPartitions;
            }else if (k.equals("7")){
                return 6; //% numPartitions;
            } else{
                return 0;
            }

        }
    }

    /* Partition by month value */
    public static class MonthPartioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {

            String k = key.toString();
            if(k.equals("1")){
                return 0; //% numPartitions;
            } else if (k.equals("2")){
                return 1; //% numPartitions;
            }else if (k.equals("3")){
                return 2; //% numPartitions;
            }else if (k.equals("4")){
                return 3; //% numPartitions;
            }else if (k.equals("5")){
                return 4; //% numPartitions;
            }else if (k.equals("6")){
                return 5; //% numPartitions;
            }else if (k.equals("7")){
                return 6; //% numPartitions;
            }else if (k.equals("8")){
                return 7; //% numPartitions;
            }else if (k.equals("9")){
                return 8; //% numPartitions;
            }else if (k.equals("10")){
                return 9; //% numPartitions;
            }else if (k.equals("11")){
                return 10; //% numPartitions;
            }else if (k.equals("12")){
                return 11; //% numPartitions;
            }
            else{
                return 0;
            }

        }
    }
}
