package k3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
//            return 0;

        System.out.println("Start Partitioner");

        String key = text.toString();

        if(key.equals("clemson")){
            return 2;
        } else if(key.equals("miami")){
            return 1;
        }
        else{
            return 0;
        }

    }
}