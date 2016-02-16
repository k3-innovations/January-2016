package Parttition;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, IntWritable> {
	    @Override
	    public int getPartition(Text passedWord, IntWritable one, int numReduceTasks) {
//		            return 0;
	    	
	    	//partitioner

	        String keyWord = passedWord.toString();
	        boolean isUpperCase = Character.isUpperCase(keyWord.charAt(0));
	        
	        if(numReduceTasks == 0)
	           return 0;
	        if(isUpperCase == true)
	            return 0 % numReduceTasks;
	        else
	            return 1 % numReduceTasks;
	        
	    }
}
