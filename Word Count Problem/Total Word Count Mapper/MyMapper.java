package k3.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
	
	 public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	        
   	  //This just modifies the word count program a little bit and counts total number of words instead...
     private final static IntWritable one = new IntWritable(1);
       private static Text word = new Text();        
       
       @Override
       protected void map(LongWritable key, Text value, Context    
  context) throws IOException, InterruptedException {

        //getting rid of special characters. step Credit to Faisal!
       	
       String str = new String(value.toString().replaceAll("[-+.^:,';’()?!]",""));
       StringTokenizer itr = new StringTokenizer(str); 
          
          while(itr.hasMoreTokens()){
       	   String tmp = itr.nextToken();
       	   //check if token contains noise words. If true, just continue iteration. Step Credited to Faisal
              if(tmp.equals("and") || tmp.equals("or") || tmp.equals("the") || tmp.equals("to") || tmp.equals("for") || tmp.equals("this") || tmp.equals("that") || tmp.equals("a") )
              		continue;
              else
           	   //set all words in the token to a String and pass it to context object
           	   word.set("Total_Words:");
               context.write(word, one);
              }
       
   }
}
