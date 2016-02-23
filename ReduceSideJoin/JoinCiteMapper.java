

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class JoinCiteMapper extends Mapper<LongWritable, Text, Text, Text> {

	    private static String[] citStr;
	    
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        citStr= value.toString().split(",");
	      
	        context.write(new Text(citStr[0]), new Text("cite\t"+citStr[1]));
	    
	}

	}

