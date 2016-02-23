

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class JoinRecordMapper extends Mapper<LongWritable, Text, Text, Text> {

	    private static String[] rcdStr;
	  

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	rcdStr= value.toString().split(",");
	        context.write(new Text(rcdStr[0]),new Text("year\t"+rcdStr[1]));
	        context.write(new Text(rcdStr[0]),new Text("date\t"+rcdStr[2]));
	        //dat.set(record[2] + "\t");
	        //context.write(pat,dat);
	    }
	}
