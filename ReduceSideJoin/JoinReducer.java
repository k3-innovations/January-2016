import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private static Text rcdStr = new Text();
		private static String rcdYear;
	    private static String rcdDate;
	    private static Integer cntCite = 0; //to count number of times a record was cited
		
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		

	        for (Text val : values){
	            String[] value = val.toString().split("\t");

	            if(value[0].equals("cite"))
	            	cntCite++;
	            else if(value[0].equals("year"))
	            	rcdYear = value[1];
	            else 
	            	rcdDate = value[1];
            }
	        
	        rcdStr.set(cntCite.toString() + " " + rcdYear + " " + rcdDate );
	        context.write(key, rcdStr);
       
    }
}
