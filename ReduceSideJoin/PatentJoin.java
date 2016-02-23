import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentJoin {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		

		 	Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Patent Data Problem Join");

	        job.setJarByClass(PatentJoin.class);
	        job.setReducerClass(JoinReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinCiteMapper.class);
	        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinRecordMapper.class);


	        FileSystem fs = FileSystem.get(conf);

	        if (fs.exists(new Path(args[2]))) {
	            fs.delete(new Path(args[2]), true);
	        }

	        FileOutputFormat.setOutputPath(job, new Path(args[2]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}


