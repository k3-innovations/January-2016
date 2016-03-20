package TotalFlightTraffic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.hash.Hash;

public class TotalFlight {
	public static class TotalFilightMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static Text port = new Text();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] airPort = line.trim().split(",");
			String[] flt = new String[2];
			flt[0] = airPort[16];
			flt[1] = airPort[17];

			int i = 0;
			for (i = 0; i < flt.length; i++) {
				String str = flt[i];
				port = new Text(str);

				context.write(port, one);
			}

		}

	}

	public static class TotalFilightReducer extends Reducer<Text, IntWritable, Text, IntWritable>

	{
		public static Map<Text, IntWritable> port1 = new HashMap<>();

		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {

			int count = 0;

			for (IntWritable val : value) {
				count += val.get();
			}
			port1.put(new Text(key), new IntWritable(count));

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, IntWritable> map = new HashMap<>();
			map.putAll(port1);
			Set<Entry<Text, IntWritable>> set = map.entrySet();
			List<Entry<Text, IntWritable>> list = new ArrayList<Entry<Text, IntWritable>>(set);

			Collections.sort(list, new Comparator<Map.Entry<Text, IntWritable>>() {

				public int compare(Entry<Text, IntWritable> o1, Entry<Text, IntWritable> o2) {
					return (o2.getValue()).compareTo(o1.getValue());
				}

			});
			int top = 0;
			for (Map.Entry<Text, IntWritable> entry : list) {

				if ((top++ == 20)) {

					break;
				}

				context.write(entry.getKey(), entry.getValue());
			}

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Flight");

		job.setJarByClass(TotalFlight.class);
		job.setMapperClass(TotalFilightMapper.class);
		job.setReducerClass(TotalFilightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

