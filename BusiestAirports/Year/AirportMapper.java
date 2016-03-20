package k3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;



public class AirportMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//    private HashMap<String, String> airports = new HashMap<String, String>();
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
////        super.setup(context);
//
//        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//        String file = cacheFiles[0].toString();
//
//        BufferedReader br = new BufferedReader(new FileReader(file));
//        String line = br.readLine();
//        line = br.readLine();
//        while(line != null){
//            String[]  data = line.split(",");
//            airports.put(data[0].replaceAll("\"", ""), data[1].replaceAll("\"",""));
//            line = br.readLine();
//        }
//        br.close();
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        String[] line = value.toString().split(",");

        k.set(line[16]);
        context.write(k,v);

        k.set(line[17]);
        context.write(k,v);

    }
}
