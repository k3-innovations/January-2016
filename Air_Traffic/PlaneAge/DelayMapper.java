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
import java.util.Arrays;


public class DelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, String> plane_data = new HashMap<String, String>();
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String file = cacheFiles[0].toString();

//        System.out.println(file);

        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        String[] year;
        while(line != null){
            String[] plane_age = line.split(",");
            if(plane_age.length>1){
                year = plane_age[3].split("/");
                if(year.length>1){
                    plane_data.put(plane_age[0], year[2]);
                }
//                System.out.println(Arrays.toString(year));
            } else{
                plane_data.put(plane_age[0], "*");
            }
            line = br.readLine();
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String[] line = value.toString().split(",");

        if(!line[14].equals("ArrDelay") && !line[14].equals("NA") && !line[10].equals("NA") && !line[10].equals("TailNum")){
            int delay = Integer.parseInt(line[14]);
            if(delay>=15){
//                System.out.println(line[10]);
                if(plane_data.containsKey(line[10])){
                    k.set(plane_data.get(line[10]));
                    context.write(k,v);
                }

            }
        }

    }
}
