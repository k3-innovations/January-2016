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
import java.util.Map;


public class EmpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<String, String> departments = new HashMap<String, String>();
    private Text k = new Text();
    private IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String file = cacheFiles[0].toString();

        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        while(line != null){
            String[]  dept = line.split(",");
            departments.put(dept[0], dept[1]);
            line = br.readLine();
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
        String[] data = line.split(",");
        int birthYear = Integer.parseInt(data[1].split("-")[0]);
//        System.out.println("birthyear: " + birthYear);
        k.set(departments.get(data[2]));
        v.set(birthYear);
        context.write(k, v);
    }
}
