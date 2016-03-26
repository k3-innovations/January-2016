package k3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class MsjMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> states = new HashMap<String, String>();
    private Text stateAbrv = new Text();
    private Text stateData = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        URI cacheFile = context.getCacheFiles()[0];
//        File file = new File(cacheFile.toString());
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String file = "";
        for (Path p : cacheFiles){
            if(p.getName().toString().trim().equals("states.txt")){
                file = p.toString();
            }
        }

        System.out.println(file);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        while(line != null){
            String[]  abbreviations = line.split("\t");
            states.put(abbreviations[0], abbreviations[1]);
            line = br.readLine();
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        String data = value.toString();
        String[] line = data.split("\t");
        String abrv = states.get(line[0]);

        stateAbrv.set(abrv);
        stateData.set(data);
        context.write(stateAbrv, stateData);
    }
}
