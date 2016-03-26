package k3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by faisal on 2/18/16.
 */
public class MsjReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        String data= "";

        for (Text val : values){
            data += val.toString() + "\t";
        }

        context.write(key, new Text(data));
    }

}
