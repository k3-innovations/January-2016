package k3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by faisal on 2/17/16.
 */
public class PatentReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

//        System.out.println("in reduce");
        ArrayList<String> patentsList = new ArrayList<String>();
        String gyear = "";
        String country = "";

        for (Text val : values){
            String[] value = val.toString().split("\t");

            if(value[0].equals("cite")) {
                if (!patentsList.contains(val.toString())) {
                    patentsList.add(value[1]);
                }
            } else if(value[0].equals("gyear")){
                gyear = value[1];
            } else{
                country = value[1];
            }
        }

        result.set(country + " " + gyear + " " + " " + patentsList.size() + " " + patentsList.toString());
        context.write(key, result);
    }

}
