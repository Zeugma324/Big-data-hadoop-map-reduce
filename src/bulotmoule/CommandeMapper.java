package bulotmoule;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommandeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        
        if (line.contains("COMNUM")) {
            return;
        }
        
        String[] tokens = line.split(";");

        
        if (tokens.length >= 2) {
            
            String comNum = tokens[0].replace("\"", "").trim();
            String magNum = tokens[1].replace("\"", "").trim();
            
            outputKey.set(magNum);

            outputValue.set("Commande," + comNum);

            context.write(outputKey, outputValue);
        }
    }
}