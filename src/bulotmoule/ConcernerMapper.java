package bulotmoule;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConcernerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("COMNUM") || line.startsWith("PRODNUM")) {
            return;
        }

        String[] tokens = line.split(";");
        
        if (tokens.length >= 3) {
            String comNum = tokens[0].trim();
            String prodNum = tokens[1].trim();
            String qteCom = tokens[2].trim();

            outputKey.set(prodNum);

            outputValue.set("Concerner," + comNum + "," + qteCom);

            context.write(outputKey, outputValue);
        }
    }
}