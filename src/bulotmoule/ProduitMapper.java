package bulotmoule;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProduitMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.contains("PRODNUM")) {
            return;
        }

        String[] tokens = line.split(";");

        if (tokens.length >= 2) {
            String prodNum = tokens[0].replace("\"", "").trim(); 
            String prodLib = tokens[1].replace("\"", "").trim(); 

            if (prodLib.equalsIgnoreCase("Moules") || prodLib.equalsIgnoreCase("Bulots")) {
                
                outputKey.set(prodNum);
                
                outputValue.set("Produit," + prodLib);

                context.write(outputKey, outputValue);
            }
        }
    }
}