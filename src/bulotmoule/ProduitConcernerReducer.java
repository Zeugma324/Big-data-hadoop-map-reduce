package bulotmoule;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProduitConcernerReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String prodLib = "Inconnu";
        
        ArrayList<String> listComNum = new ArrayList<>();

        for (Text val : values) {
            String valueStr = val.toString();
            String[] parts = valueStr.split(",");

            if (parts.length >= 2) {
                String tag = parts[0];

                if (tag.equals("Produit")) {
                    prodLib = parts[1];
                } 
                else if (tag.equals("Concerner")) {
                    listComNum.add(parts[1]);
                }
            }
        }

        outputValue.set(prodLib);

        for (String comNum : listComNum) {
            outputKey.set(comNum);
            
            context.write(outputKey, outputValue);
        }
    }
}