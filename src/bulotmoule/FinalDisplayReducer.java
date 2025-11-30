package bulotmoule;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalDisplayReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        String nomMagasin = null;
        boolean isValidOrder = false;

        for (Text val : values) {
            String valStr = val.toString();

            if (valStr.startsWith("Magasin,")) {
                nomMagasin = valStr.split(",")[1];
            } 
            else if (valStr.contains("Contient Moules et Bulots")) {
                isValidOrder = true;
            }
        }

        if (nomMagasin != null && isValidOrder) {
            
            outputValue.set(nomMagasin);
            context.write(key, outputValue);
        }
    }
}