package bulotmoule;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MagasinCommandeReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String magNom = "Inconnu"; 
        
        ArrayList<String> listComNum = new ArrayList<>();

        for (Text val : values) {
            String valueStr = val.toString();
            String[] parts = valueStr.split(",");

            if (parts.length >= 2) {
                String tag = parts[0];
                String data = parts[1];

                if (tag.equals("Magasin")) {
                    magNom = data;
                } else if (tag.equals("Commande")) {
                    listComNum.add(data);
                }
            }
        }

        outputKey.set(magNom);

        for (String comNum : listComNum) {
            outputValue.set(comNum);
            
            context.write(outputKey, outputValue);
        }
    }
}