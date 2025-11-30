package bulotmoule;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalFilterReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text("Contient Moules et Bulots");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        boolean hasMoules = false;
        boolean hasBulots = false;

        for (Text val : values) {
            String prodLib = val.toString().trim();

            if (prodLib.equalsIgnoreCase("Moules")) {
                hasMoules = true;
            }
            if (prodLib.equalsIgnoreCase("Bulots")) {
                hasBulots = true;
            }
        }

        if (hasMoules && hasBulots) {
            context.write(key, outputValue);
        }
    }
}