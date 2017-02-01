package Airlines;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class thirdReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {
    int highest;
    String str;

    public thirdReducer() {
        this.str = "";
        this.highest = 0;
    }

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += ((IntWritable) values.next()).get();
        }
        String s = key.toString();
        if (s.equals(Character.toString('\u00fe'))) {
            output.collect(new Text("The most common reason for flights cancellations"), new FloatWritable());
            if (this.str.equals("A")) {
                output.collect(new Text("Carrier"), new FloatWritable((float) this.highest));
            }
            if (this.str.equals("B")) {
                output.collect(new Text("Weather"), new FloatWritable((float) this.highest));
            }
            if (this.str.equals("C")) {
                output.collect(new Text("NAS"), new FloatWritable((float) this.highest));
            }
            if (this.str.equals("D")) {
                output.collect(new Text("Security"), new FloatWritable((float) this.highest));
            }
        }
        if (sum > this.highest) {
            this.highest = sum;
            this.str = s;
        }
    }
}