package Airlines;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class firstReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {
    float avg;
    int f;
    String[][] flightData;
    int k;

    public firstReducer() {
        this.flightData = (String[][]) Array.newInstance(String.class, new int[]{6, 2});
        this.avg = 0.0f;
        this.f = 0;
        this.k = 0;
    }

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        int i;
        float sum = 0.0f;
        int count = 0;
        while (values.hasNext()) {
            sum += (float) ((IntWritable) values.next()).get();
            count++;
        }
        this.avg = sum / ((float) count);
        String s = key.toString();
        if (this.f == 0) {
            for (i = 0; i < 6; i++) {
                this.flightData[i][1] = "0";
                this.flightData[i][0] = " ";
            }
            this.f = 1;
        }
        if (s.equals(Character.toString('\u00fe'))) {
            for (i = 0; i < 6; i++) {
                if (i == 0) {
                    output.collect(new Text("Highest probabilty of airlines on schedule"), new FloatWritable());
                } else if (i == 3) {
                    output.collect(new Text("Lowest probabilty of airlines on schedule"), new FloatWritable());
                }
                output.collect(new Text(this.flightData[i][0]), new FloatWritable(Float.parseFloat(this.flightData[i][1])));
            }
        } else if (this.avg > Float.parseFloat(this.flightData[0][1])) {
            this.flightData[2][1] = this.flightData[1][1];
            this.flightData[2][0] = this.flightData[1][0];
            this.flightData[1][1] = this.flightData[0][1];
            this.flightData[1][0] = this.flightData[0][0];
            this.flightData[0][1] = String.valueOf(this.avg);
            this.flightData[0][0] = s.replace(" in", "");
        } else if (this.avg > Float.parseFloat(this.flightData[1][1])) {
            this.flightData[2][1] = this.flightData[1][1];
            this.flightData[2][0] = this.flightData[1][0];
            this.flightData[1][1] = String.valueOf(this.avg);
            this.flightData[1][0] = s.replace(" in", "");
        } else if (this.avg > Float.parseFloat(this.flightData[2][1])) {
            this.flightData[2][1] = String.valueOf(this.avg);
            this.flightData[2][0] = s.replace(" in", "");
        }
        if (this.avg < Float.parseFloat(this.flightData[3][1]) || this.k == 0) {
            this.flightData[5][1] = this.flightData[4][1];
            this.flightData[5][0] = this.flightData[4][0];
            this.flightData[4][1] = this.flightData[3][1];
            this.flightData[4][0] = this.flightData[3][0];
            this.flightData[3][1] = String.valueOf(this.avg);
            this.flightData[3][0] = s.replace(" in", "");
            this.k = 1;
        } else if (this.avg < Float.parseFloat(this.flightData[4][1])) {
            this.flightData[5][1] = this.flightData[4][1];
            this.flightData[5][0] = this.flightData[4][0];
            this.flightData[4][1] = String.valueOf(this.avg);
            this.flightData[4][0] = s.replace(" in", "");
        } else if (this.avg < Float.parseFloat(this.flightData[5][1])) {
            this.flightData[5][1] = String.valueOf(this.avg);
            this.flightData[5][0] = s.replace(" in", "");
        }
    }
}