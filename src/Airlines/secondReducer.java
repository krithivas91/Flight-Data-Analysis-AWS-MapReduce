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

public class secondReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {
    float average;
    int f;
    int k;
    int l;
    String[][] taxiData;

    public secondReducer() {
        this.taxiData = (String[][]) Array.newInstance(String.class, new int[]{12, 2});
        this.average = 0.0f;
        this.f = 0;
        this.k = 0;
        this.l = 0;
    }

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        int i;
        float sum = 0.0f;
        int count = 0;
        while (values.hasNext()) {
            sum += (float) ((IntWritable) values.next()).get();
            count++;
        }
        if (this.f == 0) {
            for (i = 0; i < 12; i++) {
                this.taxiData[i][1] = "0";
                this.taxiData[i][0] = " ";
            }
            this.f = 1;
        }
        this.average = sum / ((float) count);
        String s = key.toString();
        if (s.contains(" in")) {
            if (this.average > Float.parseFloat(this.taxiData[0][1])) {
                this.taxiData[2][1] = this.taxiData[1][1];
                this.taxiData[2][0] = this.taxiData[1][0];
                this.taxiData[1][1] = this.taxiData[0][1];
                this.taxiData[1][0] = this.taxiData[0][0];
                this.taxiData[0][1] = String.valueOf(this.average);
                this.taxiData[0][0] = s.replace(" in", "");
            } else if (this.average > Float.parseFloat(this.taxiData[1][1])) {
                this.taxiData[2][1] = this.taxiData[1][1];
                this.taxiData[2][0] = this.taxiData[1][0];
                this.taxiData[1][1] = String.valueOf(this.average);
                this.taxiData[1][0] = s.replace(" in", "");
            } else if (this.average > Float.parseFloat(this.taxiData[2][1])) {
                this.taxiData[2][1] = String.valueOf(this.average);
                this.taxiData[2][0] = s.replace(" in", "");
            }
            if (this.average < Float.parseFloat(this.taxiData[3][1]) || this.k == 0) {
                this.taxiData[5][1] = this.taxiData[4][1];
                this.taxiData[5][0] = this.taxiData[4][0];
                this.taxiData[4][1] = this.taxiData[3][1];
                this.taxiData[4][0] = this.taxiData[3][0];
                this.taxiData[3][1] = String.valueOf(this.average);
                this.taxiData[3][0] = s.replace(" in", "");
                this.k = 1;
            } else if (this.average < Float.parseFloat(this.taxiData[4][1])) {
                this.taxiData[5][1] = this.taxiData[4][1];
                this.taxiData[5][0] = this.taxiData[4][0];
                this.taxiData[4][1] = String.valueOf(this.average);
                this.taxiData[4][0] = s.replace(" in", "");
            } else if (this.average < Float.parseFloat(this.taxiData[5][1])) {
                this.taxiData[5][1] = String.valueOf(this.average);
                this.taxiData[5][0] = s.replace(" in", "");
            }
        } else if (s.contains(" out")) {
            if (this.average > Float.parseFloat(this.taxiData[6][1])) {
                this.taxiData[8][1] = this.taxiData[7][1];
                this.taxiData[8][0] = this.taxiData[7][0];
                this.taxiData[7][1] = this.taxiData[6][1];
                this.taxiData[7][0] = this.taxiData[6][0];
                this.taxiData[6][1] = String.valueOf(this.average);
                this.taxiData[6][0] = s.replace(" out", "");
            } else if (this.average > Float.parseFloat(this.taxiData[7][1])) {
                this.taxiData[8][1] = this.taxiData[7][1];
                this.taxiData[8][0] = this.taxiData[7][0];
                this.taxiData[7][1] = String.valueOf(this.average);
                this.taxiData[7][0] = s.replace(" out", "");
            } else if (this.average > Float.parseFloat(this.taxiData[8][1])) {
                this.taxiData[8][1] = String.valueOf(this.average);
                this.taxiData[8][0] = s.replace(" out", "");
            }
            if (this.average < Float.parseFloat(this.taxiData[9][1]) || this.l == 0) {
                this.taxiData[11][1] = this.taxiData[10][1];
                this.taxiData[11][0] = this.taxiData[10][0];
                this.taxiData[10][1] = this.taxiData[9][1];
                this.taxiData[10][0] = this.taxiData[9][0];
                this.taxiData[9][1] = String.valueOf(this.average);
                this.taxiData[9][0] = s.replace(" out", "");
                this.l = 1;
            } else if (this.average < Float.parseFloat(this.taxiData[10][1])) {
                this.taxiData[11][1] = this.taxiData[10][1];
                this.taxiData[11][0] = this.taxiData[10][0];
                this.taxiData[10][1] = String.valueOf(this.average);
                this.taxiData[10][0] = s.replace(" out", "");
            } else if (this.average < Float.parseFloat(this.taxiData[11][1])) {
                this.taxiData[11][1] = String.valueOf(this.average);
                this.taxiData[11][0] = s.replace(" out", "");
            }
        }
        if (s.equals(Character.toString('\u00fe'))) {
            for (i = 0; i < 12; i++) {
                if (i == 0) {
                    output.collect(new Text("Airports with Longest taxi-in"), new FloatWritable());
                } else if (i == 3) {
                    output.collect(new Text("Airports with Shortest taxi-in"), new FloatWritable());
                } else if (i == 6) {
                    output.collect(new Text("Airports with Longest taxi-out"), new FloatWritable());
                } else if (i == 9) {
                    output.collect(new Text("Airports with Shortest taxi-out"), new FloatWritable());
                }
                output.collect(new Text(this.taxiData[i][0]), new FloatWritable(Float.parseFloat(this.taxiData[i][1])));
            }
        }
    }
}