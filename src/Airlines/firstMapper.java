package Airlines;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class firstMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    int l;
    int k;

    public firstMapper() {
        this.l = 1;
        this.k = 0;
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String strKey = "";
        String strValue = "";
        int strCount = 0;
        int i = 0;
        int length = line.length();
        while (i < length && this.l == 0) {
            if (line.charAt(i) == ',') {
                strCount++;
            }
            while (i < length && strCount == 8) {
                i++;
                char k = line.charAt(i);
                if (k == ',') {
                    strCount++;
                    break;
                }
                strKey = new StringBuilder(String.valueOf(strKey)).append(k).toString();
            }
            while (i < length && strCount == 14) {
                i++;
                char v = line.charAt(i);
                if (v == ',') {
                    strCount++;
                    if (!strValue.matches("[A-Za-z]+")) {
                        output.collect(new Text(strKey), new IntWritable(Integer.parseInt(strValue) <= 0 ? 1 : 0));
                    }
                    this.l = 1;
                } else {
                    strValue = new StringBuilder(String.valueOf(strValue)).append(v).toString();
                }
            }
            i++;
        }
        this.l = 0;
        if (this.k == 0) {
            output.collect(new Text(Character.toString('\u00fe')), new IntWritable(100));
            this.k++;
        }
    }
}