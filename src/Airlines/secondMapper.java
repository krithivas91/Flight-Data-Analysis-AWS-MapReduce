package Airlines;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class secondMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    int f;
    int k;

    public secondMapper() {
        this.f = 1;
        this.k = 0;
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String key1 = "";
        String key2 = "";
        String value1 = "";
        String value2 = "";
        int count = 0;
        int i = 0;
        int length = line.length();
        while (i < length && this.f == 0) {
            if (line.charAt(i) == ',') {
                count++;
            }
            while (i < length && count == 16) {
                i++;
                char k1 = line.charAt(i);
                if (k1 == ',') {
                    count++;
                    break;
                } else {
                    key1 = new StringBuilder(String.valueOf(key1)).append(k1).toString();
                }
            }
            while (i < length && count == 17) {
                i++;
                char k2 = line.charAt(i);
                if (k2 == ',') {
                    count++;
                    break;
                } else {
                    key2 = new StringBuilder(String.valueOf(key2)).append(k2).toString();
                }
            }
            while (i < length && count == 19) {
                i++;
                char v1 = line.charAt(i);
                if (v1 == ',') {
                    count++;
                    break;
                } else {
                    value1 = new StringBuilder(String.valueOf(value1)).append(v1).toString();
                }
            }
            while (i < length && count == 20) {
                i++;
                char v2 = line.charAt(i);
                if (v2 == ',') {
                    count++;
                    if (!value1.matches("[A-Za-z]+")) {
                        output.collect(new Text(new StringBuilder(String.valueOf(key1)).append(" in").toString()), new IntWritable(Integer.parseInt(value1)));
                    }
                    if (!value2.matches("[A-Za-z]+")) {
                        output.collect(new Text(new StringBuilder(String.valueOf(key2)).append(" out").toString()), new IntWritable(Integer.parseInt(value2)));
                    }
                    this.f = 1;
                } else {
                    value2 = new StringBuilder(String.valueOf(value2)).append(v2).toString();
                }
            }
            i++;
        }
        this.f = 0;
        if (this.k == 0) {
            output.collect(new Text(Character.toString('\u00fe')), new IntWritable(0));
            this.k++;
        }
    }
}