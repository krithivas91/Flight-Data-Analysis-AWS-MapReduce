package Airlines;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class thirdMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    int f;
    int k;

    public thirdMapper() {
        this.f = 1;
        this.k = 0;
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String key1 = "";
        int count = 0;
        int i = 0;
        int length = line.length();
        while (i < length && this.f == 0) {
            if (line.charAt(i) == ',') {
                count++;
            }
            while (i < length && count == 22) {
                i++;
                char k = line.charAt(i);
                if (k == ',') {
                    count++;
                    if (key1.matches("[A-Za-z]+") && !key1.equals("NA")) {
                        output.collect(new Text(key1), new IntWritable(1));
                    }
                    this.f = 1;
                } else {
                    key1 = new StringBuilder(String.valueOf(key1)).append(k).toString();
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