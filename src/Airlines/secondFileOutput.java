package Airlines;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

class secondFileOutput extends MultipleTextOutputFormat<Text, FloatWritable> {
	secondFileOutput() {
    }

    protected String generateFileNameForKeyValue(Text key, FloatWritable value, String name) {
        return "Airports";
    }
}