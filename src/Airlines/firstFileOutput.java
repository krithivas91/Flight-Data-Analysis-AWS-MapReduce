package Airlines;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

class firstFileOutput extends MultipleTextOutputFormat<Text, FloatWritable> {
	firstFileOutput() {
    }

    protected String generateFileNameForKeyValue(Text key, FloatWritable value, String name) {
        return "Airlines";
    }
}