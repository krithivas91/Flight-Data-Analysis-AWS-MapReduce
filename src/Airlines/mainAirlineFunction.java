package Airlines;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

public class mainAirlineFunction {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(mainAirlineFunction.class);
        conf.setJobName("airlines");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(firstMapper.class);
        conf.setReducerClass(firstReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(firstFileOutput.class);
        FileInputFormat.setInputPaths(conf, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
        JobConf conf2 = new JobConf(mainAirlineFunction.class);
        conf2.setJobName("airlines");
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);
        conf2.setMapperClass(secondMapper.class);
        conf2.setReducerClass(secondReducer.class);
        conf2.setOutputFormat(secondFileOutput.class);
        FileInputFormat.setInputPaths(conf2, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
        JobClient.runJob(conf2);
        JobConf conf3 = new JobConf(mainAirlineFunction.class);
        conf3.setJobName("airlines");
        conf3.setOutputKeyClass(Text.class);
        conf3.setOutputValueClass(IntWritable.class);
        conf3.setMapperClass(thirdMapper.class);
        conf3.setReducerClass(thirdReducer.class);
        conf3.setOutputFormat(thirdFileOutput.class);
        FileInputFormat.setInputPaths(conf3, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(conf3, new Path(args[3]));
        JobClient.runJob(conf3);
    }
}