package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4b {

    final String gtid = "ywang3564";
// Try to use text as all key and value, because both results involes in a lot of combination. 
// Get the insight from this link: https://stackoverflow.com/questions/34263288/java-hadoop-mapreduce-multiple-value
// And the first https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0

// Only have mapper and reducer, no combiner
    public static class TokenizerMapper
//    extends Mapper<Object, Text, Text, DoubleWritable>{
    extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
            {
		String line = value.toString();
		String[] F = line.split("\\t");
                String pickup = F[0];
                String count = F[2];
                String fee = F[3];
                context.write(new Text(count), new Text(fee));
}}
            
    public static class IntSumReducer
  //  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    extends Reducer<Text,Text,Text,Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            // Interation from input values. 
            for (Text val : values) {
		double cur_val = Double.parseDouble(val.toString());
		count++;
		sum += cur_val;
	    }
	   double average = sum/count;
	    String output = String.format("%,.2f", average);
            //context.write(key, new Text(Integer.toString(count) + "," + String.format("%,.2f%n", Double.toString(sum)) ) ) ;
            context.write(key, new Text(output )) ;
 	}}
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q4b");
        
        // Do not use Combiner
        job.setJarByClass(Q4b.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
