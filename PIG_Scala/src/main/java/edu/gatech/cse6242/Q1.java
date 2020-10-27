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

public class Q1 {

    final String gtid = "ywang3564";
// Try to use text as all key and value, because both results involes in a lot of combination. 
// Get the insight from this link: https://stackoverflow.com/questions/34263288/java-hadoop-mapreduce-multiple-value
// And the first https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0

// Only have mapper and reducer, no combiner
    public static class TokenizerMapper
//    extends Mapper<Object, Text, Text, DoubleWritable>{
    extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException 
            {
		String line = value.toString();
		String[] F = line.split(",");
                String pickup = F[0];
                String distance = F[2];
                String fee = F[3];

            if(Double.parseDouble(distance) > 0 &&  Double.parseDouble(fee) > 0 && Double.parseDouble(pickup) > 0  ) {
                context.write(new Text(pickup), new Text(fee));
	    }
}}
            
    public static class IntSumReducer
  //  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    extends Reducer<Text,Text,Text,Text> {
        
        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            // Interation from input values. 
            for (Text val : values) {
		double cur_val = Double.parseDouble(val.toString());
		count++;
		sum += cur_val;
	    }
	    String output = String.format("%,.2f",sum);
            //context.write(key, new Text(Integer.toString(count) + "," + String.format("%,.2f%n", Double.toString(sum)) ) ) ;
            context.write(key, new Text(Integer.toString(count) + "," + output ) ) ;
 	}}
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1");
        
        // Do not use Combiner
        job.setJarByClass(Q1.class);
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
