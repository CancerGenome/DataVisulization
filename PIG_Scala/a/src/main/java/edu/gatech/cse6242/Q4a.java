package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4a {
  static class MapperOne extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
  		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
  					String line = value.toString();
            if(line.length()>1){
  					String[] F = line.split("\t");
  					context.write(new IntWritable(Integer.parseInt(F[0])), new IntWritable(1) ); // Out Degree 
  					context.write(new IntWritable(Integer.parseInt(F[1])), new IntWritable(-1) ); // In Degree 
               }}}
          	
  static class MapperTwo extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
  		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
  					String line = value.toString();
            if(line.length()>1){
  					String[] F = line.split("\t");
  					context.write(new IntWritable(Integer.parseInt(F[1])), new IntWritable(1)); // Statistics on degree diff
}}}
  						
  static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
  		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
  					int sum = 0;
  					for(IntWritable value: values)
  						sum += value.get();
  					context.write(key, new IntWritable(sum));}}

  public static void main(String[] args) throws Exception {

    final String gtid = "ywang3564";

    Configuration config1 = new Configuration();
    String output_Dirpath = args[1]+"_tmp";
    
    /* TODO: Needs to be implemented */
    Job createJobInst1 = Job.getInstance(config1, "Q4_step1");
    createJobInst1.setJarByClass(Q4a.class);
    createJobInst1.setMapperClass(MapperOne.class);
    createJobInst1.setReducerClass(IntSumReducer.class);
    createJobInst1.setOutputKeyClass(IntWritable.class);
    createJobInst1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(createJobInst1, new Path(args[0]));
    FileOutputFormat.setOutputPath(createJobInst1, new Path(output_Dirpath));
    createJobInst1.waitForCompletion(true);

    Configuration config2 = new Configuration();
    Job createJobInst2 = Job.getInstance(config2, "Q4_step2");
    createJobInst2.setJarByClass(Q4a.class);
    createJobInst2.setMapperClass(MapperTwo.class);
    createJobInst2.setReducerClass(IntSumReducer.class);
    createJobInst2.setOutputKeyClass(IntWritable.class);
    createJobInst2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(createJobInst2, new Path(output_Dirpath));
    FileOutputFormat.setOutputPath(createJobInst2, new Path(args[1]));
    System.exit(createJobInst2.waitForCompletion(true)?0:1);

}}


          
  				
  		
