package com.neu.pdpmr.a8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdpmr.mappers.CalculateMonthlyDelay;
import com.neu.pdpmr.mappers.TopAirlineAirportMap;
import com.neu.pdpmr.reducers.MonthlyDelayReducer;
import com.neu.pdpmr.reducers.TopAirlineAirportReducer;
import com.neu.pdpmr.utils.FlightDataWritable;
import com.neu.pdpmr.utils.NonSplitableTextInputFormat;
import java.io.IOException;
/*
 * @author : Ayushi
 */
//Driver class - executes MR jobs
public class A8 {
	
	// sets the first MR job to calculate frequency of airlines/airports
    public static void findtopfive(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job j = Job.getInstance(conf);
        j.setJarByClass(TopAirlineAirportMap.class);
        j.setMapperClass(TopAirlineAirportMap.ActiveAirportAirlinesMapper.class);
        j.setPartitionerClass(TopAirlineAirportMap.TopPartitioner.class);
        j.setCombinerClass(TopAirlineAirportReducer.class);
        j.setReducerClass(TopAirlineAirportReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        j.setInputFormatClass(NonSplitableTextInputFormat.class);
        j.setNumReduceTasks(2);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1] + "/output1"));
        j.waitForCompletion(true);
    }
 // sets the second MR job to calculate average delay of airlines/airports
    public static void averageMonthlyDelayJob(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job j2 = Job.getInstance(conf);
        j2.setJarByClass(CalculateMonthlyDelay.class);
        j2.setMapperClass(CalculateMonthlyDelay.AverageMonthlyDelayMapper.class);
        j2.setPartitionerClass(CalculateMonthlyDelay.AverageMonthlyPartitioner.class);
        j2.setReducerClass(MonthlyDelayReducer.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(FlightDataWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);
        j2.setInputFormatClass(NonSplitableTextInputFormat.class);
        j2.getConfiguration().set("output1",args[1] + "/output1");
        j2.setNumReduceTasks(24);
        j2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        FileInputFormat.addInputPath(j2, new Path(args[0]));
        FileOutputFormat.setOutputPath(j2, new Path(args[1] + "/output2"));
        j2.waitForCompletion(true);

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        findtopfive(conf,args);
        averageMonthlyDelayJob(conf,args);
    }
}
