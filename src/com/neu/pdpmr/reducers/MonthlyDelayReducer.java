package com.neu.pdpmr.reducers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdpmr.utils.FlightDataWritable;
/*
 * @author : Ayushi
 */
public class MonthlyDelayReducer extends Reducer<Text, FlightDataWritable, Text, DoubleWritable> {
    // reducer to get final csvs to calculate the average delay
    public void reduce(Text key, Iterable<FlightDataWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalFlights = 0;
        double totalDelays = 0;
        for (FlightDataWritable val : values){
            totalFlights += val.getFlights().get();
            totalDelays += val.getNormalizedDelay().get();
        }
        double meanDelay = totalDelays / totalFlights;
        String keys[] = key.toString().split("_");      
        String resultKey = keys[1]+","+keys[2]+","+keys[3]+","+keys[4];
        context.write(new Text(resultKey), new DoubleWritable(meanDelay));
    }
}