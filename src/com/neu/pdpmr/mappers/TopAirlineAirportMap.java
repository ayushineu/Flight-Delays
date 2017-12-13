package com.neu.pdpmr.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import com.neu.pdpmr.utils.CSVParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
 * @author : Ayushi
 */
public class TopAirlineAirportMap {
// map function to calculate the frequency 
    public static class ActiveAirportAirlinesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private CSVParser csvParser = new CSVParser(',', '"');
        HashMap<String,Integer> airlineCounter = new HashMap<>();
        HashMap<String,Integer> airportCounter = new HashMap<>();

        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inVal = csvParser.parseLine(value.toString());
            if (inVal == null || inVal.length == 0)         // Check if record is null or empty
                return;

            if (inVal[8] != null)      
                airlineCounter.put(inVal[8], airlineCounter.getOrDefault(inVal[8], 0) + 1);
            if (inVal[23] != null)       
                airportCounter.put(inVal[23], airportCounter.getOrDefault(inVal[23], 0) + 1);
        }

      
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator it = airlineCounter.entrySet().iterator();
            Iterator it2 = airportCounter.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                context.write(new Text(String.valueOf("0_"+pair.getKey())),new IntWritable((int)pair.getValue()));// adds "0" for airline
            }
            while(it2.hasNext()){
                Map.Entry pair = (Map.Entry)it2.next();
                context.write(new Text(String.valueOf("1_"+pair.getKey())),new IntWritable((int)pair.getValue())); // adds "1" for airport
            }
        }
    }

    

   
    public static class TopPartitioner extends Partitioner<Text,IntWritable>{

        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            if(text.toString().substring(0,2).equals("0")) return 0;
            return 1;
        }
    }
}


