package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

private static final int MISSING = 9999;

@Override
public void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {
		// Your mapper implementation here
		// Note: This code was created using MaxTemperatureMapper.java class example as reference, 
		// hence the immence similarity to the class example. 
		
		String line = value.toString();
		String yearMonth = line.substring(15, 21); //This grabs the year and month from yyyymmdd: yyyymm
		
		//grab the temperature value
		int currentTemperature;
		if (line.charAt(87) == '+') { //ignores the leading plus if present
		currentTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
		currentTemperature = Integer.parseInt(line.substring(87, 92));
		}
		
		// We want to get the average temperature at 1:00pm (13 in 24hr), therefore, we need to also grab the hours from time
		String pastNoonString = line.substring(23, 25);
		int pastNoon = Integer.valueOf(pastNoonString);
		
		String quality = line.substring(92, 93);
		if (currentTemperature != MISSING && quality.matches("[01459]") && pastNoon == 13) {
		   context.write(new Text(yearMonth), new IntWritable(currentTemperature));
		}
	}
}

