package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterable<IntWritable> values,
   Context context)
   throws IOException, InterruptedException {
	
	// Your reduce function implementation here
	// Note: This code was created using MaxTemperatureReducer.java class example as reference, hence the similarity.
	int averageTemperature = 0;
	int numberOfValuesInSet = 0;
	
	// Goes through the temperature values stored in shuffled map and gets a running total.
	for (IntWritable value: values) {
	   averageTemperature = averageTemperature + value.get();
	   numberOfValuesInSet = numberOfValuesInSet + 1;
	}
	
	//Calculate the average and store the result.
    averageTemperature = averageTemperature / numberOfValuesInSet;
    context.write(key, new IntWritable(averageTemperature));
	
	}
}

