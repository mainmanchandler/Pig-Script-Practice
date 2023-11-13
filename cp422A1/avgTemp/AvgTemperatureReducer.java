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
      int totalNumberOfValues = 0;
      // Goes through the values stored in shuffled map.
      for (IntWritable value: values) {
         averageTemperature = averageTemperature + value.get();

         if (totalNumberOfValues != 0) {
            totalNumberOfValues = values.size();
         }
      }

      //Calculate the average and store the result.
      averageTemperature = averageTemperature / totalNumberOfValues;
      context.write(key, new IntWritable(averageTemperature));

   }
}

