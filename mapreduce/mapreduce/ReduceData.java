package mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceData extends
Reducer<Text, // Input key type
IntWritable, // Input value type
Text, // Output key type
IntWritable> { // Output value type

	protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value :  values) {
			count = count + value.get();
		}
		ctx.write(key, new IntWritable(count));
	}
}
