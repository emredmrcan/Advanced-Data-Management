package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapData extends
Mapper<LongWritable, // Input key type
Text, // Input value type
Text, // Output key type
IntWritable> {// Output value type

	private static final IntWritable one = new IntWritable(1);
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] ratings = value.toString().split("\n");

		String movieId = "";

		for (String rating : ratings) {
			movieId = rating.split(",")[0];
			ctx.write(new Text(movieId), one);
		}
	}

}