package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public int run(String[] args) throws Exception {		
		Path inputPath = new Path(args[2]);
		Path outputPath =new Path(args[3]);
		int numReducers = Integer.parseInt(args[1]);

		Configuration conf = this.getConf();

		// Define a new job;
		Job job = Job.getInstance(conf);

		job.setJobName("movieRatingCounter");
		// Set path of the input file/folder
		// if it is a folder, the job reads all the files in the specified folder
		FileInputFormat.addInputPath(job, inputPath);

		FileOutputFormat.setOutputPath(job, outputPath);

		// Specify the class of the Driver for this job
		job.setJarByClass(Driver.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);
		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapData.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set reduce class
		job.setReducerClass(ReduceData.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numReducers);

		// Execute the job and wait for completion
		return job.waitForCompletion(false) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {

		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);

	}
}
