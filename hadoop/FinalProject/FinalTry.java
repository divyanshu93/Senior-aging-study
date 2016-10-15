package FinalProject;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FinalTry {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Configuration());
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: FinalTry path_to_target path_to_columns output_path");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Map Side Join");
		job.setJarByClass(FinalTry.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Map2.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(otherArgs[2]), true);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			if (fields[0].equals("start")) {
				context.write(new Text(fields[0]), new Text("target," + value.toString()));
			} else {
				int val = (int) Double.parseDouble(fields[0]);
				context.write(new Text(String.valueOf(val)), new Text("target," + value.toString()));
			}
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			if (fields[0].equals("start")) {
				context.write(new Text(fields[0]), new Text("columns," + value.toString()));
			} else {
				int val = 0;
				if (fields[0].contains("\"")) {
					String[] temp = fields[0].split("\"");
					String[] str = temp[1].split(",");
					if (str.length >= 2) {
						String ff = str[0] + str[1];
						val = (int) Double.parseDouble(ff);
					}
				} else {
					val = (int) Double.parseDouble(fields[0]);
				}

				context.write(new Text(String.valueOf(val)), new Text("columns," + value.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, String> targetMap = new HashMap<String, String>();
		private HashMap<String, String> columnsMap = new HashMap<String, String>();

		@Override
		public void reduce(Text StartTime, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				// // convert to string
				// String stringValue = value.toString();
				String[] fields = value.toString().split(",");
				if (fields[0].compareTo("target") == 0) {
					targetMap.put(StartTime.toString(), value.toString());
				}
				if (fields[0].compareTo("columns") == 0) {
					columnsMap.put(StartTime.toString(), value.toString());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			TreeMap<String, String> sorted_target = new TreeMap<>(targetMap);
			TreeMap<String, String> sorted_column = new TreeMap<>(columnsMap);
			for (String StartTime : sorted_target.keySet()) {
				if (sorted_column.containsKey(StartTime)) {
					String targetValue = sorted_target.get(StartTime);
					String columnValue = sorted_column.get(StartTime);
					String result = columnValue + "," + targetValue;
					context.write(new Text(result), new Text(""));
				}
			}
		}
	}

}
