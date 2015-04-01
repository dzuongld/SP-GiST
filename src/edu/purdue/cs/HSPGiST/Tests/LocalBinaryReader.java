package edu.purdue.cs.HSPGiST.Tests;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

/**
 * Trivial class that will read from output of the local index constructor and
 * output the binary in Text form <br>
 * NOTE: part-m-xxxxx does not correspond to the file part-r-xxxxx The binary
 * output doesn't match the text; however, there is a text file that matches up
 * for each binary file
 * 
 * @author Stefan Brinton
 *
 */
public class LocalBinaryReader extends Configured implements Tool {
	public LocalBinaryReader() {
		super();
	}

	@SuppressWarnings("rawtypes")
	public static class Map extends
			Mapper<HSPIndexNode, HSPLeafNode, HSPIndexNode, HSPLeafNode> {
		public void map(HSPIndexNode key, HSPLeafNode value, Context context)
				throws IOException, InterruptedException {
			context.write((HSPIndexNode) key.copy(), (HSPLeafNode) value.copy());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "LocalBinaryTest");
		job.setJarByClass(LocalBinaryReader.class);

		job.setOutputKeyClass(HSPIndexNode.class);
		job.setOutputValueClass(HSPLeafNode.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);
		job.setMapperClass(Map.class);
		// This is a hardcoded
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTFIRSTOUT);
		FileInputFormat.setInputPaths(job,
				new Path(sb.append(CommandInterpreter.postScript).toString()));
		FileOutputFormat.setOutputPath(job, new Path("TextOutput"));

		boolean succ = job.waitForCompletion(true);
		/*
		 * FileSystem fs = FileSystem.get(getConf()); fs.delete(new
		 * Path("temp"), true);
		 */
		return succ ? 0 : 1;
	}

}