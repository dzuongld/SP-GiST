package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.*;

/**
 * A simple random sampler
 * 
 * @author Stefan Brinton
 *
 * @param <MKIn>
 * @param <MVIn>
 * @param <MKOut>
 * @param <MVOut>
 */
public class RandomSample<MKIn, MVIn, MKOut, MVOut> extends Configured
		implements Tool {
	@SuppressWarnings("rawtypes")
	public Parser parse;
	@SuppressWarnings("rawtypes")
	HSPIndex index;

	@SuppressWarnings({ "rawtypes" })
	public RandomSample(Parser parser, HSPIndex index) {
		super();
		parse = parser;
		this.index = index;
	}

	public static class SampleMap<MKIn, MVIn, MKOut, MVOut> extends
			Mapper<MKIn, MVIn, MKOut, MVOut> {
		private Random rand;
		private double percent;
		private Parser parse;
		private ArrayList<MKOut> samples;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				parse = (Parser) Class.forName(conf.get("parserClass")).newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rand = new Random();
			percent = context.getConfiguration().getDouble("mapreduce.mapper.sample-percentage", .01);
			if(percent > 1){
				percent = percent/100;
			}
			samples = new ArrayList<MKOut>();
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void map(MKIn key, MVIn value, Context context)
				throws IOException, InterruptedException {
			if (parse.isArrayParse) {
				ArrayList<Pair<MKOut, MVOut>> list = parse.arrayParse(key,
						value);
				Pair<MKOut, MVOut> pair = null;
				for (int i = 0; i < list.size(); i++) {
					pair = list.get(i);
					if (rand.nextDouble() < percent)
						samples.add((MKOut) ((Copyable) pair.getFirst()).copy());
				}
			} else {
				Pair<MKOut, MVOut> pair = parse.parse(key, value);
				if (rand.nextDouble() < percent)
					samples.add((MKOut) ((Copyable) pair.getFirst()).copy());
			}
		}
		
		public void cleanup(Context context) throws IOException{
			FileSystem hdfs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out;
			if (hdfs.exists(new Path("samp/samples"))) {
				out = hdfs.append(new Path("samp/samples"));
			}
			else
			 out = hdfs.create(new Path("samp/samples"));
			out.writeInt(samples.size());
			for(MKOut sample : samples){
				((WritableComparable<MKOut>)sample).write(out);
			}
			out.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if(args.length == 5)
			conf.set("mapreduce.mapper.sample-percentage", args[4]);
		conf.set("parserClass", parse.getClass().getName());
		conf.set("indexClass", index.getClass().getName());
		Job job = Job.getInstance(conf, "Random_Sample");
		job.setJarByClass(RandomSample.class);

		job.setOutputKeyClass(parse.keyout);
		job.setOutputValueClass(parse.valout);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(SampleMap.class);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path("temp"));
		boolean succ = job.waitForCompletion(true);
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path("temp"), true);
		return succ ? 0 : 1;
	}

}