package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.HadoopClasses.LocalHSPGiSTOutputFormat;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

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
public class RandomSample<MKIn, MVIn, MKOut, MVOut, Pred> extends Configured
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
		@SuppressWarnings("rawtypes")
		private Parser parse;

		@SuppressWarnings("rawtypes")
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				parse = (Parser) Class.forName(conf.get("parserClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rand = new Random();
			percent = context.getConfiguration().getDouble(
					"mapreduce.mapper.sample-percentage", .01);
			if (percent > 1) {
				percent = percent / 100;
			}
		}

		@SuppressWarnings({ "unchecked" })
		public void map(MKIn key, MVIn value, Context context)
				throws IOException, InterruptedException {
			if (parse.isArrayParse) {
				ArrayList<Pair<MKOut, MVOut>> list = parse.arrayParse(key,
						value);
				Pair<MKOut, MVOut> pair = null;
				for (int i = 0; i < list.size(); i++) {
					pair = list.get(i);
					if (rand.nextDouble() < percent)
						context.write(pair.getFirst(), pair.getSecond());
				}
			} else {
				Pair<MKOut, MVOut> pair = parse.parse(key, value);
				if (rand.nextDouble() < percent)
					context.write(pair.getFirst(), pair.getSecond());
			}
		}
	}

	public static class GlobalReducer<MKOut, MVOut, RKOut, RVOut, Pred> extends
			Reducer<MKOut, MVOut, RKOut, RVOut> {
		@SuppressWarnings("rawtypes")
		HSPIndex index;
		int numOfReducers = 1;

		@SuppressWarnings("rawtypes")
		public void setup(Context context) {
			try {
				index = (HSPIndex) Class.forName(
						context.getConfiguration().get("indexClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			numOfReducers = context.getConfiguration().getInt("numReduce", 1);
		}

		@SuppressWarnings("unchecked")
		public void reduce(MKOut key, Iterable<MVOut> values, Context context) {
			index.samples.add(((Copyable<MKOut>)key).copy());
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void cleanup(Context context) throws IOException, InterruptedException {
			index.setupPartitions(numOfReducers);
			HSPNode nodule = index.globalRoot;
			index.globalRoot.getSize();
			ArrayList<HSPNode> stack = new ArrayList<HSPNode>();
			while (!(stack.size() == 0 && nodule == null)) {
				if (nodule != null) {
					if (nodule instanceof HSPIndexNode<?, ?, ?>) {
						HSPIndexNode temp = (HSPIndexNode) nodule;
						context.write((RKOut) temp, null);
						for (int i = 0; i < temp.getChildren().size(); i++) {
							stack.add((HSPNode) temp.getChildren().get(i));
						}
						nodule = stack.remove(stack.size() - 1);
					} else {
						context.write(null, (RVOut) nodule);
						nodule = null;
					}
				} else
					nodule = stack.remove(stack.size() - 1);
			}
			FileSystem hdfs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream output = hdfs.create(new Path("localRoots/partRoots"));
			output.writeInt(index.partRoots.size());
			for (Pair<Pred, IntWritable> sample : ((ArrayList<Pair<Pred, IntWritable>>) index.partRoots)) {
				((WritableComparable<Pair<Pred, IntWritable>>) sample)
						.write(output);
			}
			output.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length == 5)
			conf.set("mapreduce.mapper.sample-percentage", args[4]);
		conf.set("parserClass", parse.getClass().getName());
		conf.set("indexClass", index.getClass().getName());
		Job job = Job.getInstance(conf, "Random_Sample");
		job.setJarByClass(RandomSample.class);
		long defaultBlockSize = 0;
		// Get the number of mappers that will be launched for this task
		// So we can determine the number of reducers
		int numOfReduce = 1;
		long inputFileLength = 0;
		FileSystem fileSystem = FileSystem.get(this.getConf());
		inputFileLength = fileSystem.getContentSummary(new Path(args[3]))
				.getLength();
		defaultBlockSize = fileSystem.getDefaultBlockSize(new Path(args[3]));
		if (inputFileLength > 0 && defaultBlockSize > 0) {
			numOfReduce = (int) (((inputFileLength / defaultBlockSize) + 1) * 2);
		}
		// set numOfReduce to the first valid number lte a valid number
		// E.g. Quadtree can only support partitions of size 1+3n for n >= 0
		// So if numOfReduce == 3 we must increase that to 4
		if ((numOfReduce - 1) % (index.numSpaceParts - 1) != 0)
			numOfReduce = ((numOfReduce - 1) / (index.numSpaceParts - 1) + 1)
					* (index.numSpaceParts - 1) + 1;
		job.getConfiguration().setInt("numReduce", numOfReduce);
		job.setMapOutputKeyClass(parse.keyout);
		job.setMapOutputValueClass(parse.valout);
		job.setOutputKeyClass(HSPIndexNode.class);
		job.setOutputValueClass(HSPReferenceNode.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(LocalHSPGiSTOutputFormat.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(SampleMap.class);
		job.setReducerClass(GlobalReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTSECONDOUT);
		sb.append(CommandInterpreter.postScript);
		Path globalIndexFile = new Path(sb.toString());
		FileOutputFormat.setOutputPath(job, globalIndexFile);
		boolean succ = job.waitForCompletion(true);
		if(!succ){
			FileSystem hdfs = FileSystem.get(job.getConfiguration());
			hdfs.delete(globalIndexFile, true);
		}
		return succ ? 0 : 1;
	}

}