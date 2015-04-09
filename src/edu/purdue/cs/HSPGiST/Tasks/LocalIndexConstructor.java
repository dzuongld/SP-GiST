package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.HadoopClasses.LocalHSPGiSTOutputFormat;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

/**
 * This MapReduce Job constructs the local indexes and outputs files containing
 * them as binary data It also constructs the global tree as a side-effect
 * 
 * @author Stefan Brinton
 *
 * @param <MKIn>
 *            The Mapper Input key
 * @param <MVIn>
 *            The Mapper Input value
 * @param <MKOut>
 *            The Mapper Output/Reducer Input/HSPIndex key
 * @param <MVOut>
 *            The Mapper Output/Reducer Input/HSPIndex value
 * @param <Pred>
 *            The HSPIndex predicate type
 */
public class LocalIndexConstructor<MKIn, MVIn, MKOut, MVOut, Pred> extends
		Configured implements Tool {
	Parser<?, ?, ?, ?> parser = null;

	HSPIndex<?, ?, ?> index = null;

	public LocalIndexConstructor(Parser<MKIn, MVIn, MKOut, MVOut> parser,
			HSPIndex<Pred, MKOut, MVOut> index) {
		super();
		this.parser = parser;
		this.index = index;
	}

	@Override
	public int run(String[] args) throws Exception {
		// Standardized setup
		Configuration conf = new Configuration();
		conf.set("parserClass", parser.getClass().getName());
		conf.set("indexClass", index.getClass().getName());
		conf.set("keyoutClass", parser.keyout.getName());
		conf.set("constructsecondout", CommandInterpreter.CONSTRUCTSECONDOUT);
		conf.set("postScript", CommandInterpreter.postScript);
		conf.set("globalfile", CommandInterpreter.GLOBALFILE);
		Job job = Job.getInstance(conf, "Local-Construction");
		job.setJarByClass(LocalIndexConstructor.class);
		// Mapper output is not the same as reducer output
		// So we need to use the parser to set output
		job.setMapOutputKeyClass(parser.keyout);
		job.setMapOutputValueClass(parser.valout);
		job.setOutputKeyClass(HSPIndexNode.class);
		job.setOutputValueClass(HSPLeafNode.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(LocalHSPGiSTOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTFIRSTOUT);
		FileOutputFormat.setOutputPath(job,
				new Path(sb.append(CommandInterpreter.postScript).toString()));
		job.setMapperClass(LocalMapper.class);
		job.setReducerClass(LocalReducer.class);
		job.setPartitionerClass(LocalPartitioner.class);
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
		job.setNumReduceTasks(numOfReduce);
		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			fileSystem = FileSystem.get(getConf());
			fileSystem.delete(new Path(sb.toString()), true);
		}
		fileSystem.delete(new Path("localRoots"), true);
		return succ ? 0 : 1;
	}

	/**
	 * This Mapper is responsible for parsing input from the file into the
	 * Key-Value pair types for the index
	 * 
	 * @author Stefan Brinton
	 *
	 * @param <MKIn>
	 *            Mapper input key
	 * @param <MVIn>
	 *            Mapper input value
	 * @param <MKOut>
	 *            Mapper output key
	 * @param <MVOut>
	 *            Mapper output value
	 */
	private static class LocalMapper<MKIn, MVIn, MKOut, MVOut> extends
			Mapper<MKIn, MVIn, MKOut, MVOut> {

		Parser<MKIn, MVIn, MKOut, MVOut> local;

		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			// Give each mapper a copy of the parser
			Configuration conf = context.getConfiguration();
			try {
				local = (Parser) Class.forName(conf.get("parserClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void map(MKIn key, MVIn value, Context context)
				throws IOException, InterruptedException {
			// Depending on input the parser could parse a "chunk" and return
			// multiple entries
			// Or it could output just one
			if (local.isArrayParse) {
				ArrayList<Pair<MKOut, MVOut>> list = local.arrayParse(key,
						value);
				Pair<MKOut, MVOut> pair = null;
				for (int i = 0; i < list.size(); i++) {
					pair = list.get(i);
					context.write(pair.getFirst(), pair.getSecond());
				}
			} else {
				Pair<MKOut, MVOut> pair = local.parse(key, value);
				context.write(pair.getFirst(), pair.getSecond());
			}
		}
	}

	/**
	 * This Partitioner is responsible for getting the global tree setup and the
	 * partition predicates so that partitioning may be done
	 * 
	 * @author Stefan Brinton
	 *
	 * @param <MKOut>
	 *            The Mapper output/HSPIndex key
	 * @param <MVOut>
	 *            The Mapper output/HSPIndex record value
	 * @param <Pred>
	 *            The HSPIndex predicate type
	 */
	private static class LocalPartitioner<MKOut, MVOut, Pred> extends
			Partitioner<MKOut, MVOut> implements Configurable {
		private Configuration conf;
		HSPIndex<Pred,MKOut,MVOut> index = null;

		@SuppressWarnings("unchecked")
		@Override
		public int getPartition(MKOut key, MVOut value, int numOfReducers) {
			try {
				if(index == null){
					index = (HSPIndex<Pred,MKOut,MVOut>) Class.forName(getConf().get("indexClass"))
						.newInstance();
					Pair<Pred, IntWritable> p = new Pair<Pred, IntWritable>();
					FileSystem hdfs = FileSystem.get(getConf());
					FSDataInputStream in = hdfs.open(new Path(
							"localRoots/partRoots"));
					int size = in.readInt();
					for (int i = 0; i < size; i++){
						p.readFields(in);
						index.partRoots.add(p.copy());
					}
					in.close();
				}
				
				
				for (int i = 0; i < index.partRoots.size(); i++) {
					if(index.consistent(index.partRoots.get(i).getFirst(), key, index.partRoots.get(i).getSecond().get()))
						return i;
				}
				
			} catch (Exception e) {

			}
			return numOfReducers;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration arg0) {
			conf = arg0;

		}

	}

	/**
	 * This reducer is responsible for both constructing a "local" index and for
	 * printing it out to file
	 * 
	 * @author Stefan Brinton
	 *
	 * @param <MKOut>
	 *            The Mapper output/HSPIndex key type
	 * @param <MVOut>
	 *            The Mapper output/HSPIndex record vaule type
	 * @param <RKOut>
	 *            The Reducer key output type: this is HSPIndexNode
	 * @param <RVOut>
	 *            The Reducer value output type this is HSPLeafNode
	 * @param <Pred>
	 *            The HSPIndex Predicate type
	 */
	private static class LocalReducer<MKOut, MVOut, RKOut, RVOut, Pred> extends
			Reducer<MKOut, MVOut, RKOut, RVOut> {

		HSPNode<Pred, MKOut, MVOut> root = null;
		HSPIndex<Pred, MKOut, MVOut> local = null;
		private int depth;

		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			// Get each reducer a reference to the index, setup an empty leaf
			// root, and set the depth of the root
			try {
				local = (HSPIndex) Class.forName(
						context.getConfiguration().get("indexClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int part = context.getConfiguration().getInt(
					"mapreduce.task.partition", 0);
			Pair<Pred, IntWritable> p = new Pair<Pred, IntWritable>();
			try {
				FileSystem hdfs = FileSystem.get(context.getConfiguration());
				FSDataInputStream in = hdfs.open(new Path(
						"localRoots/partRoots"));
				int size = in.readInt();
				p.readFields(in);
				for (int i = 0; i < part; i++) {
					p.readFields(in);
				}
				in.close();
			} catch (Exception e) {

			}
			root = new HSPLeafNode<Pred, MKOut, MVOut>(null, p.getFirst());
			depth = p.getSecond().get();
		}

		@SuppressWarnings("unchecked")
		public void reduce(MKOut key, Iterable<MVOut> values, Context context)
				throws IOException, InterruptedException {
			// For each key-value pair insert a new node into the local index
			MVOut val = null;
			for (MVOut value : values) {
				val = value;
				root = local.insert(root, ((Copyable<MKOut>) key).copy(),
						((Copyable<MVOut>) val).copy(), depth);
			}
		}

		@SuppressWarnings("unchecked")
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// Call getSize() this call will set the sizes of all nodes in the
			// tree once
			root.getSize();
			root.setOffset(depth);
			/*
			 * Output the nodes in pre-order
			 */
			ArrayList<HSPNode<Pred, MKOut, MVOut>> stack = new ArrayList<HSPNode<Pred, MKOut, MVOut>>();
			HSPNode<Pred, MKOut, MVOut> node = root;
			while (!(stack.size() == 0 && node == null)) {
				if (node != null) {
					if (node instanceof HSPIndexNode<?, ?, ?>) {
						HSPIndexNode<Pred, MKOut, MVOut> temp = (HSPIndexNode<Pred, MKOut, MVOut>) node;
						if (local.path == HSPIndex.PathShrink.TREE
								&& temp.getChildren().size() == 1) {
							// For any index node with a single child in a TREE
							// shrink
							// Remove it from output increment its child's
							// offset and give the child its predicate
							node = temp.getChildren().get(0);
							node.setOffset(node.getOffset() + 1);
							node.setPredicate(temp.getPredicate());
							continue;
						}
						context.write((RKOut) node, null);
						for (int i = 0; i < temp.getChildren().size(); i++) {
							stack.add(temp.getChildren().get(i));
						}
						node = stack.remove(stack.size() - 1);
					} else {
						context.write(null, (RVOut) node);
						node = null;
					}
				} else
					node = stack.remove(stack.size() - 1);
			}
		}
	}
}
