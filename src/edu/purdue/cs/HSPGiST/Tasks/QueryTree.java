package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.Predicate;
import edu.purdue.cs.HSPGiST.HadoopClasses.GlobalIndexInputFormat;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

public class QueryTree<K, R> extends Configured implements Tool {

	private K key1;
	private K key2;
	private HSPIndex<K, R> ind;

	public QueryTree(K key1, K key2, HSPIndex<K, R> ind) {
		this.key1 = key1;
		this.key2 = key2;
		this.ind = ind;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("index-numOfSplit", ind.numSpaceParts);
		conf.set("keys-file", "QueryKeys/Keys.tmp");
		conf.set("keyClassName", key1.getClass().getName());
		conf.set("indexClass", ind.getClass().getName());
		conf.set("localconstruct", CommandInterpreter.LOCALCONSTRUCT);
		conf.set("postScript", CommandInterpreter.postScript);

		// Send keys to output for sharing
		FSDataOutputStream out = FileSystem.get(conf).create(
				new Path("QueryKeys/Keys.tmp"));
		((WritableComparable<K>) key1).write(out);
		((WritableComparable<K>) key2).write(out);
		out.close();
		// Setup job
		Job job = Job.getInstance(conf, "Querying-Tree");
		job.setJarByClass(QueryTree.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(GlobalIndexInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setMapperClass(QueryMapper.class);
		job.setPartitionerClass(QueryPartitioner.class);
		job.setReducerClass(QueryReducer.class);
		// Get number of reducers (1 reducer : 1 local index file)
		StringBuilder sb = new StringBuilder(CommandInterpreter.LOCALCONSTRUCT);
		sb.append(CommandInterpreter.postScript);
		Path localIndexFile = new Path(sb.toString());
		job.setNumReduceTasks((int) (FileSystem.get(conf)
				.getContentSummary(localIndexFile).getFileCount() - 1));

		// Set input
		sb = new StringBuilder(CommandInterpreter.GLOBALCONSTRUCT);
		sb.append(CommandInterpreter.postScript);
		Path globalIndexFile = new Path(sb.toString());
		FileInputFormat.addInputPath(job, globalIndexFile);
		FileOutputFormat.setOutputPath(job, new Path("QueryResults"));
		boolean succ = job.waitForCompletion(true);
		FileSystem.get(getConf()).delete(new Path("QueryKeys"), true);
		return succ ? 0 : 1;
	}

	public static class QueryMapper extends
			Mapper<Text, NullWritable, Text, NullWritable> {
		public void map(Text key, NullWritable value, Context con)
				throws IOException, InterruptedException {
			con.write(key, value);
		}
	}

	public static class QueryPartitioner extends
			Partitioner<Text, NullWritable> {

		@Override
		public int getPartition(Text arg0, NullWritable arg1, int arg2) {
			String[] split = arg0.toString().split("-");
			return Integer.parseInt(split[split.length - 1]);
		}

	}

	public static class QueryReducer<K, R> extends
			Reducer<Text, NullWritable, Pair<K, R>, NullWritable> {
		FSDataInputStream input = null;
		HSPIndex<K, R> index = null;
		K key1;
		K key2;

		@SuppressWarnings("unchecked")
		public void slowSetup(Context context) throws IOException {
			FSDataInputStream keySource = FileSystem.get(
					context.getConfiguration()).open(
					new Path(context.getConfiguration().get("keys-file")));
			try {
				key1 = (K) Class.forName(
						context.getConfiguration().get("keyClassName"))
						.newInstance();
				key2 = ((Copyable<K>) key1).copy();
				((WritableComparable<K>) key1).readFields(keySource);
				((WritableComparable<K>) key2).readFields(keySource);
				keySource.close();
				index = (HSPIndex<K, R>) Class.forName(
						context.getConfiguration().get("indexClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
			}
		}

		@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<NullWritable> values, Context con)
				throws IOException, InterruptedException {
			if (key.toString().isEmpty())
				return;
			if(input == null)
				slowSetup(con);
			StringBuilder sb = new StringBuilder(con.getConfiguration().get(
					"localconstruct"));
			sb.append(con.getConfiguration().get("postScript"));
			input = FileSystem.get(con.getConfiguration()).open(
					new Path(sb.append('/').append(key.toString()).toString()));
			HSPIndexNode<K, R> inIndex = null;
			ArrayList<HSPIndexNode<K, R>> stack = new ArrayList<HSPIndexNode<K, R>>();
			HSPIndexNode<K, R> curr = new HSPIndexNode<K, R>();
			// Handle consistent local tree
			HSPLeafNode<K, R> inLeaf = null;
			// get back its first node
			curr = new HSPIndexNode<K, R>();
			if (input.readBoolean()) {
				inIndex = new HSPIndexNode<K, R>();
				inIndex.readFields(input);
				curr = (HSPIndexNode<K, R>) inIndex.copy();
			} else {
				inLeaf = new HSPLeafNode<K, R>();
				inLeaf.readFields(input);
				for (int i = 0; i < inLeaf.getKeyRecords().size(); i++) {
					Pair<K, R> data = new Pair<K, R>();
					data = inLeaf.getKeyRecords().get(i);
					if (index.range(data.getFirst(), key1, key2))
						con.write(data, NullWritable.get());
				}
				return;
			}

			// set our level as the root's (kept in offset)
			int level = curr.getOffset();
			while (true) {
				while (curr.getChildren().size() == 0) {
					if (stack.size() == 0) {
						input.close();
						return;
					}
					curr = stack.remove(stack.size() - 1);
					level--;
					level -= curr.getOffset();
				}
				curr.getChildren().remove(0);
				boolean type = input.readBoolean();
				long size = input.readLong();
				Predicate obj = null;
				try {
					Class<Predicate> clazz = (Class<Predicate>) Class
							.forName(input.readUTF());
					obj = clazz.newInstance();
					obj.readFields(input);
				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException e) {
				}
				int offset = input.readInt();

				if (index.range(obj, key1, key2, level + 1)) {
					if (type) {
						inIndex = new HSPIndexNode<K, R>();
						inIndex.setOffset(offset);
						int count = input.readInt();
						for (int i = 0; i < count; i++) {
							// populate node with dummy children to get
							// right size
							inIndex.getChildren().add(
									new HSPIndexNode<K, R>(inIndex,
											(Predicate) null));
						}
						stack.add((HSPIndexNode<K, R>) curr.copy());
						curr = (HSPIndexNode<K, R>) inIndex.copy();
						level++;
						level += offset;
					} else {
						int count = input.readInt();
						for (int i = 0; i < count; i++) {
							Pair<K, R> data = new Pair<K, R>();
							data.readFields(input);
							if (index.range(data.getFirst(), key1, key2))
								con.write(data, NullWritable.get());
						}
					}
				} else {
					long pos = input.getPos();
					input.seek(pos + size);
				}
			}

		}

	}

}
