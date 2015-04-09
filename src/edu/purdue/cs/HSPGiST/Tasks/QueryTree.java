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
import edu.purdue.cs.HSPGiST.HadoopClasses.GlobalIndexInputFormat;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

public class QueryTree<T, K, R> extends Configured implements Tool {

	private K key1;
	private K key2;
	private HSPIndex<T, K, R> ind;

	public QueryTree(K key1, K key2, HSPIndex<T, K, R> ind) {
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
		conf.set("constructfirstout", CommandInterpreter.CONSTRUCTFIRSTOUT);
		conf.set("postScript", CommandInterpreter.postScript);
		FSDataOutputStream out = FileSystem.get(conf).create(
				new Path("QueryKeys/Keys.tmp"));
		((WritableComparable<T>) key1).write(out);
		((WritableComparable<T>) key2).write(out);
		out.close();
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
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTFIRSTOUT);
		sb.append(CommandInterpreter.postScript);
		Path localIndexFile = new Path(sb.toString());
		job.setNumReduceTasks((int) (FileSystem.get(conf)
				.getContentSummary(localIndexFile).getFileCount() - 1));
		sb = new StringBuilder(CommandInterpreter.CONSTRUCTSECONDOUT);
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

	public static class QueryReducer<T, K, R> extends
			Reducer<Text, NullWritable, Pair<K,R>, NullWritable> {
		FSDataInputStream input = null;
		HSPIndex<T,K,R> index = null;
		K key1;
		K key2;
		@SuppressWarnings("unchecked")
		public void setup(Context context) throws IOException {
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
				index = (HSPIndex<T,K,R>) Class.forName(
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
			StringBuilder sb = new StringBuilder(con.getConfiguration().get("constructfirstout"));
			sb.append(con.getConfiguration().get("postScript"));
			input = FileSystem.get(con.getConfiguration()).open(
					new Path(sb.append('/').append(key.toString()).toString()));
			HSPIndexNode<T, K, R> inIndex = null;
			ArrayList<HSPIndexNode<T, K, R>> stack = new ArrayList<HSPIndexNode<T, K, R>>();
			HSPIndexNode<T, K, R> curr = new HSPIndexNode<T, K, R>();
			// Handle consistent local tree
			HSPLeafNode<T, K, R> inLeaf = null;
			// get back its first node
			curr = new HSPIndexNode<T, K, R>();
			if (input.readBoolean()) {
				inIndex = new HSPIndexNode<T, K, R>();
				inIndex.readFields(input);
				curr = (HSPIndexNode<T, K, R>) inIndex.copy();
			} else {
				inLeaf = new HSPLeafNode<T, K, R>();
				inLeaf.readFields(input);
			}

			// set our level as 1 below the root's (kept in offset)
			int level = curr.getOffset() + 1;
			while (stack.size() != 0 || curr.getChildren().size() != 0) {
				if (curr.getChildren().size() == 0) {
					curr = stack.remove(stack.size() - 1);
					level--;
					level -= curr.getOffset();
					continue;
				}
				curr.getChildren().remove(0);
				boolean type = input.readBoolean();
				long size = input.readLong();
				T obj = null;
				try {
					Class<T> clazz = (Class<T>) Class.forName(input.readUTF());
					obj = clazz.newInstance();
					((WritableComparable<T>) obj).readFields(input);
				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException e) {
				}
				int offset = input.readInt();

				if (index.range(obj, key1, key2, level + 1)) {
					if (type) {
						inIndex = new HSPIndexNode<T, K, R>();
						inIndex.setOffset(offset);
						int count = input.readInt();
						for (int i = 0; i < count; i++) {
							// populate node with dummy children to get
							// right size
							inIndex.getChildren()
									.add(new HSPIndexNode<T, K, R>(inIndex,
											(T) null));
						}
						stack.add((HSPIndexNode<T, K, R>) curr.copy());
						curr = (HSPIndexNode<T, K, R>) inIndex.copy();
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
			input.close();
		}
		
	}
	
}
