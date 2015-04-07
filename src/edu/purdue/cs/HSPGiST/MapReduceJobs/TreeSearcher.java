package edu.purdue.cs.HSPGiST.MapReduceJobs;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

public class TreeSearcher<T, K, R> extends Configured implements Tool {

	private K key1;
	private K key2;
	private HSPIndex<T, K, R> ind;

	public TreeSearcher(K key1, K key2, HSPIndex<T, K, R> ind) {
		this.key1 = key1;
		this.key2 = key2;
		this.ind = ind;
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTSECONDOUT);

		Path globalIndexFile = new Path(sb
				.append(CommandInterpreter.postScript).append("/")
				.append(CommandInterpreter.GLOBALFILE).toString());
		FSDataInputStream input = hdfs.open(globalIndexFile);
		FSDataOutputStream output = hdfs.create(new Path(
				"BouncingReader/QueryResult.txt"));

		// Read Global index to find which local indices to read

		HSPIndexNode<T, K, R> inIndex = null;
		ArrayList<HSPIndexNode<T, K, R>> stack = new ArrayList<HSPIndexNode<T, K, R>>();
		ArrayList<String> refs = new ArrayList<String>();
		HSPIndexNode<T, K, R> curr = new HSPIndexNode<T, K, R>();
		if (input.readBoolean()) {
			inIndex = new HSPIndexNode<T, K, R>();
			inIndex.readFields(input);
			curr = (HSPIndexNode<T, K, R>) inIndex.copy();
		} else {
			HSPReferenceNode<T, K, R> inRef = new HSPReferenceNode<T, K, R>();
			inRef.readFields(input);
			refs.add(inRef.getReference().toString());
		}

		int level = 2;
		while (stack.size() != 0 || curr.getChildren().size() != 0) {
			if (curr.getChildren().size() == 0) {
				curr = stack.remove(stack.size() - 1);
				level--;
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

			if (ind.range(obj, key1, key2, level + 1)) {
				if (type) {
					input.readInt();
					inIndex = new HSPIndexNode<T, K, R>();
					int count = input.readInt();
					for (int i = 0; i < count; i++) {
						// populate node with dummy children to get
						// right size
						inIndex.getChildren().add(
								new HSPIndexNode<T, K, R>(inIndex, (T) null));
					}
					stack.add((HSPIndexNode<T, K, R>) curr.copy());
					curr = (HSPIndexNode<T, K, R>) inIndex.copy();
					level++;
				} else {
					refs.add(input.readUTF());
				}
			} else {
				if (type)
					size += Integer.SIZE >> 3;
				input.seek(input.getPos() + size);
			}

		}
		HSPLeafNode<T, K, R> inLeaf = null;
		for (String ref : refs) {
			sb = new StringBuilder(CommandInterpreter.CONSTRUCTFIRSTOUT);

			Path local = new Path(sb.append(CommandInterpreter.postScript)
					.append("/").append(ref).toString());
			input = hdfs.open(local);
			curr = new HSPIndexNode<T, K, R>();
			if (input.readBoolean()) {
				inIndex = new HSPIndexNode<T, K, R>();
				inIndex.readFields(input);
				curr = (HSPIndexNode<T, K, R>) inIndex.copy();
			} else {
				inLeaf = new HSPLeafNode<T, K, R>();
				inLeaf.readFields(input);
			}
			stack.clear();
			level = curr.getOffset() + 1;
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

				if (ind.range(obj, key1, key2, level + 1 + offset)) {
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
							output.writeBytes(data.toString() + "\n");
						}
					}
				} else {
					long pos = input.getPos();
					input.seek(pos + size);
				}

			}

		}
		output.close();
		hdfs.close();
		return 0;
	}

}
