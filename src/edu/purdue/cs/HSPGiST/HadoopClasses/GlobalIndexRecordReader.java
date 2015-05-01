package edu.purdue.cs.HSPGiST.HadoopClasses;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.Predicate;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;

public class GlobalIndexRecordReader extends RecordReader<Text, NullWritable> {
	@SuppressWarnings("rawtypes")
	HSPIndex index;
	Object key1;
	Object key2;
	Text key;
	FSDataInputStream in;
	int level = 1;
	HSPIndexNode inIndex = null;
	HSPIndexNode curr = new HSPIndexNode();
	ArrayList<HSPIndexNode> stack = new ArrayList<HSPIndexNode>();
	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((float) in.getPos()) / ((float) (in.getPos() + in.available()));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		FSDataInputStream keySource = FileSystem.get(arg1.getConfiguration())
				.open(new Path(arg1.getConfiguration().get("keys-file")));
		try {
			key1 = Class.forName(arg1.getConfiguration().get("keyClassName"))
					.newInstance();
			key2 = ((Copyable) key1).copy();
			((WritableComparable) key1).readFields(keySource);
			((WritableComparable) key2).readFields(keySource);
			keySource.close();

			index = (HSPIndex) Class.forName(
					arg1.getConfiguration().get("indexClass")).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		in = FileSystem.get(arg1.getConfiguration()).open(
				((FileSplit) arg0).getPath());
		in.readBoolean();
		inIndex = new HSPIndexNode();
		inIndex.readFields(in);
		curr = (HSPIndexNode) inIndex.copy();
		if (inIndex.getPredicate() != null && level == 1)
			level = 2;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			while (true) {
				// If the current node has no more children
				// get the next node of the stack and continue in case
				// it too has run out of children this continue will
				// also catch when the stack is empty and we have an empty
				// index with the while condition
				while (curr.getChildren().size() == 0) {
					if (stack.size() == 0)
						return false;
					curr = stack.remove(stack.size() - 1);
					level--;
				}
				// we are processing one of curr's children so remove a dummy
				// child
				curr.getChildren().remove(0);
				// The next fields are universal to all nodes
				boolean type = in.readBoolean();
				long size = in.readLong();
				Predicate obj = null;
				try {
					obj = (Predicate) Class.forName(in.readUTF()).newInstance();
					obj.readFields(in);
				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException e) {
				}
				if (index.range(obj, key1, key2, level + 1)) {
					// We are consistent so read in the node
					if (type) {
						// Index node reader
						// Offset is unimportant in the global tree
						in.readInt();
						inIndex = new HSPIndexNode();
						int count = in.readInt();
						for (int i = 0; i < count; i++) {
							// populate node with dummy children to get
							// right size
							inIndex.getChildren().add(
									new HSPIndexNode(inIndex, null));
						}
						stack.add((HSPIndexNode) curr.copy());
						curr = (HSPIndexNode) inIndex.copy();
						level++;
					} else {

						key = new Text(in.readUTF());
						return true;
					}
				} else {
					// Catch the offset as it doesn't get written for reference
					// nodes
					if (type)
						size += Integer.SIZE >> 3;
					in.seek(in.getPos() + size);
				}

			}
		} catch (EOFException e) {
			return false;
		}
	}
}
