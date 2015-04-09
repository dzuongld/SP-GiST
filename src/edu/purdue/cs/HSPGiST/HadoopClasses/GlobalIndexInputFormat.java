package edu.purdue.cs.HSPGiST.HadoopClasses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Sized;

public class GlobalIndexInputFormat extends FileInputFormat<Text, NullWritable> {

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new GlobalIndexRecordReader();
	}

	@SuppressWarnings("rawtypes")
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();
			long length = file.getLen();
			if (length != 0) {
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus) {
					blkLocations = ((LocatedFileStatus) file)
							.getBlockLocations();
				} else {
					FileSystem fs = path.getFileSystem(job.getConfiguration());
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path)) {
					long blockSize = file.getBlockSize();
					long splitSize = computeSplitSize(blockSize, minSize,
							maxSize);
					long bytesRemaining = length;

					if (bytesRemaining > splitSize * 1.1) {
						FileSystem hdfs = path.getFileSystem(job
								.getConfiguration());
						FSDataInputStream in = hdfs.open(path);
						in.readBoolean();
						HSPIndexNode inIndex = new HSPIndexNode();
						inIndex.readFields(in);
						long rootSize = inIndex.getPredicate().getClass()
								.getName().length()
								+ ((Sized) inIndex.getPredicate()).getSize()
								+ 2
								+ (Integer.SIZE >> 2)
								+ (Long.SIZE >> 3)
								+ 1;
						int count = job.getConfiguration().getInt(
								"index-numOfSplit", 2);
						for (int i = 0; i < count; i++) {
							if (in.readBoolean()) {
								inIndex.readFields(in);
								splitSize = inIndex.size
										+ inIndex.getPredicate().getClass()
												.getName().length()
										+ ((Sized) inIndex.getPredicate())
												.getSize() + 2
										+ (Integer.SIZE >> 3)
										+ (Long.SIZE >> 3) + 1;
								if (i == 0) {
									splitSize += rootSize;
								}
								in.seek(in.getPos() + inIndex.size
										- (Integer.SIZE >> 3));
								int blkIndex = getBlockIndex(blkLocations,
										length - bytesRemaining);
								try {
									splits.add(makeSplit(path, length
											- bytesRemaining, splitSize,
											blkLocations[blkIndex].getHosts(),
											blkLocations[blkIndex]
													.getCachedHosts()));
								} catch (NoSuchMethodError e) {
									// we are on an older version of hadoop that
									// doesn't
									// have .getCachedHosts() (Hathi doesn't)

									splits.add(makeSplit(path, length
											- bytesRemaining, bytesRemaining,
											blkLocations[blkIndex].getHosts()));

								}
								bytesRemaining -= splitSize;
							}
						}
					}
					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						try {
							splits.add(makeSplit(path, length - bytesRemaining,
									bytesRemaining,
									blkLocations[blkIndex].getHosts(),
									blkLocations[blkIndex].getCachedHosts()));
						} catch (NoSuchMethodError e) {
							// we are on an older version of hadoop that
							// doesn't
							// have .getCachedHosts() (Hathi doesn't)
							splits.add(makeSplit(path, length
									- bytesRemaining, bytesRemaining,
									blkLocations[blkIndex].getHosts()));
						}
					}
				} else { // not splitable
					splits.add(makeSplit(path, 0, length,
							blkLocations[0].getHosts(),
							blkLocations[0].getCachedHosts()));
				}
			} else {
				// Create empty hosts array for zero length files
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		return splits;
	}

}
