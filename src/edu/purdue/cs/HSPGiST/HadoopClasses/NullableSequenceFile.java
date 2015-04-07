package edu.purdue.cs.HSPGiST.HadoopClasses;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;

public class NullableSequenceFile {

	/**
	 * The class encapsulating with the metadata of a file. The metadata of a
	 * file is a list of attribute name/value pairs of Text type.
	 *
	 */
	public static class Metadata implements Writable {

		private TreeMap<Text, Text> theMetadata;

		public Metadata() {
			this(new TreeMap<Text, Text>());
		}

		public Metadata(TreeMap<Text, Text> arg) {
			if (arg == null) {
				this.theMetadata = new TreeMap<Text, Text>();
			} else {
				this.theMetadata = arg;
			}
		}

		public Text get(Text name) {
			return this.theMetadata.get(name);
		}

		public void set(Text name, Text value) {
			this.theMetadata.put(name, value);
		}

		public TreeMap<Text, Text> getMetadata() {
			return new TreeMap<Text, Text>(this.theMetadata);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.theMetadata.size());
			Iterator<Map.Entry<Text, Text>> iter = this.theMetadata.entrySet()
					.iterator();
			while (iter.hasNext()) {
				Map.Entry<Text, Text> en = iter.next();
				en.getKey().write(out);
				en.getValue().write(out);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			int sz = in.readInt();
			if (sz < 0)
				throw new IOException("Invalid size: " + sz
						+ " for file metadata object");
			this.theMetadata = new TreeMap<Text, Text>();
			for (int i = 0; i < sz; i++) {
				Text key = new Text();
				Text val = new Text();
				key.readFields(in);
				val.readFields(in);
				this.theMetadata.put(key, val);
			}
		}

		@Override
		public boolean equals(Object other) {
			if (other == null) {
				return false;
			}
			if (other.getClass() != this.getClass()) {
				return false;
			} else {
				return equals((Metadata) other);
			}
		}

		public boolean equals(Metadata other) {
			if (other == null)
				return false;
			if (this.theMetadata.size() != other.theMetadata.size()) {
				return false;
			}
			Iterator<Map.Entry<Text, Text>> iter1 = this.theMetadata.entrySet()
					.iterator();
			Iterator<Map.Entry<Text, Text>> iter2 = other.theMetadata
					.entrySet().iterator();
			while (iter1.hasNext() && iter2.hasNext()) {
				Map.Entry<Text, Text> en1 = iter1.next();
				Map.Entry<Text, Text> en2 = iter2.next();
				if (!en1.getKey().equals(en2.getKey())) {
					return false;
				}
				if (!en1.getValue().equals(en2.getValue())) {
					return false;
				}
			}
			if (iter1.hasNext() || iter2.hasNext()) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			assert false : "hashCode not designed";
			return 42; // any arbitrary constant will do
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("size: ").append(this.theMetadata.size()).append("\n");
			Iterator<Map.Entry<Text, Text>> iter = this.theMetadata.entrySet()
					.iterator();
			while (iter.hasNext()) {
				Map.Entry<Text, Text> en = iter.next();
				sb.append("\t").append(en.getKey().toString()).append("\t")
						.append(en.getValue().toString());
				sb.append("\n");
			}
			return sb.toString();
		}
	}

	public static Writer createWriter(Configuration conf, Path name,
			Class key, Class val) throws IOException {
		return new Writer(conf, Writer.file(name), Writer.keyClass(key),
				Writer.valueClass(val));
	}
	
	/** Write key/value pairs to a sequence-format file. */
	public static class Writer implements java.io.Closeable {
		private Configuration conf;
		FSDataOutputStream out;
		DataOutputBuffer buffer = new DataOutputBuffer();

		Class keyClass;
		Class valClass;

		DataOutputStream deflateOut = null;
		Metadata metadata = null;

		protected Serializer keySerializer;
		protected Serializer uncompressedValSerializer;
		protected Serializer compressedValSerializer;

		public static interface Option {
		}

		static class FileOption extends Options.PathOption implements Option {
			FileOption(Path path) {
				super(path);
			}
		}

		static class KeyClassOption extends Options.ClassOption implements
				Option {
			KeyClassOption(Class<?> value) {
				super(value);
			}
		}

		static class ValueClassOption extends Options.ClassOption implements
				Option {
			ValueClassOption(Class<?> value) {
				super(value);
			}
		}

		public static Option file(Path value) {
			return new FileOption(value);
		}

		public static Option keyClass(Class<?> value) {
			return new KeyClassOption(value);
		}

		public static Option valueClass(Class<?> value) {
			return new ValueClassOption(value);
		}

		

		/**
		 * Construct a uncompressed writer from a set of options.
		 * 
		 * @param conf
		 *            the configuration to use
		 * @param options
		 *            the options used when creating the writer
		 * @throws IOException
		 *             if it fails
		 */
		Writer(Configuration conf, Option... opts) throws IOException {
			FileOption fileOption = Options.getOption(FileOption.class, opts);
			KeyClassOption keyClassOption = Options.getOption(
					KeyClassOption.class, opts);
			ValueClassOption valueClassOption = Options.getOption(
					ValueClassOption.class, opts);
			// check consistency of options
			if (fileOption == null) {
				throw new IllegalArgumentException("file must be specified");
			}
			FSDataOutputStream out;
			Path p = fileOption.getValue();
			FileSystem fs = p.getFileSystem(conf);
			int bufferSize = getBufferSize(conf);
			short replication = fs.getDefaultReplication(p);
			long blockSize = fs.getDefaultBlockSize(p);
			Progressable progress = null;
			out = fs.create(p, true, bufferSize, replication, blockSize,
					progress);
			Class<?> keyClass = keyClassOption == null ? Object.class
					: keyClassOption.getValue();
			Class<?> valueClass = valueClassOption == null ? Object.class
					: valueClassOption.getValue();
			init(conf, out, keyClass, valueClass, new Metadata());
		}

		/** Initialize. */
		@SuppressWarnings("unchecked")
		void init(Configuration conf, FSDataOutputStream out, Class keyClass,
				Class valClass, Metadata metadata) throws IOException {
			this.conf = conf;
			this.out = out;
			this.keyClass = keyClass;
			this.valClass = valClass;
			this.metadata = metadata;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.keySerializer = serializationFactory.getSerializer(keyClass);
			if (this.keySerializer == null) {
				throw new IOException(
						"Could not find a serializer for the Key class: '"
								+ keyClass.getCanonicalName() + "'. "
								+ "Please ensure that the configuration '"
								+ CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
								+ "' is "
								+ "properly configured, if you're using"
								+ "custom serialization.");
			}
			this.keySerializer.open(buffer);
			this.uncompressedValSerializer = serializationFactory
					.getSerializer(valClass);
			if (this.uncompressedValSerializer == null) {
				throw new IOException(
						"Could not find a serializer for the Value class: '"
								+ valClass.getCanonicalName() + "'. "
								+ "Please ensure that the configuration '"
								+ CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
								+ "' is "
								+ "properly configured, if you're using"
								+ "custom serialization.");
			}
			this.uncompressedValSerializer.open(buffer);
		}

		/** Returns the configuration of this file. */
		Configuration getConf() {
			return conf;
		}

		/** Close the file. */
		@Override
		public synchronized void close() throws IOException {
			keySerializer.close();
			uncompressedValSerializer.close();
			if (compressedValSerializer != null) {
				compressedValSerializer.close();
			}

			if (out != null) {

				// Close the underlying stream iff we own it...
				out.close();
				out = null;
			}
		}

		/** Append a key/value pair. */
		public void append(Writable key, Writable val) throws IOException {
			append((Object) key, (Object) val);
		}

		/** Append a key/value pair. */
		@SuppressWarnings("unchecked")
		public synchronized void append(Object key, Object val)
				throws IOException {
			


			buffer.reset();
			if (key != null) {
				if (key.getClass() != keyClass)
					throw new IOException("wrong key class: "
							+ key.getClass().getName() + " is not " + keyClass);
				// Append the 'key'
				keySerializer.serialize(key);
				int keyLength = buffer.getLength();
				if (keyLength < 0)
					throw new IOException("negative length keys not allowed: "
							+ key);
			}
			// Append the 'value'
			if(val != null){
				if (val.getClass() != valClass)
					throw new IOException("wrong value class: "
							+ val.getClass().getName() + " is not " + valClass);
				uncompressedValSerializer.serialize(val);
			}

			out.write(buffer.getData(), 0, buffer.getLength()); // data
		}
	} // class Writer

	/** Get the configured buffer size */
	private static int getBufferSize(Configuration conf) {
		return conf.getInt("io.file.buffer.size", 4096);
	}

}