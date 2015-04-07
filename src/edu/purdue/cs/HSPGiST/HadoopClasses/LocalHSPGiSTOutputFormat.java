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

package edu.purdue.cs.HSPGiST.HadoopClasses;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.purdue.cs.HSPGiST.HadoopClasses.NullableSequenceFile.Writer;

public class LocalHSPGiSTOutputFormat<K, V> extends SequenceFileOutputFormat<K, V> {

	protected edu.purdue.cs.HSPGiST.HadoopClasses.NullableSequenceFile.Writer getWriter(TaskAttemptContext context,
			Class<?> keyClass, Class<?> valueClass) throws IOException {
		Configuration conf = context.getConfiguration();

		Path file = getDefaultWorkFile(context, "");
		return edu.purdue.cs.HSPGiST.HadoopClasses.NullableSequenceFile.createWriter(conf, file, keyClass, valueClass);
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		final Writer out = getWriter(context,
				context.getOutputKeyClass(), context.getOutputValueClass());

		return new RecordWriter<K, V>() {

			public void write(K key, V value) throws IOException {
					out.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException {
				out.close();
			}
		};
	}
}
