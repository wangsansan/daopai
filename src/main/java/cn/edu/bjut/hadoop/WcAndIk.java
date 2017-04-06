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
package cn.edu.bjut.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

public class WcAndIk {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			byte[] bytes = value.getBytes();
			InputStream is = new ByteArrayInputStream(bytes);
			Reader reader = new InputStreamReader(is);
			IKSegmentation ikSegmentation = new IKSegmentation(reader);
//			FileSplit split = (FileSplit)context.getInputSplit();
//			int splitIndex = split.getPath().toString().indexOf("file");
//			String fileName = split.getPath().toString().substring(splitIndex);
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit)inputSplit).getPath().getName();
			Lexeme t = null;
			while ((t = ikSegmentation.next()) != null) {
				context.write(new Text(t.getLexemeText() + ":" + fileName), new Text(1+""));
			}
		}
	}

	public static class IntSumCombiner extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				String strValue = new String(value.toString().getBytes());
				sum += Integer.parseInt(strValue);
			}
			
			String strKey = new String(key.toString().getBytes());//千万不要写成String strKey = new String(key.getBytes());
			String[] keys = strKey.split(":");
			
			String newKey = null;
			String newValue = null;
			if (keys.length == 2) {
				newKey = keys[0];
				newValue = keys[1] + ":" + sum;
			}
			
			context.write(new Text(newKey), new Text(newValue));
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();
			for(Text text:values){
				sb.append(text.toString() + ";");
			}
			
			context.write(key, new Text(sb.toString()));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "daopai-zn");
		job.setJarByClass(WcAndIk.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
