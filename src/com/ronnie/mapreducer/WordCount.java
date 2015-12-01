package com.ronnie.mapreducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ���ʵ�ͳ��
 * @author ronnie
 */
public class WordCount extends Configured implements Tool{
	/**
	 * Map �� :���н��ж�ȡ,����Value ��һ�е�ֵ.
	 */
	static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text word = new Text();
		private IntWritable count = new IntWritable(1);
		/*
		 * ���Ǹ����map����.
		 */
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//��ȡһ�е�����
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			
			while(stringTokenizer.hasMoreTokens()){
				String workValue = stringTokenizer.nextToken();
				word.set(workValue);
				//ÿ�õ�һ�����ʣ���д�뵽reduce��.
				context.write(word, count);
			}
			
		}
	}
	/** 
	 * Reduce ������
	 */
	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		/*
		 * ��map�д��ݵ�����
		 */
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int count = 0;//���ֵĴ���
			
			for (IntWritable value : values) {
				count+=value.get() ;
			}
			
			result.set(count);
			//д����
			context.write(key, result);
		}
	}
	/*
	 * Driver ����
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new WordCount(), args);
		//��������
		System.exit(result);
	}
	@Override
	public int run(String[] args) throws Exception {
		//��ȡ������Ϣ
		Configuration configuration = getConf();
		//��ȡjob
		Job job = Job.getInstance(configuration, "wordcount");
		//����Job���е���
		job.setJarByClass(WordCount.class);
		
		
		//������������key��value������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//����mapper��reducer��
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		//�������������Ŀ¼
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//�ύjob
		boolean result = job.waitForCompletion(true);
		return result ? 0 : -1;
	}
}
