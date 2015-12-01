package com.ronnie.mapreducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 自关联查询(单表关联)
 * @author ronnie
 */
public class InnerQuery extends Configured implements Tool {

	public static class InnerMapper extends Mapper<Text, Text, Text, Text>{
		@Override
		protected void map(Text key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String childName = new String();
			String parentName = new String();
			String flag = new String();//标记是左表还是右表.
			int i = 0;
			//找到child和parent的分界点.
			while(line.charAt(i) != ' '){
				i++;
			}
			String values[] = {line.substring(0, i),line.substring(i+1, line.length())};
			
			if("child".equals(values[0])){
				childName = values[0];
				parentName = values[1];
				
				flag = "r";
				context.write(new Text(values[1]), new Text(flag + "+" + childName ));
				
				flag = "l";
				context.write(new Text(values[0]), new Text(flag  + "+" + parentName));
			}
			
		}
	}
	private static int time = 0;
	public static class InnerReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			//输出表头
			if(time == 0){
				context.write(new Text("child"), new Text("grandParent"));
				time++;
			}
			int grandChildNum = 0;
			String [] grandChild = new String[10];
			
			int grandParentNum = 0;
			String[] grandParent = new String[10];
			
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext()){
				String tmp = iterator.next().toString();
				
				int len = tmp.length();
				int i = 2;//从左右表的标识位后面的+号开始
				
				if(len == 0)
					continue;
				
				//获取标识位
				char flag = tmp.charAt(0);
				
				if(flag == 'l'){
					String childName = new String();
					while(i < len){
						childName = childName + tmp.charAt(i);
						i++;
					}
					grandChild[grandChildNum] = childName;
					grandChildNum++;
				}else{
					String parentName = new String();
					while(i < len){
						parentName = parentName + tmp.charAt(i);
						i++;
					}
					grandParent[grandParentNum] = parentName;
					grandParentNum++;
				}
			}
			//做笛卡尔乘积.
			if(grandChildNum != 0 && grandParentNum != 0){
				for(int m = 0;m < grandChildNum;m++){
					for(int n = 0;n<grandParentNum;n++){
						if(grandChild[m] != null && grandParent[n] != null){
							context.write(new Text(grandChild[m]), new Text(grandParent[n]));
						}
					}
				}
			}
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InnerQuery.class);
		job.setMapperClass(InnerMapper.class);
		job.setReducerClass(InnerReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		boolean result = job.waitForCompletion(true);
		return result ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new InnerQuery(), args);
		System.exit(result);
	}
	
	
}
