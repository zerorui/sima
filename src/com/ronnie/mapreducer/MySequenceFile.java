package com.ronnie.mapreducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * ����sequenceFile �ļ�.
 * 	sequenceFile : ��¼key/value �Ե��б�.�����л��Ķ������ļ�.
 * @author ronnie
 */
public class MySequenceFile {
	public static String[] values = {
		"hello world",
		"hello hadoop",
		"hello java",
		"hello man"
	};
	public static void main(String[] args) throws URISyntaxException, IOException {
		//uri:��Ҫ����sequenceFile�ļ���λ��.
		URI uri = new URI(args[0]);
		Configuration conf = new Configuration();
//		FileSystem fileSystem = FileSystem.get(uri, conf);
		
		Path path =  new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(path),
					Writer.keyClass(key.getClass()),Writer.valueClass(value.getClass()),
					Writer.compression(CompressionType.BLOCK));//��ʾ���ÿ�ѹ��.
		
		for(int i = 0 ; i < 10000; i++){
			key.set(i + 1);
			value.set(values[i % values.length]);
			writer.append(key, value);
		}
	}
	public  void readSequenceFile() throws Exception{
		URI uri = new URI("");
		Configuration conf = new Configuration();
		
		Path path = new Path(uri);
		
		@SuppressWarnings("resource")
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
		
		//�õ�key��value
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		long position = reader.getPosition();
		while(reader.next(key, value)){
			String syncSeen = reader.syncSeen() ? "*" : "";
			position = reader.getPosition();//��ʼ��һ����¼.
			System.out.println(position + syncSeen);
		}
	}
	public void compressSequenceFile(){
		
	}
}
