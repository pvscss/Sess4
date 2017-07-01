import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class InvalidRecordsMapper 
extends Mapper<LongWritable,Text,Text,IntWritable> 
{
	private final static String Pattern="|";
	private final static String NA="NA";
	
	  private final static IntWritable one = new IntWritable(1);	 
	  //private Text word = new Text();
	
	@Override
	public void map(LongWritable key,Text value, Context context)
	throws IOException,InterruptedException
	{
		String line=value.toString();
		
		StringTokenizer tokenizer=new StringTokenizer(line,Pattern);
		
		String Err=tokenizer.nextToken();
		String Err1=tokenizer.nextToken();
		
		if(!Err.equalsIgnoreCase(NA) && !Err1.equalsIgnoreCase(NA))
			{
				context.write(value,one);				
			}
			
		//}
		
	}
}
