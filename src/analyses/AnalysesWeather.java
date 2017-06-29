package analyses;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class AnalysesWeather extends Configured implements Tool {
	
	private int agg, dateTo, dateFrom;
	private int[] metric;
	
	public AnalysesWeather(int dateTo, int dateFrom, int[] metric, int agg){
		this.dateTo = dateTo;
		this.dateFrom = dateFrom;
		this.agg = agg;
		this.metric = metric;
	}
	
    public int run(String[] args) throws Exception {
    	
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), AnalysesWeather.class);
        conf.setJobName("AnalysesWeather");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        
        //Send some variable to map function
        conf.setInt("aggColumn", getAgg());
        conf.setInt("iMetricPosition", getMetric()[0]);
        conf.setInt("fMetricPosition", getMetric()[1]);
        
        //Providing the mapper and reducer class names
        conf.setMapperClass(AnalysesWeatherMap.class);
        conf.setReducerClass(AnalysesWeatherReduce.class);
        
        //Set hadoop input path
        String root = "/usr/local/hadoop/input/";
        
        Path out = new Path("/usr/local/hadoop/output");
        
        FileSystem hdfs = FileSystem.get(conf);

        // delete output folder if exists
	    if (hdfs.exists(out)) hdfs.delete(out, true);
        
	    //Define output folder
        FileOutputFormat.setOutputPath(conf, out);
        
	    //Read all defined years folder
	    for(int i = getDateTo(); i <= getDateFrom(); i++){
	    	FileInputFormat.addInputPath(conf, new Path(root+i+"/*"));
	    }
	   
        JobClient.runJob(conf);
        return 0;
    }

	public int getAgg() {
		return agg;
	}

	public int[] getMetric() {
		return metric;
	}

	public int getDateTo() {
		return dateTo;
	}

	public int getDateFrom() {
		return dateFrom;
	}
}