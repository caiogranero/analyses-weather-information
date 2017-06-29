package analyses;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class AnalysesWeather extends Configured implements Tool {
	
	private int metric, dateTo, dateFrom;
	private int[] aggregation;
	
	/**
	 * 
	 * @param dateTo { First year we need to process }
	 * @param dateFrom { Last year we need to process }
	 * @param metric { Metric we need to use to calculate statistics }
	 * @param aggregation { Parameter we need to use to group result }
	 */
	public AnalysesWeather(int dateTo, int dateFrom, int metric, int[] aggregation){
		this.dateTo = dateTo;
		this.dateFrom = dateFrom;
		this.aggregation = aggregation;
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
        conf.setInt("metricPosition", getMetric());
        conf.setInt("aggregationFirstPosition", getAggregation()[0]);
        conf.setInt("aggregationLastPosition", getAggregation()[1]);
        
        //Providing the mapper and reducer class names
        conf.setMapperClass(AnalysesWeatherMap.class);
        conf.setReducerClass(AnalysesWeatherReduce.class);
        
        //Set hadoop input path
        String root = "/usr/local/hadoop/input/";
        
        //Set hadoop output path
        Path out = new Path("/usr/local/hadoop/output");
        
        FileSystem hdfs = FileSystem.get(conf);

        // delete output folder if exists
	    if (hdfs.exists(out)) hdfs.delete(out, true);
        
	    //Define output folder
        FileOutputFormat.setOutputPath(conf, out);
        
	    //Read all years folder
	    for(int i = getDateTo(); i <= getDateFrom(); i++){
	    	FileInputFormat.addInputPath(conf, new Path(root+i+"/*"));
	    }
	   
        JobClient.runJob(conf);
        return 0;
    }

	public int[] getAggregation() {
		return aggregation;
	}

	public int getMetric() {
		return metric;
	}

	public int getDateTo() {
		return dateTo;
	}

	public int getDateFrom() {
		return dateFrom;
	}
}