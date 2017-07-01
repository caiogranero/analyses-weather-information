package analyses;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

public class AnalysesWeather extends Configured implements Tool {

	private int metric, dateTo, dateFrom;
	private int[] aggregation;
	private JobConf confAvg;
	private JobConf confStandard;
	/**
	 * 
	 * @param dateTo
	 *            { First year we need to process }
	 * @param dateFrom
	 *            { Last year we need to process }
	 * @param metric
	 *            { Metric we need to use to calculate statistics }
	 * @param aggregation
	 *            { Parameter we need to use to group result }
	 */
	public AnalysesWeather(int dateTo, int dateFrom, int metric, int[] aggregation) {
		this.dateTo = dateTo;
		this.dateFrom = dateFrom;
		this.aggregation = aggregation;
		this.metric = metric;
	}

	public void setAvgJob() throws IOException{
		// creating a JobConf object and assigning a job name for identification purposes
		getConfAvg().setJobName("AnalysesWeatherAvg");
	
		// Setting configuration object with the Data Type of output Key and Value
		getConfAvg().setOutputKeyClass(Text.class);
		getConfAvg().setOutputValueClass(DoubleWritable.class);
	
		// Send some variable to map function
		getConfAvg().setInt("metricPosition", getMetric());
		getConfAvg().setInt("aggregationFirstPosition", getAggregation()[0]);
		getConfAvg().setInt("aggregationLastPosition", getAggregation()[1]);
	
		// Providing the mapper and reducer class names
		getConfAvg().setMapperClass(AnalysesWeatherMap.class);
	
		getConfAvg().setReducerClass(AnalysesWeatherAvgReduce.class);
	
		// Set hadoop output path
		Path out = new Path("/usr/local/hadoop/output/avg");
	
		FileSystem hdfs = FileSystem.get(getConfAvg());
	
		String inputAvg = "/usr/local/hadoop/input/";
		
		// delete output folder if exists
		if (hdfs.exists(out))
			hdfs.delete(out, true);
	
		// Read all years folder
		for (int i = getDateTo(); i <= getDateFrom(); i++) {
			FileInputFormat.addInputPath(getConfAvg(), new Path(inputAvg + i + "/*"));
		}
		
		// Define output folder
		FileOutputFormat.setOutputPath(getConfAvg(), out);
	}
	
	
	public int run(String[] args) throws Exception {

		// creating a JobConf object and assigning a job name for identification purposes
		this.confAvg = new JobConf(getConf(), AnalysesWeather.class);		
		setAvgJob();
		
		// creating a JobConf object and assigning a job name for identification purposes
		this.confStandard = new JobConf(getConf(), AnalysesWeather.class);				
		setAvgStandard();

		JobClient.runJob(getConfAvg());
		JobClient.runJob(getConfStandard());

		return 0;
	}

	private void setAvgStandard() throws IOException {
		// TODO Auto-generated method stub
		// creating a JobConf object and assigning a job name for identification purposes
		getConfStandard().setJobName("AnalysesWeatherStd");
	
		// Setting configuration object with the Data Type of output Key and Value
		getConfStandard().setOutputKeyClass(Text.class);
		getConfStandard().setOutputValueClass(DoubleWritable.class);
	
		// Send some variable to map function
		getConfStandard().setInt("metricPosition", getMetric());
		getConfStandard().setInt("aggregationFirstPosition", getAggregation()[0]);
		getConfStandard().setInt("aggregationLastPosition", getAggregation()[1]);
	
		// Providing the mapper and reducer class names
		getConfStandard().setMapperClass(AnalysesWeatherMap.class);
	
		getConfStandard().setReducerClass(AnalysesWeatherStandardDeviationReduce.class);
	
		// Set hadoop output path
		Path out = new Path("/usr/local/hadoop/output/std");
	
		FileSystem hdfs = FileSystem.get(getConfStandard());
	
		// delete output folder if exists
		if (hdfs.exists(out))
			hdfs.delete(out, true);
		
		String inputAvg = "/usr/local/hadoop/input/";
		
		// Read all years folder
		for (int i = getDateTo(); i <= getDateFrom(); i++) {
			FileInputFormat.addInputPath(getConfStandard(), new Path(inputAvg + i + "/*"));
		}
		
		// Define output folder
		FileOutputFormat.setOutputPath(getConfStandard(), out);
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

	public JobConf getConfAvg() {
		return confAvg;
	}

	public void setConfAvg(JobConf confAvg) {
		this.confAvg = confAvg;
	}

	public JobConf getConfStandard() {
		return confStandard;
	}

	public void setConfStandard(JobConf confStandard) {
		this.confStandard = confStandard;
	}
}