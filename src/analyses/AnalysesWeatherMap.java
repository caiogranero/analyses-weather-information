package analyses;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;

public class AnalysesWeatherMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private static int YEARMODA = 2;
	
	private DoubleWritable metric = new DoubleWritable();
    private Text aggregation = new Text();
    private boolean isHeader = true;
    private int metricPosition, aggregationFirstPosition, aggregationLastPosition;
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        setMetricPosition(job.getInt("metricPosition", -1));
        setAggregationFirstPosition(job.getInt("aggregationFirstPosition", -1));
        setAggregationLastPosition(job.getInt("aggregationLastPosition", -1));
    }
    
    public double checkMissing(Double value){
    	if(value == 999.9 || value == 9999.9){
    		return 0.0;
    	}
    	return value;
    }
    
    //map method that performs the tokenizer job and framing the initial key value pairs
    // after all lines are converted into key-value pairs, reducer is called.
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
    	
    	//read one line per time
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        //Don't read the first line. We don't need header
        if(!isHeader()){
	        int iColumn = 0;
	        
	        //Read each column finding what user choose before execute	
	        while (tokenizer.hasMoreTokens()) {
	        	String token = tokenizer.nextToken();
	        	
	        	//Check if the current column index are the date column
	        	if(iColumn == YEARMODA){ //
	        		setAggregation(token.substring(getAggregationFirstPosition(), getAggregationLastPosition()));
	        	}
	        	
	        	//Check if the current column index are the param column
	        	if(iColumn == getMetricPosition()){
	        		setMetric(checkMissing(Double.parseDouble(token)));
	        	}
	        	
	        	iColumn++;
	        }
	        
	        output.collect(getAggregation(), getMetric());
	        
        } else {
        	setHeader(false);
        }
    }

	public DoubleWritable getMetric() {
		return metric;
	}

	public void setMetric(Double metric) {
		this.metric.set(metric);
	}

	public Text getAggregation() {
		return aggregation;
	}

	public void setAggregation(String string) {
		this.aggregation.set(string);
	}

	public boolean isHeader() {
		return isHeader;
	}

	public void setHeader(boolean isHeader) {
		this.isHeader = isHeader;
	}

	public int getAggregationFirstPosition() {
		return aggregationFirstPosition;
	}

	public void setAggregationFirstPosition(int aggregationFirstPosition) {
		this.aggregationFirstPosition = aggregationFirstPosition;
	}

	public int getAggregationLastPosition() {
		return aggregationLastPosition;
	}

	public void setAggregationLastPosition(int aggregationLastPosition) {
		this.aggregationLastPosition = aggregationLastPosition;
	}

	public int getMetricPosition() {
		return metricPosition;
	}

	public void setMetricPosition(int metricPosition) {
		this.metricPosition = metricPosition;
	}

}