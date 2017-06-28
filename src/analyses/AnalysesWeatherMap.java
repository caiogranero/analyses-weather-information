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
    private int aggColumn;
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        setAggColumn(job.getInt("aggColumn", -1));
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
        
        if(!isHeader()){
	        int i = 0;
	        
	        //Read each column finding what user choose before execute	
	        while (tokenizer.hasMoreTokens()) {
	        	String token = tokenizer.nextToken();
	        	
	        	if(i == YEARMODA){ //
	        		setAggregation(token.substring(0, 4));
	        	}
	        	
	        	if(i == getAggColumn()){
	        		setMetric(checkMissing(Double.parseDouble(token)));
	        	}
	        	
	            i++;
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

	public int getAggColumn() {
		return aggColumn;
	}

	public void setAggColumn(int aggColumn) {
		this.aggColumn = aggColumn;
	}
}