package analyses;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AnalysesWeatherStandardDeviationReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		int qtt = 0;
		double avg = 0;
		double sumVariance = 0.0;
		double variance = 0.0;
		
		try{
            Path pt=new Path("hdfs://localhost:9000/usr/local/hadoop/output/part-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
                        
            while (line != null){
            	StringTokenizer tokenizer = new StringTokenizer(line);
                line=br.readLine();
            }
        }catch(Exception e){
        	
		}
		
		LinkedList<Double> cache = new LinkedList<>();
		
		/**
		 * This sum all input values lines and count how much register has been iterated. 
		 */
		while (values.hasNext()) {
			double _thisValue = values.next().get();

			cache.add(_thisValue);
			
			sum += _thisValue;
			qtt++;
		}

		avg = sum/qtt;
		
		/**
		 * Create a iterator and calculate variance, this is required to standard deviation
		 */
		Iterator<Double> cacheValues = cache.iterator();
		
		while(cacheValues.hasNext()){
			sumVariance += Math.pow((avg - Double.parseDouble(cacheValues.next().toString())),2);
		}	
		
		variance = sumVariance/qtt;
		
		/**
		 * Generate the standard deviation.
		 */
		
		double standardDeviation = Math.sqrt(variance);
		
		output.collect(key, new DoubleWritable(standardDeviation));
	}
}
