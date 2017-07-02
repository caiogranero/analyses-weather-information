package analyses;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
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

public class AnalysesWeatherStandardDeviationReduce extends MapReduceBase
		implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		int qtt = 0;
		double sumVariance = 0.0;
		double variance = 0.0;

		/**
		 * Open avg map reduce output to get the current average, then, save in a HashMap all values.
		 */
		
		Path pt = new Path("hdfs://localhost:9000/usr/local/hadoop/output/avg/part-00000");
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<Integer, Double> average = new HashMap<Integer, Double>();
		
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			
			// Parse the line, result in key, value
			StringTokenizer tokenizer = new StringTokenizer(line);
				
			int category = Integer.parseInt(tokenizer.nextToken());
			double value = Double.parseDouble(tokenizer.nextToken());
			
			average.put(category, value);
		}

		while (values.hasNext()) {
			sumVariance += Math.pow((values.next().get() - average.get(Integer.parseInt(key.toString()))), 2);
			qtt++;
		}
		
		variance = sumVariance / (qtt-1);
		
		/**
		 * Generate the standard deviation.
		 */

		double standardDeviation = Math.sqrt(variance);
		
		output.collect(key, new DoubleWritable(standardDeviation));
	}
}
