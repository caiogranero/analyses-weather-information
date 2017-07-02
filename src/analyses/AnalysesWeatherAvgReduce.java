package analyses;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AnalysesWeatherAvgReduce extends MapReduceBase
		implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	/*
	 * iterates through all the values available with a key and add them
	 * together and give the final result as the key and sum of its values
	 */
	private double getAverage(Iterator<DoubleWritable> values){
		int sum = 0;
		int qtd = 0;
		
		while (values.hasNext()) {
			sum += values.next().get();
			qtd++;
		}
		
		return sum/qtd;
	};
	
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {

		output.collect(key, new DoubleWritable(getAverage(values)));
		
	}

}
