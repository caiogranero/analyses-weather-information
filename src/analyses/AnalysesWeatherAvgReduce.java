package analyses;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AnalysesWeatherAvgReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		int sum = 0;
		int qtd = 0;
		double avg = 0;
		
		/*
		 * iterates through all the values available with a key and add them
		 * together and give the final result as the key and sum of its values
		 */
		while (values.hasNext()) {
			sum += values.next().get();
			qtd++;
		}
		
		avg = sum/qtd;
		
		output.collect(key, new DoubleWritable(avg));
	}

}
