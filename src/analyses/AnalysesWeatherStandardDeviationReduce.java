package analyses;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

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
		int sum = 0;
		int qtt = 0;
		double avg = 0;
		double sumVariance = 0.0;
		double variance = 0.0;

		LinkedList<Double> cache = new LinkedList<>();

		/**
		 * This sum all input values lines and count how much register has been
		 * iterated.
		 */
		while (values.hasNext()) {
			double _thisValue = values.next().get();

			cache.add(_thisValue);

			sum += _thisValue;
			qtt++;
		}

		avg = sum / qtt;

		/**
		 * Create a iterator and calculate variance, this is required to
		 * standard deviation
		 */
		Iterator<Double> cacheValues = cache.iterator();

		while (cacheValues.hasNext()) {
			sumVariance += Math.pow((avg - Double.parseDouble(cacheValues.next().toString())), 2);
		}

		variance = sumVariance / qtt;

		/**
		 * Generate the standard deviation.
		 */

		double standardDeviation = Math.sqrt(variance);

		output.collect(key, new DoubleWritable(standardDeviation));
	}
}
