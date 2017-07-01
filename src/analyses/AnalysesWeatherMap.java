package analyses;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AnalysesWeatherMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static int YEARMODA = 2;

	private DoubleWritable metric = new DoubleWritable();
	private Text aggregation = new Text();
	private boolean isHeader = true;
	private int metricPosition, aggregationFirstPosition, aggregationLastPosition;
	private boolean canUse = true; // if have a invalid number in dataset, don't
									// use it.

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		setMetricPosition(job.getInt("metricPosition", -1));
		setAggregationFirstPosition(job.getInt("aggregationFirstPosition", -1));
		setAggregationLastPosition(job.getInt("aggregationLastPosition", -1));
	}

	/**
	 * Metric value as 999.9 or 9999.9, its equals 0
	 * 
	 * @param value
	 * @return
	 */
	public double checkMissing(Double value) {
		if (value == 999.9 || value == 9999.9) {
			return 0.0;
		}
		return value;
	}

	// map method that performs the tokenizer job and framing the initial key
	// value pairs
	// after all lines are converted into key-value pairs, reducer is called.
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {

		// read one line per time
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		// Don't read the first line. We don't need header
		if (!isHeader()) {
			int iColumn = 0;

			// Read each column finding what user choose before execute
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();

				// Check if the current column index are the date column
				if (iColumn == YEARMODA) { //
					setAggregation(token.substring(getAggregationFirstPosition(), getAggregationLastPosition()));
				}

				// Check if the current column index are the param column
				if (iColumn == getMetricPosition()) {

					// Some dataset have * as last character, we need to remove
					// the *, then use the value.
					if (token.substring(token.length() - 1) == "*") {
						token = token.substring(0, token.length() - 1);
					}

					try {
						setMetric(checkMissing(Double.parseDouble(token)));
					} catch (NumberFormatException e) {
						setCanUse(false);
						return;
					}

				}

				iColumn++;
			}

			// Validate if the current line is a validate number
			if (isCanUse()) {
				output.collect(getAggregation(), getMetric());
			} else {
				setCanUse(true);
			}

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

	public boolean isCanUse() {
		return canUse;
	}

	public void setCanUse(boolean canUse) {
		this.canUse = canUse;
	}

}