package analyses;

import java.awt.Dimension;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class AnalysesWeatherChart extends javax.swing.JFrame {
	
	private static final long serialVersionUID = 1L;
	private ChartPanel chartPanel;
	private DefaultCategoryDataset dataset;
	private JFreeChart freeChart;
	
	public AnalysesWeatherChart(JFrame frame) {
        this.dataset = new DefaultCategoryDataset();

    	this.freeChart =  ChartFactory.createLineChart(
                "Análise do clima", "", "",
                this.dataset, PlotOrientation.VERTICAL, true, true, false);
        
    	ChartPanel cp = new ChartPanel(freeChart) {
			private static final long serialVersionUID = 1L;

			@Override
            public Dimension getPreferredSize() {
                return new Dimension(600, 300);
            }
        };
    	
        this.chartPanel = cp;
        
        cp.setBounds(440, 85, 780, 520);
        frame.getContentPane().add(cp); 
    }

	public void newPoint(double value, String series, String category){
        getDataset().addValue(value, series, category);
	}
	
	public ChartPanel getChartPanel() {
		return chartPanel;
	}

	public DefaultCategoryDataset getDataset() {
		return dataset;
	}

	public void setDataset(DefaultCategoryDataset dataset) {
		this.dataset = dataset;
	}

	public JFreeChart getFreeChart() {
		return freeChart;
	}
}
