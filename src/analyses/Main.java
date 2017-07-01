package analyses;

import java.awt.EventQueue;

public class Main {
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					AnalysesWeatherVisualization window = new AnalysesWeatherVisualization(args);
					window.getFrame().setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
}
