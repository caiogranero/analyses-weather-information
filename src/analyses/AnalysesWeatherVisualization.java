package analyses;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class AnalysesWeatherVisualization {

	private JFrame frame;
	private AnalysesWeatherChart chart;
	private String labelAxisX, labelAxisY;
	private int prevision, dateTo, dateFrom;
	private boolean canMakePrevision = false;
	
	/**
	 * Create the application.
	 */
	public AnalysesWeatherVisualization(String[] args) {
		initialize(args);
	}	
	
	/**
	 * Read HDFS output generate by mapreduce, then start data in  graph
	 */
	public void readHdfsOutput(String type) {
		
		String series = "";
		
		if(type == "avg"){
			series = "Média";
		} else if(type == "std"){
			series = "Desvio Padrão";
		}
		
		try {
			Path pt = new Path("hdfs://localhost:9000/usr/local/hadoop/output/"+type+"/part-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			OrdinaryLeastSquares ols = new OrdinaryLeastSquares();
			
			// read one line per time
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				
				// Parse the line, result in key, value
				StringTokenizer tokenizer = new StringTokenizer(line);
					
				String category = tokenizer.nextToken();
				double value = Double.parseDouble(tokenizer.nextToken());
				
				ols.getSimpleRegression().addData(Double.parseDouble(category), value);
				
				//Insert axis labels
		        getChart().getFreeChart().getCategoryPlot().getRangeAxis().setAttributedLabel(getLabelAxisY());
		        getChart().getFreeChart().getCategoryPlot().getDomainAxis().setAttributedLabel(getLabelAxisX());
		        
				// Insert data in graph
				getChart().newPoint(value, series, category); 
			}			
			
			if(isCanMakePrevision()){
				//This for, calculate the defined futures points
				for(int i = getDateFrom()+1; i <= getPrevision(); i++){
					getChart().newPoint(ols.getFutureY(i), series + " previsão", Integer.toString(i));
				}
			}
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize(String[] args) {
		this.frame = new JFrame();
		getFrame().setBounds(300, 300, 1300, 900);
		getFrame().setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		getFrame().getContentPane().setLayout(null);

		JTextField txtAnoInicial = new JTextField();
		txtAnoInicial.setBounds(105, 25, 114, 19);
		getFrame().getContentPane().add(txtAnoInicial);
		txtAnoInicial.setColumns(10);

		JTextField txtAnoFinal = new JTextField();
		txtAnoFinal.setBounds(362, 25, 114, 19);
		getFrame().getContentPane().add(txtAnoFinal);
		txtAnoFinal.setColumns(10);

		JTextField txtPrevisao = new JTextField();
		txtPrevisao.setBounds(730, 25, 114, 19);
		frame.getContentPane().add(txtPrevisao);
		txtPrevisao.setColumns(10);
		
		JLabel lblAnoFinal = new JLabel("Ano final");
		lblAnoFinal.setBounds(274, 27, 70, 15);
		getFrame().getContentPane().add(lblAnoFinal);

		JLabel lblAnoInicial = new JLabel("Ano inicial");
		lblAnoInicial.setBounds(12, 27, 105, 15);
		getFrame().getContentPane().add(lblAnoInicial);

		JLabel lblDataFim = new JLabel("Último ano da previsão");
		lblDataFim.setBounds(549, 27, 163, 15);
		frame.getContentPane().add(lblDataFim);
		
		/**
		 ***************************************
		 * Creating metric radion buttons *
		 ***************************************
		 */

		JLabel lblEscolhaACaracterstica = new JLabel("Escolha a característica que será analisada");
		lblEscolhaACaracterstica.setBounds(12, 80, 332, 15);
		getFrame().getContentPane().add(lblEscolhaACaracterstica);

		JRadioButton rdbtnTemperatura = new JRadioButton("Temperatura");
		rdbtnTemperatura.setBounds(12, 105, 149, 23);
		getFrame().getContentPane().add(rdbtnTemperatura);

		JRadioButton rdbtnTemperaturaMxima = new JRadioButton("Temperatura máxima");
		rdbtnTemperaturaMxima.setBounds(12, 130, 183, 23);
		getFrame().getContentPane().add(rdbtnTemperaturaMxima);

		JRadioButton rdbtnTemperaturaMnima = new JRadioButton("Temperatura mínima");
		rdbtnTemperaturaMnima.setBounds(12, 155, 231, 25);
		getFrame().getContentPane().add(rdbtnTemperaturaMnima);

		JRadioButton rdbtnVisibilidade= new JRadioButton("Visibilidade");
		rdbtnVisibilidade.setBounds(12, 180, 149, 23);
		getFrame().getContentPane().add(rdbtnVisibilidade);

		JRadioButton rdbtnSLP= new JRadioButton("Pressão do mar");
		rdbtnSLP.setBounds(12, 205, 149, 23);
		getFrame().getContentPane().add(rdbtnSLP);

		JRadioButton rdbtnWDSP = new JRadioButton("Humidade");
		rdbtnWDSP.setBounds(12, 230, 149, 23);
		getFrame().getContentPane().add(rdbtnWDSP);

		JRadioButton rdbtnGUST = new JRadioButton("Rajada de vento");
		rdbtnGUST.setBounds(12, 255, 149, 23);
		getFrame().getContentPane().add(rdbtnGUST);

		JRadioButton rdbtnSTP = new JRadioButton("Pressão");
		rdbtnSTP.setBounds(12, 280, 149, 23);
		getFrame().getContentPane().add(rdbtnSTP);

		ButtonGroup bgMetric = new ButtonGroup();
		bgMetric.add(rdbtnTemperatura);
		bgMetric.add(rdbtnTemperaturaMxima);
		bgMetric.add(rdbtnTemperaturaMnima);
		bgMetric.add(rdbtnVisibilidade);
		bgMetric.add(rdbtnSLP);
		bgMetric.add(rdbtnWDSP);
		bgMetric.add(rdbtnGUST);
		bgMetric.add(rdbtnSTP);

		/**
		 ***************************************
		 * Creating aggregation radion buttons *
		 ***************************************
		 */
		    
		JLabel lblAtributoDeAgregao = new JLabel("Atributo de agregação");
		lblAtributoDeAgregao.setBounds(12, 320, 171, 15);
		getFrame().getContentPane().add(lblAtributoDeAgregao);

		JRadioButton rdbtnDia = new JRadioButton("Dia ");
		rdbtnDia.setBounds(12, 355, 149, 23);
		getFrame().getContentPane().add(rdbtnDia);

		JRadioButton rdbtnMs = new JRadioButton("Mês");
		rdbtnMs.setBounds(12, 380, 149, 23);
		getFrame().getContentPane().add(rdbtnMs);

		JRadioButton rdbtnAno = new JRadioButton("Ano");
		rdbtnAno.setBounds(12, 405, 149, 23);
		getFrame().getContentPane().add(rdbtnAno);
		
		ButtonGroup bgAggregation = new ButtonGroup();
		bgAggregation.add(rdbtnDia);
		bgAggregation.add(rdbtnMs);
		bgAggregation.add(rdbtnAno);
		
		this.chart = new AnalysesWeatherChart(getFrame());

		JButton btnGerarRelatrio = new JButton("Gerar relatório");

		btnGerarRelatrio.addMouseListener(new MouseAdapter() {

			@Override
			public void mouseClicked(MouseEvent arg0) {

				int metric = -1;
				int dateTo, dateFrom, prevision;
				
				int[] aggregation = new int[2];

				try {
					
					dateTo = Integer.parseInt(txtAnoInicial.getText());
					dateFrom = Integer.parseInt(txtAnoFinal.getText());
					if (!rdbtnAno.isSelected()) {
						prevision = Integer.parseInt(txtAnoFinal.getText());
					} else {
						prevision = Integer.parseInt(txtPrevisao.getText());
					}
					
				} catch (NumberFormatException e) {
					JOptionPane.showMessageDialog(btnGerarRelatrio,
							"Você digitou um ano inválido. Digite novamente, por favor.", "Atenção!", 0);
					return;
				}
				
				if(dateTo > dateFrom){
					JOptionPane.showMessageDialog(btnGerarRelatrio,
							"Você digitou o ano final maior do que o inicial. Digite novamente, por favor.", "Atenção!", 0);
					return;
				} else if(dateFrom > prevision){
					JOptionPane.showMessageDialog(btnGerarRelatrio,
							"Você digitou o ano de previsão menor do que o ano final. Digite novamente, por favor.", "Atenção!", 0);
					return;
				} else {
					setDateTo(dateTo);
					setDateFrom(dateFrom);
					setPrevision(prevision);
				}

				if (rdbtnTemperatura.isSelected()) {
					metric = 3;
					setLabelAxisY("Temperatura");
				} else if (rdbtnTemperaturaMxima.isSelected()) {
					metric = 14;
					setLabelAxisY("Temperatura Máxima");
				} else if (rdbtnTemperaturaMnima.isSelected()) {
					metric = 15;
					setLabelAxisY("Temperatura Mínima");
				} else if (rdbtnVisibilidade.isSelected()) {
					metric = 11;
					setLabelAxisY("Visibilidade");
				} else if (rdbtnSLP.isSelected()) {
					metric = 7;
					setLabelAxisY("SLP");
				} else if (rdbtnWDSP.isSelected()) {
					metric = 13;
					setLabelAxisY("WDSP");
				} else if (rdbtnGUST.isSelected()) {
					metric = 16;
					setLabelAxisY("GUST");
				} else if (rdbtnSTP.isSelected()) {
					metric = 9;
					setLabelAxisY("STP");
				} else {
					JOptionPane.showMessageDialog(btnGerarRelatrio,
							"Parece que você não preencheu um atributo de métrica, por favor selecione um.", "Atenção!", 0);
					return;
				}

				if (rdbtnDia.isSelected()) {
					aggregation[0] = 6;
					aggregation[1] = 8;
					setLabelAxisX("Dia");
				} else if (rdbtnMs.isSelected()) {
					aggregation[0] = 4;
					aggregation[1] = 6;
					setLabelAxisX("Mês");
				} else if (rdbtnAno.isSelected()) {
					aggregation[0] = 0;
					aggregation[1] = 4;
					setLabelAxisX("Ano");
					setCanMakePrevision(true);
				} else {
					JOptionPane.showMessageDialog(btnGerarRelatrio,
							"Parece que você não preencheu um parametro de agregação, por favor selecione um.", "Atenção!", 0);
					return;
				}

				try {
					
					//Run Hadoop, run!
					ToolRunner.run(new Configuration(),
							new AnalysesWeather(getDateTo(), getDateFrom(), metric, aggregation), args);
					
					//Clear graph content, this make possible to run more than one time.
					getChart().getDataset().clear();
					
					readHdfsOutput("avg");
					readHdfsOutput("std");
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		btnGerarRelatrio.setBounds(930, 22, 140, 25);
		getFrame().getContentPane().add(btnGerarRelatrio);
	}

	public JFrame getFrame() {
		return frame;
	}

	public AnalysesWeatherChart getChart() {
		return chart;
	}

	public void setChart(AnalysesWeatherChart chart) {
		this.chart = chart;
	}

	public String getLabelAxisX() {
		return labelAxisX;
	}

	public void setLabelAxisX(String labelAxisX) {
		this.labelAxisX = labelAxisX;
	}

	public String getLabelAxisY() {
		return labelAxisY;
	}

	public void setLabelAxisY(String labelAxisY) {
		this.labelAxisY = labelAxisY;
	}

	public int getPrevision() {
		return prevision;
	}

	public void setPrevision(int prevision) {
		this.prevision = prevision;
	}

	public int getDateTo() {
		return dateTo;
	}

	public void setDateTo(int dateTo) {
		this.dateTo = dateTo;
	}

	public int getDateFrom() {
		return dateFrom;
	}

	public void setDateFrom(int dateFrom) {
		this.dateFrom = dateFrom;
	}

	public boolean isCanMakePrevision() {
		return canMakePrevision;
	}

	public void setCanMakePrevision(boolean canMakePrevision) {
		this.canMakePrevision = canMakePrevision;
	}
}
