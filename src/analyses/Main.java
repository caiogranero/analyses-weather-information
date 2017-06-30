package analyses;

import java.awt.EventQueue;

import javax.swing.JFrame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import javax.swing.JTextField;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.swing.JRadioButton;

public class Main {

	private JFrame frame;
	private JTextField txtAnoInicial;
	private JTextField txtAnoFinal;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Main window = new Main(args);
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public Main(String[] args) {
		initialize(args);
	}
	
	public void readHdfs(){
        try{
            Path pt=new Path("hdfs://localhost:9000/usr/local/hadoop/output/part-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                    System.out.println(line);
                    line=br.readLine();
            }
        }catch(Exception e){
        	
		}
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize(String[] args) {
		frame = new JFrame();
		frame.setBounds(300, 300, 1200, 600);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		txtAnoInicial = new JTextField();
		txtAnoInicial.setBounds(105, 25, 114, 19);
		frame.getContentPane().add(txtAnoInicial);
		txtAnoInicial.setColumns(10);
		
		txtAnoFinal = new JTextField();
		txtAnoFinal.setBounds(362, 25, 114, 19);
		frame.getContentPane().add(txtAnoFinal);
		txtAnoFinal.setColumns(10);
				
		JLabel lblAnoFinal = new JLabel("Ano final");
		lblAnoFinal.setBounds(274, 27, 70, 15);
		frame.getContentPane().add(lblAnoFinal);
		
		JLabel lblAnoInicial = new JLabel("Ano inicial");
		lblAnoInicial.setBounds(12, 27, 105, 15);
		frame.getContentPane().add(lblAnoInicial);
		
		/**
		 ***************************************
		 * Creating metric radion buttons *
		 ***************************************
		 */
		
		JLabel lblEscolhaACaracterstica = new JLabel("Escolha a característica que será analisada");
		lblEscolhaACaracterstica.setBounds(12, 85, 332, 15);
		frame.getContentPane().add(lblEscolhaACaracterstica);
		
		JRadioButton rdbtnTemperatura = new JRadioButton("Temperatura");
		rdbtnTemperatura.setBounds(12, 108, 149, 23);
		frame.getContentPane().add(rdbtnTemperatura);
		
		JRadioButton rdbtnTemperaturaMxima = new JRadioButton("Temperatura máxima");
		rdbtnTemperaturaMxima.setBounds(12, 135, 183, 23);
		frame.getContentPane().add(rdbtnTemperaturaMxima);
		
		JRadioButton rdbtnTemperaturaMnima = new JRadioButton("Temperatura mínima");
		rdbtnTemperaturaMnima.setBounds(12, 162, 231, 25);
		frame.getContentPane().add(rdbtnTemperaturaMnima);
		
		JRadioButton rdbtnVisibilidade= new JRadioButton("Visibilidade");
		rdbtnVisibilidade.setBounds(12, 191, 149, 23);
		frame.getContentPane().add(rdbtnVisibilidade);
		
		JRadioButton rdbtnSLP= new JRadioButton("SLP");
		rdbtnSLP.setBounds(12, 218, 149, 23);
		frame.getContentPane().add(rdbtnSLP);
		
		JRadioButton rdbtnWDSP = new JRadioButton("WDSP");
		rdbtnWDSP.setBounds(12, 244, 149, 23);
		frame.getContentPane().add(rdbtnWDSP);
		
		JRadioButton rdbtnGUST = new JRadioButton("GUST");
		rdbtnGUST.setBounds(12, 271, 149, 23);
		frame.getContentPane().add(rdbtnGUST);
		
		JRadioButton rdbtnSTP = new JRadioButton("STP");
		rdbtnSTP.setBounds(12, 298, 149, 23);
		frame.getContentPane().add(rdbtnSTP);
		
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
		lblAtributoDeAgregao.setBounds(385, 85, 171, 15);
		frame.getContentPane().add(lblAtributoDeAgregao);
		
		JRadioButton rdbtnDia = new JRadioButton("Dia ");
		rdbtnDia.setBounds(385, 123, 149, 23);
		frame.getContentPane().add(rdbtnDia);
		
		JRadioButton rdbtnDiams = new JRadioButton("Dia/Mês");
		rdbtnDiams.setBounds(385, 150, 149, 23);
		frame.getContentPane().add(rdbtnDiams);
		
		JRadioButton rdbtnMs = new JRadioButton("Mês");
		rdbtnMs.setBounds(385, 177, 149, 23);
		frame.getContentPane().add(rdbtnMs);
		
		JRadioButton rdbtnAno = new JRadioButton("Ano");
		rdbtnAno.setBounds(385, 204, 149, 23);
		frame.getContentPane().add(rdbtnAno);
		
		ButtonGroup bgAggregation = new ButtonGroup();
		bgAggregation.add(rdbtnDia);
		bgAggregation.add(rdbtnDiams);
		bgAggregation.add(rdbtnMs);
		bgAggregation.add(rdbtnAno);
		
		JButton btnGerarRelatrio = new JButton("Gerar relatório");
		
		btnGerarRelatrio.addMouseListener(new MouseAdapter() {
			
			@Override
			public void mouseClicked(MouseEvent arg0) {
				
				int dateTo = -1;
				int dateFrom = -1;
				int res;
				int metric = -1;
				
				int[] aggregation = new int[2];
				
				try{
					dateTo = Integer.parseInt(txtAnoInicial.getText());
					dateFrom = Integer.parseInt(txtAnoFinal.getText());
				} catch(NumberFormatException e){
					JOptionPane.showMessageDialog(btnGerarRelatrio, "Você digitou um ano inválido. Digite novamente, por favor.","Atenção!", 0);
					return;
				}
				
				if(rdbtnTemperatura.isSelected()){
					metric = 3;
				} else if(rdbtnTemperaturaMxima.isSelected()){
					metric = 14;
				} else if(rdbtnTemperaturaMnima.isSelected()){
					metric = 15;
				} else if(rdbtnVisibilidade.isSelected()){
					metric = 11;
				} else if(rdbtnSLP.isSelected()){
					metric = 7;
				} else if(rdbtnWDSP.isSelected()){
					metric = 13;
				} else if(rdbtnGUST.isSelected()){
					metric = 16;
				} else if(rdbtnSTP.isSelected()){
					metric = 9;
				} else {
					JOptionPane.showMessageDialog(btnGerarRelatrio, "Parece que você não preencheu um atributo de métrica, selecione um.","Atenção!", 0);
					return;
				}
				
				if(rdbtnDia.isSelected()){
					aggregation[0] = 6;
					aggregation[1] = 8;
				} else if(rdbtnDiams.isSelected()){
					aggregation[0] = 4;
					aggregation[1] = 8;
				} else if(rdbtnMs.isSelected()){
					aggregation[0] = 6;
					aggregation[1] = 8;
				} else if(rdbtnAno.isSelected()){
					aggregation[0] = 0;
					aggregation[1] = 4;
				} else {
					JOptionPane.showMessageDialog(btnGerarRelatrio, "Parece que você não preencheu um parametro de agregação, selecione um.","Atenção!", 0);
					return;
				}
				
				try {
					res = ToolRunner.run(new Configuration(), new AnalysesWeather(dateTo, dateFrom, metric, aggregation), args);
					System.exit(res);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		});
		
		btnGerarRelatrio.setBounds(270, 335, 162, 25);
		frame.getContentPane().add(btnGerarRelatrio);
	}
	
	
}
