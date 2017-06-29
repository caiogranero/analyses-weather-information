package analyses;

import java.awt.EventQueue;

import javax.swing.JFrame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import javax.swing.JTextField;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
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

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize(String[] args) {
		frame = new JFrame();
		frame.setBounds(300, 300, 800, 400);
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
		rdbtnWDSP.setBounds(8, 243, 149, 23);
		frame.getContentPane().add(rdbtnWDSP);
		
		ButtonGroup bgAgg = new ButtonGroup();
		bgAgg.add(rdbtnTemperatura);
		bgAgg.add(rdbtnTemperaturaMxima);
		bgAgg.add(rdbtnTemperaturaMnima);
		bgAgg.add(rdbtnVisibilidade);
		bgAgg.add(rdbtnSLP);
		bgAgg.add(rdbtnWDSP);
		
		JLabel lblEscolhaACaracterstica = new JLabel("Escolha a característica que será analisada");
		lblEscolhaACaracterstica.setBounds(12, 112, 70, 15);
		frame.getContentPane().add(lblEscolhaACaracterstica);
		
		JLabel lblAtributoQueSer = new JLabel("Atributo que será gerado as estatísticas");
		lblAtributoQueSer.setBounds(12, 85, 304, 15);
		frame.getContentPane().add(lblAtributoQueSer);
			
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
		
		ButtonGroup bgMetric = new ButtonGroup();
		bgMetric.add(rdbtnDia);
		bgMetric.add(rdbtnDiams);
		bgMetric.add(rdbtnMs);
		bgMetric.add(rdbtnAno);
		
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
		
		btnGerarRelatrio.setBounds(276, 242, 162, 25);
		frame.getContentPane().add(btnGerarRelatrio);
	}
}
