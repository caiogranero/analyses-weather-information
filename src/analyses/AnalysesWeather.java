package analyses;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class AnalysesWeather extends Configured implements Tool {
	
	private static int agg, metric, dateTo, dateFrom;
	
	/**
	 * Return corresponding position from dataset by selected aggregation parameter 
	 * @param insertedValue
	 * @return
	 */
	public int getAggColumnIndex(int insertedValue){
		switch(insertedValue){
			case 1: //TEMP
				return 3;
			case 2: //MAX
				return 14;
			case 3: //MIN
				return 15;
			case 4: //VISIB
				return 11;
			case 5: //WDSP
				return 13;
			case 6: //SLP
				return 7;
			default:
				return -1;
		}
	}
	
	public static void askYearRange(Scanner sc){
    	System.out.print("Por favor, digite o range de anos que você deseja analisar no formato: 1990/2017. \n");
        String yearRange = sc.next();
        
        try{
        	dateTo = Integer.parseInt(yearRange.substring(0, 4));
        	dateFrom = Integer.parseInt(yearRange.substring(5, 9));
        } catch(Exception e){
        	System.out.println("Ops, parece que você errou, mas não se desespere.\n");
        	askYearRange(sc);
        }
    }
    
    public static void askAggregation(Scanner sc){
    	System.out.println("Por favor, selecione qual o atributo que você deseja que seja feita a agregação.\n");
    	
        System.out.print("------------------------------\n");
        System.out.print("1. Temperatura\n");
        System.out.print("2. Temperatura Máxima\n");
        System.out.print("3. Temperatura Mínima\n");
        System.out.print("4. Visibilidade\n");
        System.out.print("5. WDSP\n");
        System.out.print("6. SLP\n");
        System.out.print("------------------------------\n");
        
        try{
        	agg = sc.nextInt();
        } catch(NumberFormatException e){
        	System.out.println("Ops, parece que você errou, mas não se desespere.\n");
        	askAggregation(sc);
        }        
    }
	
    public int run(String[] args) throws Exception {
    	
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), AnalysesWeather.class);
        conf.setJobName("AnalysesWeather");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        
        //Send some variable to map function
        conf.setInt("aggColumn", getAggColumnIndex(agg));
        
        //Providing the mapper and reducer class names
        conf.setMapperClass(AnalysesWeatherMap.class);
        conf.setReducerClass(AnalysesWeatherReduce.class);
        
        //Set hadoop input path
        String root = "/usr/local/hadoop/input/";
        
        Path out = new Path("/usr/local/hadoop/output");
        
        FileSystem hdfs = FileSystem.get(conf);

        // delete output folder if exists
	    if (hdfs.exists(out)) hdfs.delete(out, true);
        
	    //Define output folder
        FileOutputFormat.setOutputPath(conf, out);
        
	    //Read all defined years folder
	    for(int i = dateTo; i <= dateFrom; i++){
	    	FileInputFormat.addInputPath(conf, new Path(root+i+"/*"));
	    }
	   
        JobClient.runJob(conf);
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        
        System.out.println("Bem vindo.\n");
                
        askYearRange(sc);
        askAggregation(sc);
        metric = 1;
        
        
        int res = ToolRunner.run(new Configuration(), new AnalysesWeather(), args);
        System.exit(res);
    }
}