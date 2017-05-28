package exp_merge_ob;

import static java.lang.System.out;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import basic.btree.*;
import basic.util.DataCreator;
import basic.util.DataRetriever;

public class exp_ob_memory1 {
	
	static int num_lines_1m=47660;//number of lines of 1MB BAT file
	static int num_lines;//number of lines in the tbat and bat files
	static int max_exp_times;//maximum iteration times of experiment
	static ArrayList<Double> pers=new ArrayList<Double>();//update percentages
	static int appendix_num_split=10; //number of split files for appendixes
	static double sel_per=0.1;//selection percentage
	static String result_dir= "data/exp_merge_ob/";
	static String data_dir="data/";
	static String tbat_file_name_original=data_dir+"tbat.txt";
	static String tbat_file_name_copy1=tbat_file_name_original.substring(0, tbat_file_name_original.length()-4)+"_cp1.txt";
    static String tbat_file_name_copy2=tbat_file_name_original.substring(0, tbat_file_name_original.length()-4)+"_cp2.txt";
    
	public static void main(String[] args) throws Exception{
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		final String result_file_name=result_dir+"result-ob-memory.txt";
		PrintWriter result_file = null;
		if(args.length < 3){
			out.println("OB-Tree Loading Memory Test: please input num_lines "
					+ " per1 per2 per3 ... ");
			System.exit(0);
		}else{
			num_lines = Integer.parseInt(args[0]);
			for(int i=1;i<args.length;i++){
				pers.add(Double.parseDouble(args[i]));
			}
			result_file=new PrintWriter(new FileWriter(result_file_name));
			Process p = Runtime.getRuntime().exec("hostname");
			BufferedReader command_input =
			    new BufferedReader(new InputStreamReader(p.getInputStream()));
			result_file.println("* Hostname: "+command_input.readLine());
			command_input.close();
			p.destroy();
			result_file.println("* Total lines: "+num_lines);
			result_file.println("* Percentages: "+pers);
			result_file.println();
		}	
		
		
		long start_global=System.currentTimeMillis();

	
		//---prepare files---
		prepareFiles();//generate TBAT and update files for each perc
		
		//---prepare variables---
		int tbat_line_length= DataRetriever.getLineLength(tbat_file_name_original);
		HashMap<Double, ArrayList<Double>> all_times_merge_ob=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, ArrayList<Double>> all_times_merge_bi=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, Double> mean_times_merge_ob=new HashMap<Double, Double>();
		HashMap<Double, Double> mean_times_merge_bi=new HashMap<Double, Double>();
		HashMap<Double, Double> all_memories_ob=new HashMap<Double, Double>();//ob-tree memory used
		
		//---do the experiment---
		for(double per:pers){
			out.println("exp: update "+per+"%");
			result_file.println("* exp: update "+per+"%");
			String update_file_name=data_dir+"update_"+num_lines+"_"+per+".txt";
			
			ArrayList<Double> merge_bi_time_temp=new ArrayList<Double>();
			ArrayList<Double> merge_ob_time_temp=new ArrayList<Double>();
			
			// bulk loading of update list file into OB-tree
			//OBTree changed to <Integer,Integer> BTree
			OBTree obtree = new OBTree();
			obtree.loadUpdateFile(update_file_name);
			all_memories_ob.put(per,obtree.toKB());		    
		}
		out.println("Major expriment finished!");
		out.println();
		result_file.println("\n#Analysis:\n");

		//---------memory - OB-merge -------
		result_file.println("OB-Merge memory used:\n");
		result_file.format("%3s, %10s \n","perc","KB");
		for (double per : pers) {
			result_file.format("%-3.2f, %10.3f \n", per, all_memories_ob.get(per));
		}
		
		//end of file
		result_file.println();
		String program_end_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		result_file.println("Program Started at: "+program_start_date_time);
		result_file.println("Program Ended at:   "+program_end_date_time);
		long end_global=System.currentTimeMillis();
		double elapsedTime=(end_global-start_global)/1000.0;
		result_file.println("Elapsed Time:"+elapsedTime+"s\n");
		
		result_file.close();
		out.println("Elapsed Time:"+elapsedTime+"s");
	}
	
	/**
	 * prepare files for merge progressive select experiment
	 * files include:
	 * 	- tbat and bat files
	 *  - updat list file
	 *  - appendix files for each update file (AOC appendix files)
	 *  - selection oid file
	 *
	 */
	public static void prepareFiles() throws IOException {
		//-----prepare tbat and bat files-----
				DataCreator.prepareTBAT(num_lines, tbat_file_name_original);
				out.println("TBAT file "+tbat_file_name_original+" created");
				//-----prepare update and appendix files p%=1%-5%-----
				for(double per:pers){
					//create update files
					String update_file_name=data_dir+"update_"+num_lines+"_"+per+".txt";
					DataCreator.prepareUpdateList1(per, num_lines, update_file_name, 1);
					out.println("Update file: "+update_file_name+" created");
				}
				
				
				//-----prepare selection query files-----
//				DataCreator.prepareSelectionFile(select_file_name, sel_per, num_lines);
//				System.out.println("Selection files created");
	}


}
