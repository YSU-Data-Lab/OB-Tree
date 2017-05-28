package exp_select.obtree2;

import basic.btree.OBTree;
import basic.util.DataRetriever;
import basic.util.MathTool;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import static java.lang.System.out;

public class exp_obtree2_size {
	
	static String format_string1="%-15s\t %-15s\t %-15s\t\n";
	public static void main(String args[]) throws IOException{
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		long start=System.currentTimeMillis();
		PrintWriter result_file= new PrintWriter(new FileWriter("data/result-obtree-size.txt"));
		String data_folder=prepare_files2.data_folder;
		long num_lines_body = 0;
		int max_exp_times=0;
		ArrayList<Double> pers_update=new ArrayList();
		ArrayList<Double> pers_select=new ArrayList();

		//----------accept input arguments--------------
		if(args.length<=3){
			out.println("Please input num_lines "
					+ " max_exp_times -u update_per1 per2 per3 ... -s select_per1 per2 per3 ...");
			System.exit(0);
		}else{
			num_lines_body = Long.parseLong(args[0]);
			max_exp_times = Integer.parseInt(args[1]);
			
			if(args[2].equalsIgnoreCase("-u")){
				int i=3;
				while(!args[i].equalsIgnoreCase("-s")){
					pers_update.add(Double.parseDouble(args[i++]));
				}
				i++;
				while(i<args.length){
					pers_select.add(Double.parseDouble(args[i++]));
				}
			}else{
				int i=3;
				while(!args[i].equalsIgnoreCase("-u")){
					pers_select.add(Double.parseDouble(args[i++]));
				}
				i++;
				while(i<args.length){
					pers_update.add(Double.parseDouble(args[i++]));
				}
			}
			
			Process p = Runtime.getRuntime().exec("hostname");
			BufferedReader command_input =
			    new BufferedReader(new InputStreamReader(p.getInputStream()));
			result_file.println();
			result_file.println("% Hostname: "+command_input.readLine());
			command_input.close();
			p.destroy();
			result_file.println("% Total lines: "+num_lines_body);
			result_file.println("% BAT & TBAT Update Percentages: "+pers_update);
			result_file.println("% Selection Percentages: "+pers_select);
		}
		result_file.println("% size unit: KB");
		result_file.println();
		result_file.format(format_string1,	"exp", "per_update", "size");
		
		//-----------------------------------experiment body----------------------------------

		ArrayList<Double> obtree_size_list=new ArrayList<Double>();
        for (double per_update:pers_update){
			out.println("per_update:"+per_update);

			String tbat_file_name1=data_folder+"tbat_l"+num_lines_body+"_p"+per_update+"_1.txt";//unclean
			String tbat_file_name2=data_folder+"tbat_l"+num_lines_body+"_p"+per_update+"_2.txt";//merged
			String update_file_name=data_folder+"update_l"+num_lines_body+"_p"+per_update+".txt";

			int tbat_line_length= DataRetriever.getLineLength(tbat_file_name2);

			long btree_c_start=System.currentTimeMillis();
			OBTree obtree = new OBTree();
			obtree.loadUpdateFile(update_file_name);
			Double btree_c_time=(double)(System.currentTimeMillis()-btree_c_start)/1000.0d;
			//OB-tree size
			double obtree_size=obtree.toKB();
			obtree_size_list.add(obtree_size);
			result_file.format(format_string1,  "obtree_size", per_update, obtree_size);
			result_file.flush();//flush each time for long experiments

		}//end pers_update

		result_file.println();

		result_file.format("%-15s\t %-15s\t\n", "obtree_size_mean", MathTool.mean(obtree_size_list));
		result_file.println();

		//-------------summary and elapsed time calculation------
		long end=System.currentTimeMillis();
		double elapsedTime=(end-start)/1000.0;
		out.println("%Elapsed Time:"+elapsedTime+"s");
		result_file.println("% Elapsed Time:"+elapsedTime+"s");
		String program_end_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		result_file.println("% Program Started at: "+program_start_date_time);
		result_file.println("% Program Ended at:   "+program_end_date_time);
		result_file.println();
		result_file.close();
	}//---end of main---

}
