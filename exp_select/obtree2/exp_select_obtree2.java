package exp_select.obtree2;

import basic.btree.OBTree;
import basic.storage_model.BAT;
import basic.storage_model.TBAT;
import basic.util.DataCreator;
import basic.util.DataRetriever;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static java.lang.System.lineSeparator;
import static java.lang.System.out;

public class exp_select_obtree2 {
	
	static String format_string1="%-15s\t %-15s\t %-15s\t %-15s\t %-15s\n";
	
	public static void main(String args[]) throws IOException{
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		long start=System.currentTimeMillis();
		PrintWriter result_file= new PrintWriter(new FileWriter("data/result-select.txt"));
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
			result_file.println("% Hostname: "+command_input.readLine());
			command_input.close();
			p.destroy();
			result_file.println("% Total lines: "+num_lines_body);
			result_file.println("% BAT & TBAT Update Percentages: "+pers_update);
			result_file.println("% Selection Percentages: "+pers_select);
			result_file.println();
		}
		
		result_file.format(format_string1,	"exp", "per_update", "per_select", "iteration", "time");
		
		//-----------------------------------experiment body----------------------------------

        /**
         * for each per_update (update percentage) and per_select (select percentage)
         * experiment work flow
         *
         * 2. selection tests
         * 2.1 obtree creation
         *      obtree creation: loadUpdateFile -> appendixTree(i.e. obtree)
         * 2.2 obtree selection
         *      use select_list
         * 2.3 tbat (uncleaned, without index) selection
         *      selectTBAT_uncleaned
         * 2.4 bat (updated) selection
         *      selectBAT
         */

        for (double per_update:pers_update){
			out.println("per_update:"+per_update);

			String tbat_file_name1=data_folder+"tbat_l"+num_lines_body+"_p"+per_update+"_1.txt";//unclean
			String tbat_file_name2=data_folder+"tbat_l"+num_lines_body+"_p"+per_update+"_2.txt";//merged
			String update_file_name=data_folder+"update_l"+num_lines_body+"_p"+per_update+".txt";

			
			int tbat_line_length= DataRetriever.getLineLength(tbat_file_name2);
			
			for (double per_select:pers_select){
				out.println("per_update:"+per_update+" | per_select:"+per_select);

				String select_file_name=data_folder+"select_l"+num_lines_body+"_p"+per_select+".txt";
				ArrayList<Double> bat_select_time_temp=new ArrayList<Double>();
				ArrayList<Double> tbat_select_time_temp=new ArrayList<Double>();
				
				for(int i=1;i<=max_exp_times;i++){
					out.println("\tloop:"+i);
					//---search tbat cleaned
					long tbat_c_start=System.currentTimeMillis();
                    TBAT.selectTBAT_body(tbat_file_name2,select_file_name,num_lines_body,tbat_line_length);
                    Double tbat_c_time=(double)(System.currentTimeMillis()-tbat_c_start)/1000.0d;
					result_file.format(format_string1,	"tbat_c_s", per_update, per_select, i, tbat_c_time);//record time

					//---search tbat uncleaned (without OB-tree index)---
                    // too long we include the time of uncleaned search in 10000L and 10MB only
//					long tbat_start=System.currentTimeMillis();
//					TBAT.selectTBAT_Uncleaned(tbat_file_name1,select_file_name,num_lines_body,tbat_line_length);
//					Double tbat_time=(double)(System.currentTimeMillis()-tbat_start)/1000.0d;
//					result_file.format(format_string1,	"tbat_un_s", per_update, per_select, i, tbat_time);

					//---load update appendix into btree---
					//btree create start
					long btree_c_start=System.currentTimeMillis();
					OBTree obtree = new OBTree();
					obtree.loadUpdateFile(update_file_name);
					Double btree_c_time=(double)(System.currentTimeMillis()-btree_c_start)/1000.0d;
					//OB-tree creation time
					result_file.format(format_string1,	"obtree_c", per_update, per_select, i, btree_c_time);
					result_file.format(format_string1,  "obtree_size", per_update, per_select, i, obtree.toKB());

					//---search tbat (uncleaned) with OB-tree index---
					long btree_s_start=System.currentTimeMillis();
					TBAT.searchWithOBTree(obtree,tbat_file_name1,select_file_name,num_lines_body,tbat_line_length);
					Double btree_s_time=(double)(System.currentTimeMillis()-btree_s_start)/1000.0d;
					//record TBAT selection time with OB-tree
					result_file.format(format_string1,	"tbat_ob_s", per_update, per_select, i, btree_s_time);

				}//end max_exp_time
			}//end pers_select
			result_file.println("\n");
			result_file.flush();//flush each time for long experiments

		}//end pers_update
		
		//-------------summary and elapsed time calculation------
		long end=System.currentTimeMillis();
		double elapsedTime=(end-start)/1000.0;
		out.println("%Elapsed Time:"+elapsedTime+"s");
		result_file.println("% Elapsed Time:"+elapsedTime+"s");
		String program_end_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		result_file.println("% Program Started at: "+program_start_date_time);
		result_file.println("% Program Ended at:   "+program_end_date_time);
		result_file.close();
	}//---end of main---

}
