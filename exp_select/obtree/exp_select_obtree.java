package exp_select.obtree;

import static java.lang.System.out;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import basic.btree.*;
import basic.storage_model.BAT;
import basic.storage_model.TBAT;
import basic.util.DataCreator;
import basic.util.DataRetriever;

public class exp_select_obtree {
	
	static String format_string1="%-15s\t %-15s\t %-15s\t %-15s\t %-15s\n";
	
	public static void main(String args[]) throws IOException{
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		long start=System.currentTimeMillis();
		PrintWriter result_file= new PrintWriter(new FileWriter("data/result-select.txt"));
		
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
		
		//-----------------------------------experiment body-----------------------------------------

        /**
         * for each per_update (update percentage) and per_select (select percentage)
         * experiment work flow
         * 1. make selection list randomly -> select_list
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
			String tbat_file_name="data/tbat_"+per_update+".txt";
			String bat_file_name="data/bat_"+per_update+".txt";
			String update_file_name="data/update_"+per_update+".txt";
			
			int tbat_line_length= DataRetriever.getLineLength(tbat_file_name);
			int bat_line_length=DataRetriever.getLineLength(bat_file_name);			
			
			for (double per_select:pers_select){
				out.println("per_update:"+per_update+" | per_select:"+per_select);
				
				ArrayList<Double> bat_select_time_temp=new ArrayList<Double>();
				ArrayList<Double> tbat_select_time_temp=new ArrayList<Double>();
				
				for(int i=1;i<=max_exp_times;i++){
					out.println("\tloop:"+(i+1));
					//make selection list
					List<Long> select_list= DataCreator.makeUpdateList(per_select, num_lines_body);
					long value;
					

					//---load update appendix into btree---
					//btree create start
					long btree_c_start=System.currentTimeMillis();
					OBTree obtree = new OBTree();
					obtree.loadUpdateFile(update_file_name);
					Double btree_c_time=(double)(System.currentTimeMillis()-btree_c_start)/1000.0d;
					//OB-tree creation time
					result_file.format(format_string1,	"btree_c", per_update, per_select, i, btree_c_time);

					//---btree select start---
					RandomAccessFile tbat_file=new RandomAccessFile(new File(tbat_file_name), "r");//open
					long offset;
					long btree_s_start=System.currentTimeMillis();
					for(long target_oid:select_list){
						offset=obtree.searchKey(target_oid);
						if(offset!=DataRetriever.NOT_FOUND){
							value= TBAT.searchAppendixByOffSet(tbat_file, num_lines_body, tbat_line_length, offset, 2);//in a tbat, value is at 2 (3rd position in one line)
						}else{
							value= TBAT.selectTBAT_body(tbat_file_name, num_lines_body, tbat_line_length, target_oid);
						}
					}
					tbat_file.close();
					Double btree_s_time=(double)(System.currentTimeMillis()-btree_s_start)/1000.0d;
					//record TBAT selection time with OB-tree
					result_file.format(format_string1,	"btree_s", per_update, per_select, i, btree_s_time);


					//---select tbat uncleaned (without OB-tree index)--
					long tbat_start=System.currentTimeMillis();
					for(long target_oid:select_list){
						value= TBAT.selectTBAT_Uncleaned(tbat_file_name, num_lines_body, tbat_line_length, target_oid);
					}
					Double tbat_time=(double)(System.currentTimeMillis()-tbat_start)/1000.0d;
					//record TBAT selection time without OB-tree
					result_file.format(format_string1,	"tbat_s", per_update, per_select, i, tbat_time);

					//---select bat---
					long bat_start=System.currentTimeMillis();
					for(long target_oid:select_list){
						value= BAT.selectBAT(bat_file_name, num_lines_body, bat_line_length, target_oid);
					}
					Double bat_time=(double)(System.currentTimeMillis()-bat_start)/1000.0d;
					//record BAT selection time
					result_file.format(format_string1,	"bat_s", per_update, per_select, i, bat_time);
				}//end max_exp_time
			}//end pers_select
			result_file.println("\n");
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
