package exp_merge_ob;

import static java.lang.System.out;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import basic.btree.*;
import basic.util.*;

public class exp_merge_ob {
	
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
//		final String result_file_name=result_dir+"result-merge-ob-"+
//	    		(new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime()));
		final String result_file_name=result_dir+"result-merge-ob.txt";
		PrintWriter result_file = null;
		if(args.length < 3){
			out.println("Please input num_lines "
					+ " max_exp_times per1 per2 per3 ... ");
			System.exit(0);
		}else{
			num_lines = Integer.parseInt(args[0]);
			max_exp_times = Integer.parseInt(args[1]);
			for(int i=2;i<args.length;i++){
				pers.add(Double.parseDouble(args[i]));
			}
			result_file=new PrintWriter(new FileWriter(result_file_name));
			Process p = Runtime.getRuntime().exec("hostname");
			BufferedReader command_input =
			    new BufferedReader(new InputStreamReader(p.getInputStream()));
			result_file.println("* Hostname: "+command_input.readLine());
			command_input.close();
			p.destroy();
			result_file.println("* OB-Tree degree: "+BTree.M);
			result_file.println("* Total lines: "+num_lines);
			result_file.println("* Percentages: "+pers);
			result_file.println();
		}	
		
		
		long start_global=System.currentTimeMillis();

	
		//---prepare files---
		prepareFiles();//generate TBAT and update files for each perc
		int tbat_line_length= DataRetriever.getLineLength(tbat_file_name_original);
		
		//---prepare variables---
		HashMap<Double, ArrayList<Double>> all_times_merge_ob=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, ArrayList<Double>> all_times_merge_bi=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, Double> mean_times_merge_ob=new HashMap<Double, Double>();
		HashMap<Double, Double> mean_times_merge_bi=new HashMap<Double, Double>();
		HashMap<Double, Double> all_memories_ob=new HashMap<Double, Double>();//ob-tree memory used
		HashMap<Double, Long> disk_access_ob=new HashMap<Double, Long>();//ob-tree disk access
		HashMap<Double, Long> disk_access_bi=new HashMap<Double, Long>();//binary search merge disk access

		//---do the experiment---
		for(double per:pers){
			

			out.println("exp: update "+per+"%");
			result_file.println("* exp: update "+per+"%");
			String update_file_name=data_dir+"update_"+num_lines+"_"+per+".txt";
			
			ArrayList<Double> merge_bi_time_temp=new ArrayList<Double>();
			ArrayList<Double> merge_ob_time_temp=new ArrayList<Double>();
			
			// bulk loading of update list file into OB-tree
			// OBTree<Integer, Integer> obtree = new OBTree<Integer, Integer>();
			// OBTree changed to <Integer,Integer> BTree
			OBTree obtree = new OBTree();
			obtree.loadUpdateFile(update_file_name);
			all_memories_ob.put(per,obtree.toKB());
			long disk_access_ob_temp=0;
			long disk_access_bi_temp=0;
			for(int i=0;i<max_exp_times;i++){
				out.println("loop:"+(i+1));
				result_file.println("loop:"+(i+1));
				//========binary search merge=========
				BasicTools.copyFile(tbat_file_name_original, tbat_file_name_copy2);
			    long startTime_bi = System.currentTimeMillis();//check the starting time.
			    long temp1= DataUpdator.sortMergeFileToTBAT4(tbat_file_name_copy2, update_file_name, 1);
			    disk_access_bi_temp+=temp1;
		        long endTime_bi = System.currentTimeMillis(); // ending time
		        double elapsedTime_bi = (endTime_bi - startTime_bi) / 1000.0; //conversion to seconds
		        merge_bi_time_temp.add(elapsedTime_bi);
//		        out.println("bi disk count:"+temp1);
				
				//========ob-tree merge=========
		        //OB-Tree merge entry_list into body (here we use the copy1 file)
				BasicTools.copyFile(tbat_file_name_original, tbat_file_name_copy1);
				long startTime_ob = System.currentTimeMillis();//check the starting time.
				RandomAccessFile reader_ob=new RandomAccessFile(new File(update_file_name),"r");
		        RandomAccessFile writer_ob=new RandomAccessFile(new File(tbat_file_name_copy1),"rw");
		        long temp2=DataUpdator.mergeAppendixToTBAT_OBTree(obtree, reader_ob, writer_ob, tbat_line_length);
		        disk_access_ob_temp+=temp2;
//		        out.println("ob disk count:"+temp2);

		        reader_ob.close();
		        writer_ob.close();
//		        result_file.println("OB-Tree merge finished");
		        long endTime_ob = System.currentTimeMillis(); // ending time
		        double elapsedTime_ob = (endTime_ob - startTime_ob) / 1000.0; //conversion to seconds
		        merge_ob_time_temp.add(elapsedTime_ob);
//		        result_file.println("Elapsed Time - OB-Merge:" + elapsedTime_ob + "s\n\n");//total running time
			}//end of max_exp_times loop
			
		    all_times_merge_bi.put(per, merge_bi_time_temp);
		    all_times_merge_ob.put(per, merge_ob_time_temp);
		    mean_times_merge_bi.put(per, MathTool.mean(merge_bi_time_temp));
		    mean_times_merge_ob.put(per, MathTool.mean(merge_ob_time_temp));
		    disk_access_ob.put(per, disk_access_ob_temp/max_exp_times);
		    disk_access_bi.put(per, disk_access_bi_temp/max_exp_times);
		}
		out.println("Major expriment finished!");
		out.println();
		result_file.println("\n#Analysis:\n");

		//-------merge time----------
//		result_file.println("Merge Time:");
//		result_file.format("%3s, %3s, %10s, %10s\n","perc","it","trad","ob");
//		for(double per:pers){
//			ArrayList<Double> merge_bi_time_temp=all_times_merge_bi.searchKey(per);
//			ArrayList<Double> merge_ob_time_temp=all_times_merge_ob.searchKey(per);
//			for(int i=0;i<max_exp_times;i++){
//				result_file.format("%-3.2f, %3d, %10.3f, %10.3f\n",
//						per, i+1, merge_bi_time_temp.searchKey(i), merge_ob_time_temp.searchKey(i));
//			}
//		}
//		result_file.println();
		
		result_file.println("Merge Time (CSV for R):");
		result_file.format("%3s, %3s, %10s, %10s\n","perc","it","time", "groups");
		for(double per:pers){
			ArrayList<Double> merge_bi_time_temp=all_times_merge_bi.get(per);
			for(int i=0;i<max_exp_times;i++){
				result_file.format("%-3.2f, %3d, %10.3f, %10s\n",
						per, i+1, merge_bi_time_temp.get(i), "bi");
			}
		}
		for(double per:pers){
			ArrayList<Double> merge_ob_time_temp=all_times_merge_ob.get(per);
			for(int i=0;i<max_exp_times;i++){
				result_file.format("%-3.2f, %3d, %10.3f, %10s\n",
						per, i+1, merge_ob_time_temp.get(i), "ob");
			}
		}
		result_file.println();
		
		result_file.println("Mean Values:");
		result_file.format("%3s, %10s, %10s, %10s\n","perc","trad","ob","overhead");
		for(double per:pers){
			result_file.format("%-3.2f, %10.3f, %10.3f, %10.3f\n",
					per, mean_times_merge_bi.get(per),mean_times_merge_ob.get(per),
					(mean_times_merge_bi.get(per)-mean_times_merge_ob.get(per))/mean_times_merge_ob.get(per)*100.0);
		}
		
		result_file.print("\n\n");
		
		//---------disk access -------
		result_file.println("disk access:");
		result_file.format("%3s, %10s, %10s, %10s\n","perc","trad","ob","overhead");
		for (double per : pers) {
			result_file.format("%-3.2f, %10d, %10d, %10.3f\n", per, disk_access_bi.get(per), disk_access_ob.get(per), 
					(disk_access_bi.get(per)-disk_access_ob.get(per))*1.0/disk_access_ob.get(per)*100.0);
		}
		result_file.println();

		//---------memory - OB-merge -------
		result_file.println("OB-Merge memory used:");
		result_file.format("%3s, %10s\n","perc","KB");
		for (double per : pers) {
			result_file.format("%-3.2f, %10.3f\n", per, all_memories_ob.get(per));
		}
		result_file.println();

		//end of file
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
