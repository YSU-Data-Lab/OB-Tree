package exp_merge_progressive;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import basic.btree.*;
import basic.storage_model.TBAT;
import basic.util.*;

public class exp_merge_progressive_btree {
	
	static int num_lines_1m=47660;
	static int num_lines;
//	static int num_lines=64*num_lines_1m;
	static ArrayList<Double> pers=new ArrayList<Double>();//update percentages
	static int appendix_num_split=10; //number of split files for appendixes
	static double sel_per=0.1;//selection percentage
	final static String dir_name= "results/exp_merge/";
	final static String bat_file_name=dir_name+"bat.txt";
	final static String tbat_file_name=dir_name+"tbat.txt";
	final static String tbat_temp_file_name=dir_name+"tbat_temp.txt";
	final static String select_file_name=dir_name+"select_"+sel_per+".txt";
	final static String result_file_name=dir_name+"results/result-merge-progressive-select.txt";
	
	public static void main(String[] args) throws IOException{
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		PrintWriter result_file= new PrintWriter(new FileWriter(result_file_name));
		
		if(args.length<1){
			System.out.println("Input: num_lines\n");
			System.exit(0);
		}else{
			num_lines = Integer.parseInt(args[0]);
			System.out.println("Number of lines for experiment:"+num_lines);
			result_file.println("Number of lines for experiment:"+num_lines+"\n");
		}
		BTree<Integer, Integer> appendixBTree = new BTree<Integer, Integer>();
		int OFF=1;
		String a;
		String timestamp; 
		int b;
		int valueOfA;
		
		
		long start=System.currentTimeMillis();
		for(int p=1;p<=5;p++){
			pers.add(p*0.01);
		}
		
		
		
		//---prepare files---
		prepareFiles();
		
		//---do the experiment---
		int tbat_line_length= DataRetriever.getLineLength(tbat_file_name);
		int bat_line_length = DataRetriever.getLineLength(bat_file_name);
		List<Integer> select_list= DataCreator.loadSelectionFile(select_file_name);
		//for progressive approach
		HashMap<Double, ArrayList<Double>> all_times_select=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, ArrayList<Double>> all_times_merge=new HashMap<Double, ArrayList<Double>>();
		HashMap<Double, Double> all_memories=new HashMap<Double, Double>();
		//for eacher approach
//		HashMap<Double, Double> all_times_select2=new HashMap<Double, Double>();
		HashMap<Double, Double> all_memories_eager=new HashMap<Double, Double>();

		for(double per:pers){
			System.out.println("exp: update "+per+"%");
			result_file.println("* exp: update "+per+"%");
			//-----progressive approach-----
			BasicTools.copyFile(tbat_file_name, tbat_temp_file_name);
			System.out.println("copied temp file");
			ArrayList<Double> times_select=new ArrayList<Double>();
			ArrayList<Double> times_merge=new ArrayList<Double>();
			ArrayList<Double> memories=new ArrayList<Double>();
			String appendix_file_prefix=dir_name+"appendix_"+per;
			ArrayList<String> appendix_file_names=new ArrayList<String>();
			for(int i=1;i<=appendix_num_split;i++){
				appendix_file_names.add(appendix_file_prefix+"_"+i+".txt");
				Scanner reads = new Scanner(new File(dir_name + "update_" + per + ".txt"));
				OFF = 1;
				while (reads.hasNext()) {
					a = reads.next(); // read OID
					b = reads.nextInt(); // read VALUE
					a = a.substring(0, a.length() - 1); // removing the comma that was auto-generated
					valueOfA = Integer.parseInt(a); // placing that number into a variable
					if (appendixBTree.get(valueOfA) != null) {
						appendixBTree.findReplace(valueOfA, OFF);
					} else {
						appendixBTree.put(valueOfA, OFF);
					}// end of if-else
					OFF++;
				}
				//reads.close();    
				
				
			}
			for(int index=0;index<=appendix_num_split;index++){
				System.out.println("progressive sort merge tbat index:"+index);
				if(index!=0){
				    Runtime runtime = Runtime.getRuntime();//Get the Java runtime
				    long start_merge=System.currentTimeMillis();
					DataUpdator.sortMergeFileToTBAT2(tbat_temp_file_name, appendix_file_prefix+"_"+index+".txt", 1);
					long end_merge=System.currentTimeMillis();
					double elapsed_time_merge=(end_merge-start_merge)/1000.0;
					times_merge.add(elapsed_time_merge);
					runtime.gc();//Run garbage collector
					long memory = runtime.totalMemory() - runtime.freeMemory();//used memory
				    memories.add(MathTool.bytesToKB(memory)*1.0);
				    appendix_file_names.remove(0);
				}
				long target_value;
				System.out.println("exp select TBAT uncleaned");
				long start2=System.currentTimeMillis();
				//System.out.println(appendixBTree); 
				try{
					RandomAccessFile updates = new RandomAccessFile(new File(dir_name + "update_" + per + ".txt"), "r");
				for(int target_oid:select_list){
					if(appendixBTree.get(target_oid) == null){
					target_value = TBAT.selectTBAT_Uncleaned2(tbat_file_name, num_lines,
							tbat_line_length, target_oid);
					}else{
						//target_value = DataRetriever.selectTBAT_Uncleaned_Split2(appendix_file_names,
						//0, bat_line_length, target_oid);
						//System.out.println(appendixBTree.searchKey(target_oid));
						target_value = TBAT.searchAppendixByOffSet(updates, 0,
								bat_line_length, appendixBTree.get(target_oid), 1);
					}
					//System.out.printf("Target OID %d has value %d     ", target_oid, target_value); 
				}
				}catch(Exception ex){
					System.out.println("File Problems Again");
				}
				long end2=System.currentTimeMillis();
				double elapsed_time2=(end2-start2)/1000.0;
				times_select.add(elapsed_time2);
			}
			all_times_merge.put(per, times_merge);
			all_times_select.put(per, times_select);
			all_memories.put(per, MathTool.mean(memories));
			
			
			//-----eager approach-----
			System.out.println("eager sort merge tbat");
			BasicTools.copyFile(tbat_file_name, tbat_temp_file_name);
			Runtime runtime2 = Runtime.getRuntime();//Get the Java runtime
			runtime2.gc();//Run garbage collector
			DataUpdator.sortMergeFileToTBAT2(tbat_temp_file_name, dir_name+"update_"+per+".txt", 0);
			long memory2 = runtime2.totalMemory() - runtime2.freeMemory();//used memory
		    all_memories_eager.put(per, MathTool.bytesToKB(memory2)*1.0);
		}
		System.out.println("Major expriment finished!");
		System.out.println();
		result_file.println("\n#Progressive:\n");
		
		//-------merge time----------
		result_file.println("Merge Time:\n");
		//print table head
		result_file.print("update_per\\merge_per");		
		for(int index=1;index<=appendix_num_split;index++){
			result_file.print("|"+(int)(index*10)+"%");
		}
		result_file.println();
		result_file.print("---");
		for(int index=0;index<=appendix_num_split;index++){
			result_file.print("|---");
		}
		result_file.println();
		//print table body
		for(double per:pers){
			result_file.print(per+"");
			ArrayList<Double> times_merge=all_times_merge.get(per);
			for(int i=0;i<times_merge.size();i++){
				result_file.print("|"+times_merge.get(i));
			}
			result_file.println();
		}
		
		result_file.println();
		
		
		//--------selection time--------
		result_file.println("Select Time After Update:\n");
		result_file.print("update_per\\merge_per");		
		for(int index=0;index<=appendix_num_split;index++){
			result_file.print("|"+(int)(index*10)+"%");
		}
		result_file.println();
		result_file.print("---");
		for(int index=0;index<=appendix_num_split;index++){
			result_file.print("|---");
		}
		result_file.println();
		
		//select time: print table body
		for(double per:pers){
			result_file.print(per+"");
			ArrayList<Double> times_select=all_times_select.get(per);
			for(int i=0;i<times_select.size();i++){
				result_file.print("|"+times_select.get(i));
			}
			result_file.println();
		}
		
		result_file.println();
		
		result_file.println("Average Memory Usage (KB):\n");
		double mean_progressive=0;
		//memory: table head
		result_file.println("update_per|memory");
		result_file.println("---|---");
		//memory: table body
		for(double per:pers){
			result_file.println(per+"|"+all_memories.get(per));
			mean_progressive+=all_memories.get(per);
		}
		mean_progressive=mean_progressive/all_memories.size();
		result_file.println("Average Memory Usage (KB): "+mean_progressive);		
		
		result_file.println();
		
		result_file.println("\n#Eager:\n");
		result_file.println("Average Memory Usage (KB):\n");
		double mean_eager=0;
		//memory: table head
		result_file.println("update_per|memory");
		result_file.println("---|---");
		//memory: table body
		for(double per:pers){
			result_file.println(per+"|"+all_memories_eager.get(per));
			mean_eager+=all_memories_eager.get(per);
		}
		mean_eager=mean_eager/all_memories.size();
		result_file.println("Average Memory Usage (KB): "+mean_eager+"\n");
		
		
		String program_end_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		result_file.println("Program Started at: "+program_start_date_time+"\n");
		result_file.println("Program Ended at:   "+program_end_date_time+"\n");
		long end=System.currentTimeMillis();
		double elapsedTime=(end-start)/1000.0;
		result_file.println("Elapsed Time:"+elapsedTime+"s\n");
		
		result_file.close();
		System.out.println("Elapsed Time:"+elapsedTime+"s");
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
				DataCreator.prepareData(num_lines, bat_file_name, tbat_file_name);
				System.out.println("TBAT and BAT created");
				
				//-----prepare update and appendix files p%=1%-5%-----
				for(double per:pers){
					//create update files
					String update_file_name=dir_name+"update_"+per+".txt";
					DataCreator.prepareUpdateList1(per, num_lines, update_file_name);
					//create appendix files
					String appendix_file_prefix=dir_name+"appendix_"+per;
					int appendix_block_size=(int)(per*num_lines)/appendix_num_split;
					DataCreator.creaetTBATAppendix(
							update_file_name, appendix_file_prefix,appendix_block_size);
				}
				System.out.println("Update and appendix files created");
				
				//-----prepare selection query files-----
				DataCreator.prepareSelectionFile(select_file_name, sel_per, num_lines);
				System.out.println("Selection files created");
	}


}
