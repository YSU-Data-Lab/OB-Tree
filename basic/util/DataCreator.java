package basic.util;

import basic.storage_model.BAT;
import basic.storage_model.TBAT;

import java.io.*;
import java.util.*;



public class DataCreator {

	public static void prepareData(long num_lines, String bat_file_name,
								   String tbat_file_name) throws IOException{

		PrintWriter bat_file= new PrintWriter(new FileWriter(bat_file_name));
		PrintWriter tbat_file = new PrintWriter(new FileWriter(tbat_file_name));

		String bat_str="";
		String tbat_str="";
		String timestampstr="";

		timestampstr=String.format("%d", System.currentTimeMillis());
		timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());

		for(long i=0;i<num_lines;i++){
			bat_file.format(BAT.bat_format,i+1,0);
			tbat_file.format(TBAT.tbat_format,timestampstr,i+1,0);
		}
		bat_file.close();
		tbat_file.close();
	}

	public static void prepareBAT(int num_lines, String bat_file_name) throws IOException{

		PrintWriter bat_file= new PrintWriter(new FileWriter(bat_file_name));

		String bat_str="";
		String timestampstr="";

		timestampstr=String.format("%d", System.currentTimeMillis());
		timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());

		for(int i=0;i<num_lines;i++){
			bat_file.format(BAT.bat_format,i+1,0);
		}
		bat_file.close();
	}

	public static void prepareTBAT(long num_lines, String tbat_file_name) throws IOException{
		PrintWriter tbat_file = new PrintWriter(new FileWriter(tbat_file_name));
		String tbat_str="";
		String timestampstr="";
		timestampstr=String.format("%d", System.currentTimeMillis());
		timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
		for(long i=0;i<num_lines;i++){
			tbat_file.format(TBAT.tbat_format,timestampstr,i+1,0);
		}
		tbat_file.close();
	}

	/**
	 * version 1 allow duplicated update values
	 * @param per
	 * @param num_lines
	 * @param update_file_name
	 * @throws IOException
	 */
	public static void prepareUpdateList1(double per, int num_lines,
	String update_file_name) throws IOException {
		prepareUpdateList1(per, num_lines,update_file_name, 0);
	}

	/**
	 * version 1 allow duplicated update values
	 * @param per
	 * @param num_lines
	 * @param update_file_name
	 * @throws IOException
	 */
	public static void prepareUpdateList1(double per, int num_lines,
	String update_file_name, int oid_position) throws IOException {
		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		int update_num_lines=(int)(per*num_lines);
		int total_updated=0;
		int current_line=1;
		Random rand=new Random();
		String format=oid_position==1?TBAT.tbat_format:BAT.bat_format;
		String timestampstr=null;

		if(oid_position==0){
			while(total_updated < update_num_lines){
				current_line=rand.nextInt(num_lines)+1;
				update_file.format(BAT.bat_format, current_line, -1);
				total_updated++;
			}
		}else{
			while(total_updated < update_num_lines){
				timestampstr=String.format("%d", System.currentTimeMillis());
				timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
				current_line=rand.nextInt(num_lines)+1;
				update_file.format(TBAT.tbat_format, timestampstr, current_line, -1);
				total_updated++;
			}
		}
		update_file.close();
	}

	/**
	 * the oids are randomly generated and shuffled
	 */
	public static void prepareUpdateList2(double per, int num_lines,
			String update_file_name) throws IOException {

		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		List<Integer> update_list=makeUpdateList(per, num_lines);
		for(Integer current_line : update_list){
			update_file.format(BAT.bat_format, (int)current_line, -1);
		}
		update_file.close();
	}

	/**
	 * update value is same as line number
	 */
	public static void prepareUpdateList3(double per, long num_lines,
			String update_file_name) throws IOException {

		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		List<Long> update_list=makeUpdateList(per, num_lines);
		for(Long current_line : update_list){
			update_file.format(BAT.bat_format, (long)current_line, (long)current_line);
		}
		update_file.close();
	}

	/**
	 * version 4 creates TBAT.tbat_formate update list
	 * allow duplicated update values
	 * the update value increases from 1
	 */
	public static void prepareUpdateList4(double per, int num_lines,
	String update_file_name) throws IOException {
		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		int update_num_lines=(int)(per*num_lines);
		int total_updated=0;
		int current_line=1;
		int update_value=1;
		String timestampstr;
		Random rand=new Random();
		while(total_updated < update_num_lines){
			timestampstr=String.format("%d", System.currentTimeMillis());
			timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
			current_line=rand.nextInt(num_lines)+1;
			update_file.format(TBAT.tbat_format, timestampstr, current_line, update_value++);
			total_updated++;
		}
		update_file.close();
	}

	/**
	 * version 4.1
	 * num_lines use long
	 */
	public static void prepareUpdateList41(double per, long num_lines,
										  String update_file_name) throws IOException {
		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		long update_num_lines=(long)(per*num_lines);
		long total_updated=0;
		long current_line=1;
		long update_value=1;
//		String timestampstr;
		Random rand=new Random();
		while(total_updated < update_num_lines){
//			timestampstr=String.format("%d", System.currentTimeMillis());
//			timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
			current_line=(long)(rand.nextDouble()*num_lines)+1L;
			update_file.format(BAT.bat_format, current_line, update_value++);
			total_updated++;
		}
		update_file.close();
	}

	/**
	 * doesn't need to shuffle a list randomly
	 * we just need to make a file
	 * for oid =1 to num_lines
	 *  each time there is a random double probability p generated
	 *  if p<=per
	 *  then write this oid with the update value++
	 */
	public static void prepareUpdateList5(double per, long num_lines,
										   String update_file_name) throws IOException {
		PrintWriter update_file=new PrintWriter(new FileWriter(update_file_name));
		long update_num_lines=(long)(per*num_lines);
		long total_updated=0;
		long current_line=1;
		long update_value=1;
//		String timestampstr;
		Random rand=new Random();
		double p;//random probability
		while(total_updated < update_num_lines){
			p=rand.nextDouble();
			if(p<=per){
				update_file.format(BAT.bat_format, current_line, update_value++);
				total_updated++;
			}
			if(current_line++>=num_lines)
				current_line=1;
		}
		update_file.close();
	}

    static List<Integer> makeList(int begin, int end){
        List<Integer> list=new ArrayList(end-begin+1);
        for (int i=begin;i<=end;i++){
            list.add(i);
        }
        return list;
    }

    static List<Long> makeListLong(long begin, long end){
		List<Long> list=new ArrayList();
		for (long i=begin;i<=end;i++){
			list.add(i);
		}
		return list;
	}

	/**
	 * the update list doesn't need to be sorted 2014-10-02
	 */
	public static List<Integer> makeUpdateList(double per, int num_lines){
		List list=makeList(1,num_lines);
		Collections.shuffle(list);
		int update_num_lines=(int)(per*num_lines);
		List<Integer> update_list_sorted=list.subList(0, update_num_lines);
//		Collections.sort(update_list_sorted);
		return update_list_sorted;
	}

	/**
	 * the update list doesn't need to be sorted 2014-10-02
	 */
	public static List<Long> makeUpdateList(double per, long num_lines){
		List list=makeListLong(1,num_lines);
		Collections.shuffle(list);
		long update_num_lines=(long)(per*num_lines);
		List<Long> update_list_sorted=list.subList(0, (int)update_num_lines);
//		Collections.sort(update_list_sorted);
		return update_list_sorted;
	}

	/**
	 * works only for a small selection file
	 */
	public static void prepareSelectionFile(String output_file_name, double sel_per, long num_lines) throws IOException{
		PrintWriter output_file=new PrintWriter(new BufferedWriter(new FileWriter(output_file_name)));
		List<Long> list=DataCreator.makeUpdateList(sel_per, num_lines);
		for(long oid:list){
			output_file.println(oid+"");
		}
		output_file.close();
	}

	/**
	 * produce selection of large size
	 * same as prepareUpdateList5 use a probability menor
	 */
	public static void prepareSelectionFile5(String output_file_name, double sel_per, long num_lines) throws IOException{
		PrintWriter output_file=new PrintWriter(new BufferedWriter(new FileWriter(output_file_name)));
//		List<Long> list=DataCreator.makeUpdateList(sel_per, num_lines);
//		for(long oid:list){
//			output_file.println(oid+"");
//		}
		long sel_num_lines=(long)(sel_per*num_lines);//total selection number
		long total_selected=0;
		long current_line=1;
		Random rand=new Random();
		double p;//random probability
		while(total_selected < sel_num_lines){
			p=rand.nextDouble();
			if(p<=sel_per){
				output_file.format("%d\n",current_line);
				total_selected++;
			}
			if(current_line++>=num_lines)
				current_line=1;
		}
		output_file.close();
	}
	
	public static List<Integer> loadSelectionFile(String input_file_name) throws IOException{
		BufferedReader file_in=new BufferedReader(new FileReader(input_file_name));
		List<Integer> list=new ArrayList<Integer>();
		String line="";
		while((line=file_in.readLine())!=null){
			list.add(Integer.parseInt(line.trim()));
		}
		file_in.close();
		return list;
	}
	

	/**
	 * 
	 * @param update_file_name
	 * @param appendix_file_prefix
	 * @param appendix_block_size
	 * @return number of appendix files returned
	 * @throws IOException
	 */
	public static int creaetTBATAppendix(String update_file_name, String appendix_file_prefix, int appendix_block_size) throws IOException{
		if(appendix_block_size==0){
			throw new IOException("appendix_block_size is zero!");
		}
		BufferedReader update_file_in =new BufferedReader(new FileReader(update_file_name));	
		ArrayList<String> update_lines=new ArrayList<String>();//buffer of update file
		
		//read update file to buffer
		String line="";
		ArrayList<String> split_buffer=new ArrayList<String>();//buffer of to split the update file buffer
		int appendix_file_index=1;
		int split_buffer_count=0;
		String timestampstr="";
		long current_time_mills=System.currentTimeMillis();
		
		while((line = update_file_in.readLine()) != null){
			split_buffer.add(line);
			if(++split_buffer_count % appendix_block_size == 0){
				timestampstr=String.format("%d", current_time_mills++);
				timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
				saveStringBufferToFile(appendix_file_prefix+"_"+(appendix_file_index++)+".txt",split_buffer,timestampstr);
				split_buffer.clear();
			}
		}
		
		//dump the rest of update file
		if(!split_buffer.isEmpty()){
			timestampstr=String.format("%d", current_time_mills++);
			timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
			saveStringBufferToFile(appendix_file_prefix+"_"+(appendix_file_index++)+".txt",split_buffer,timestampstr);
		}
		update_file_in.close();
		return appendix_file_index--;
	}
	
	public static void saveStringBufferToFile(String output_file_name, ArrayList<String> buffer, String timestampstr) throws IOException{
		PrintWriter output_file=new PrintWriter(new BufferedWriter(new FileWriter(output_file_name)));
		for(String line:buffer){
			output_file.println(timestampstr+","+line);
		}
		output_file.close();
	}

	
//	/**
//	 * devide the appendix files into a given number of split files
//	 */
//	public static void creaetTBATAppendix2(String update_file_name, String appendix_file_prefix, 
//			int appendix_num) throws IOException{
//		if(appendix_num==0){
//			throw new IOException("appendix_num is zero!");
//		}
//		
//		BufferedReader update_file_in =new BufferedReader(new FileReader(update_file_name));	
//		ArrayList<String> update_lines=new ArrayList<String>();//buffer of update file
//		
//		
//		//read update file to buffer
//		String line="";
//		ArrayList<String> split_buffer=new ArrayList<String>();//buffer of to split the update file buffer
//		int appendix_file_index=1;
//		int split_buffer_count=0;
//		String timestampstr="";
//		long current_time_mills=System.currentTimeMillis();
//		
//		while((line = update_file_in.readLine()) != null){
//			split_buffer.add(line);
//			if(++split_buffer_count % appendix_block_size == 0){
//				timestampstr=String.format("%d", current_time_mills++);
//				timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
//				saveStringBufferToFile(appendix_file_prefix+"_"+(appendix_file_index++)+".txt",split_buffer,timestampstr);
//				split_buffer.clear();
//			}
//		}
//		
//		//dump the rest of update file
//		if(!split_buffer.isEmpty()){
//			timestampstr=String.format("%d", current_time_mills++);
//			timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
//			saveStringBufferToFile(appendix_file_prefix+"_"+(appendix_file_index++)+".txt",split_buffer,timestampstr);
//		}
//		update_file_in.close();
//	}
	
	
}

