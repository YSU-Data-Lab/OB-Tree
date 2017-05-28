package basic.util;

import java.io.*;

public class FileSplitter {

	// total splitting partitions
	public static int total_partition_num=3;
	
	public FileSplitter() {
		
	}
	
	public FileSplitter(int total_partition_num) {
		this.total_partition_num=total_partition_num;
	}
	
	public void setTotalPartitionNum(int total_partition_num) {
		this.total_partition_num=total_partition_num;
	}

	public static void splitByPartitionNum(String input_file_name, int total_partition_num) throws IOException{
		long file_line_num = DataRetriever.getFileLineNumber(input_file_name);
		long chunk_line_num = (file_line_num -1)/ total_partition_num +1 ;//line number in each chunk. rounded up!
//		System.out.println("chunk_line_num="+chunk_line_num);
		BufferedReader input_file=new BufferedReader(new FileReader(input_file_name)); 
		long current_chunk_line_num=0;//line number in current chunk
		int current_chunk_num=1;
		PrintWriter output_file = new PrintWriter(new BufferedWriter(new FileWriter(input_file_name+"_"+current_chunk_num)));
		String current_line;
		while((current_line=input_file.readLine())!=null){
			if(current_chunk_line_num < chunk_line_num){
				output_file.println(current_line);
				current_chunk_line_num++;
			}else {
				output_file.close();				
				current_chunk_num++;
				output_file = new PrintWriter(new BufferedWriter(new FileWriter(input_file_name+"_"+current_chunk_num)));
				output_file.println(current_line);
				current_chunk_line_num=1;
			}
		}
		output_file.close();
		input_file.close();
	}

}
