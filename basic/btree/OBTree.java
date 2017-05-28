package basic.btree;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import basic.util.DataRetriever;
import basic.storage_model.TBAT;

/**
 * OBTree use Long oid, and Long offset to insert into BTree
 */
@SuppressWarnings("unchecked")
public class OBTree extends BTree <Long, Long>{
	long total_inserts;
//	public OBTree(){
//		super();
//		key_max=key_min=0L;//in OBTree initially both max and min keys (oid) are 0
//	}

	public long loadUpdateFile(String update_file_name) throws IOException{//if file read had timestamp use this
		this.total_inserts=0;
		long off=1;
		String a;
		long b;
		long oid;
		Scanner reads = new Scanner(new File(update_file_name));
		while (reads.hasNext()) {
			a = reads.next(); // read OID
			b = reads.nextLong(); // read VALUE
			a = a.substring(0, a.length() - 1); // removing the comma that was auto-generated
			oid = Long.parseLong(a); // placing that number into a variable
			if (get(oid) != null) {
				findReplace(oid, off);
			} else {
				put(oid, off);
				total_inserts++;
			}// end of if-else
			off++;
		}
		reads.close();
		return total_inserts;
	}

	/**
	 * load update the appendix of an updated file into a new BTree
	 */
	public OBTree loadAppendixIntoOBTree(String update_file_name) throws IOException{
		return new OBTree().loadAppendixIntoOBTree(update_file_name);
	}

	/**
	 *
	 * @param update_file_name
	 * @param line_width
	 * @param start_line_num >=1
	 * @param end_line_num >=1
	 * @return
	 * @throws IOException
	 */
	public long loadAppendixRangeIntoOBTree(String update_file_name, int line_width, long start_line_num, long end_line_num) throws IOException{
		total_inserts=0;
		long current_line_num=start_line_num;
		BufferedReader input_file=new BufferedReader(new FileReader(update_file_name));
		input_file.skip((start_line_num-1)*line_width);//skip first start_line_num - 1 lines
		String current_line, a, b;
		long oid;
		long off=current_line_num;//offset starts with current line num in the update file
		while((current_line=input_file.readLine())!=null && current_line_num <= end_line_num){
			//only take the 1st part of "oid, val" after split and convert to long oid
			oid=Long.parseLong(current_line.split(",")[0].trim());
			if(get(oid)!=null){//if this oid already exists in obtree
				findReplace(oid,off);//replace with new offset
			} else {//o.w. insert this new oid
				put(oid, off);
				total_inserts++;
			}
			off++;
			current_line_num++;
		}
		input_file.close();
		return total_inserts;
	}

	public long getTotal_inserts(){return total_inserts;}

	public long searchKey(long oid){
		Long offset=get(oid);
		if(offset!=null){
			return offset;
		}else{
			return DataRetriever.NOT_FOUND;
		}
	}

	/**
	 * obtree selection experiment using a selection file
	 * @param tbat_file_name
	 * @param select_file_name
	 * @param num_lines_body
	 * @param tbat_line_length
	 * @param search_value if true the searching for value by offset will be used
	 * @throws IOException
	 */
	public void searchSelectionFile(String tbat_file_name, String select_file_name, long num_lines_body, int tbat_line_length, boolean search_value) throws IOException{
		BufferedReader select_file=new BufferedReader(new FileReader(select_file_name));
		RandomAccessFile tbat_file=new RandomAccessFile(new File(tbat_file_name), "r");
		String str;
		long target_oid;
		long offset;
		long value;
		while((str=select_file.readLine())!=null && str.length()!=0){
			target_oid=Long.parseLong(str);
			offset=searchKey(target_oid);
			if(search_value) {
				if (offset != DataRetriever.NOT_FOUND) {
					value = TBAT.searchAppendixByOffSet(tbat_file, num_lines_body, tbat_line_length, offset, 2);//in a tbat, value is at 2 (3rd position in one line)
					//out.println("***found in obtree: oid="+target_oid+" | value="+value);
				} else {
					value = TBAT.selectTBAT_body(tbat_file,num_lines_body,tbat_line_length,target_oid);
				}
			}
		}
		tbat_file.close();
		select_file.close();
	}

}




