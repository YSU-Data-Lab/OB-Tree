package basic.util;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import static java.lang.System.out;

public class DataRetriever {
//	public static int DEFAULT_BUFFER_SIZE=8192;

	/**
	 * 
	 * select the value of the target oid
	 * given a tbat with appendix appended at the end of the tbat file
	 * 
	 * @param file_name
	 * @param num_lines_body
	 * @param line_length
	 * @param oid_position the position of the oid (for tbat =1, for bat=1)
	 * @param target_oid
	 * @return target_value
	 * @throws IOException
	 */
	
	//public static final int NOT_FOUND=Integer.MIN_VALUE;
	//public static final long NOT_FOUND=Long.MIN_VALUE;
	public static final long NOT_FOUND=-1;
	public static final long NO_VALUE=-9999;


	/**
	 * binary search for oid in the body of tbat (not the appended part)
	 * @param oid_position the position of the oid (for tbat =1, for bat=1)
	 */
	public static long binarySearchValue(RandomAccessFile file, long num_lines_body, int line_length,
			int oid_position, long target_oid) throws IOException{
		long low=0;
		long high=num_lines_body-1;
		long mid, oid_mid;
		String bat_current_line;
		
		while(low<=high){
			mid=(low+high)/2;
			file.seek(mid*line_length);
			bat_current_line=file.readLine();
			oid_mid=Long.parseLong(bat_current_line.split(",")[oid_position].trim());
			if(oid_mid == target_oid){
				//file.seek(0);//reset file pointer after updating
				//System.out.println("found at: "+oid_mid);
				return oid_mid;
			}else if(oid_mid < target_oid) low=mid+1;
			else high=mid-1;
		}
		System.out.println("Not found");
		return Integer.MIN_VALUE;
	}


	/**
	 * searchKey the length of one line in a file
	 */
	public static int getLineLength(String file_name) throws IOException {
		RandomAccessFile randomReader=new RandomAccessFile(new File(file_name),"r");
		String first_line=randomReader.readLine();
		randomReader.close();
		int line_length=first_line.length()+1;//include '\n'
		return line_length;
	}
	
	/**
	 * searchKey the total line numbers in a file
	 * reference: http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	 */
	public static int getFileLineNumber(String file_name) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file_name));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean endsWithoutNewLine = false;
			while ((readChars = is.read(c)) != -1) {
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n')
						++count;
				}
				endsWithoutNewLine = (c[readChars - 1] != '\n');
			}
			if (endsWithoutNewLine) {
				++count;
			}
			return count;
		} finally {
			is.close();
		}
	}

}



//	/**
//	 * binary search TBAT body
//	 * this method don't need line_length
//	 */
//	public static long selectTBAT_body(String file_name,int num_lines_body, int target_oid) throws IOException{
//		long value=0;
//		int oid_position=1;
//		RandomAccessFile file=new RandomAccessFile(new File(file_name), "r");
//		String first_line=file.readLine();
//		file.seek(0);
//		int line_length=first_line.length()+1;//include '\n'
//		value=binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
//		file.close();
//		return value;
//	}
