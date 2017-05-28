package basic.storage_model;
/**
 * Created by fyu on 11/1/16.
 */
import basic.util.DataRetriever;

import java.io.*;

public class BAT {
    public static final String bat_format = "%10d,%10d\n";

    public static void searchSelectFile(String bat_file_name, String select_file_name, long num_lines_body, int bat_line_length) throws IOException{
        BufferedReader select_file=new BufferedReader(new FileReader(select_file_name));
        String str;
        long target_oid;
        long offset;
        long value;
        while((str=select_file.readLine())!=null && str.length()!=0) {
            target_oid = Long.parseLong(str);
            value=selectBAT(bat_file_name, num_lines_body, bat_line_length, target_oid);
        }
        select_file.close();
    }

    public static long selectBAT(String file_name,long num_lines, int line_length,long target_oid) throws IOException{
        int oid_position=0;
        RandomAccessFile file=new RandomAccessFile(new File(file_name), "r");
        long value= DataRetriever.binarySearchValue(file, num_lines, line_length, oid_position, target_oid);
        file.close();
        return value;
    }
}
