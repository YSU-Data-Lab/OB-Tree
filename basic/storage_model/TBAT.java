package basic.storage_model;

import basic.util.DataRetriever;
import basic.btree.OBTree;


import java.io.*;
import java.util.ArrayList;
import static java.lang.System.out;

/**
 * Created by fyu on 11/1/16.
 */
public class TBAT {
    public static final String tbat_format = "%s,%10d,%10d\n";

    /**
     * search in appendix by offset
     * file is the updated tbat file
     * offset must start from 1!!!
     */
    public static long searchAppendixByOffSet(RandomAccessFile file, long num_lines_body, int line_length,long offset, int value_position) throws IOException {
        long value= DataRetriever.NO_VALUE;
        file.seek((offset + num_lines_body - 1) * line_length);
        String line = file.readLine();
        if(line!=null) {
            //out.println("searchAppendixByOffSet:"+line);
            value = Long.parseLong(line.split(",")[value_position].trim());
        }
        file.seek(0);
        return value;
    }


    /**
     * same method
     * save file handler open time
     */
    public static long selectTBAT_body(RandomAccessFile file, long num_lines_body, int line_length, long target_oid) throws IOException{
        long value;
        int oid_position=1;
        value= DataRetriever.binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
        return value;
    }


    /**
     * fyu
     * search only the body of a TBAT using binary search, regardless of the appendix
     * used for searching in combination with btree (which stores data in the appendix)
     */
    public static long selectTBAT_body(String file_name, long num_lines_body, int line_length, long target_oid) throws IOException{
        long value;
        int oid_position=1;
        RandomAccessFile file=new RandomAccessFile(new File(file_name), "r");
        value= DataRetriever.binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
        file.close();
        return value;
    }

    /**
     * use select file
     */
    public static void selectTBAT_body(String tbat_file_name, String select_file_name, long num_lines_body, int line_length) throws IOException{
        BufferedReader select_file=new BufferedReader(new FileReader(select_file_name));
        String str;
        long target_oid;
        long offset;
        long value;
        while((str=select_file.readLine())!=null && str.length()!=0) {
            target_oid = Long.parseLong(str);
            value=selectTBAT_body(tbat_file_name,num_lines_body,line_length,target_oid);
        }
        select_file.close();
    }

    public static void selectTBAT_Uncleaned(String tbat_file_name, String select_file_name, long num_lines_body, int line_length) throws IOException{
        BufferedReader select_file=new BufferedReader(new FileReader(select_file_name));
        String str;
        long target_oid;
        long offset;
        long value;
        while((str=select_file.readLine())!=null && str.length()!=0) {
            target_oid = Long.parseLong(str);
            value=selectTBAT_Uncleaned(tbat_file_name, num_lines_body, line_length, target_oid);
        }
        select_file.close();
    }


    public static long selectTBAT_Uncleaned(String tbat_file_name, long num_lines_body, int line_length, long target_oid) throws IOException{
        long value=0;
        int oid_position=1;
        BufferedReader append_reader=new BufferedReader(new FileReader(tbat_file_name));
        value=searchAppendedFile(append_reader, num_lines_body, line_length, oid_position, target_oid);
        append_reader.close();
        if(value== DataRetriever.NOT_FOUND){
            RandomAccessFile file=new RandomAccessFile(new File(tbat_file_name), "r");
            value= DataRetriever.binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
            file.close();
        }
        return value;
    }

    public static long selectTBAT_Uncleaned2(String file_name, int num_lines_body, int line_length, int target_oid) throws IOException{
        long value=0;
        int oid_position=1;
            RandomAccessFile file=new RandomAccessFile(new File(file_name), "r");
            value= DataRetriever.binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
            file.close();

        return value;
    }


    /**
     * @param oid_position the position of the oid (for tbat =1, for bat=0)
     */
    public static long searchAppendedFile(BufferedReader append_reader, long num_lines_body, int line_length,
            int oid_position, long target_oid) throws IOException{

        append_reader.skip((num_lines_body)*line_length);
        // skip the body of the updated tbat file, only read the appended part at the end
        String current_line;
        int temp_oid;
        long temp_value;
        long value= DataRetriever.NOT_FOUND;

        while((current_line=append_reader.readLine())!=null){
            temp_oid=Integer.parseInt(current_line.split(",")[oid_position].trim());
            if(temp_oid==target_oid){
                value=Integer.parseInt(current_line.split(",")[oid_position+1].trim());
            }
        }
        return value;
    }

    /**
     *
     */
    public static void searchWithOBTree(OBTree obtree, String tbat_file_name, String select_file_name, long num_lines_body, int tbat_line_length) throws IOException{
        BufferedReader select_file=new BufferedReader(new FileReader(select_file_name));
        RandomAccessFile tbat_file=new RandomAccessFile(new File(tbat_file_name), "r");//open
        String str;
        long offset;
        long target_oid;
        long value;
        while((str=select_file.readLine())!=null && str.length()!=0) {
            target_oid = Long.parseLong(str);
            offset=obtree.searchKey(target_oid);
            if(offset!=DataRetriever.NOT_FOUND){
                value= TBAT.searchAppendixByOffSet(tbat_file, num_lines_body, tbat_line_length, offset, 2);//in a tbat, value is at 2 (3rd position in one line)
            }else{
                value= TBAT.selectTBAT_body(tbat_file_name, num_lines_body, tbat_line_length, target_oid);
            }
        }
        tbat_file.close();
        select_file.close();
    }


        /*method for Eric Jones Thesis--begin*/
    /**
     * select the value of the target oid
     * given a tbat file and a list of split appendix files
     */
    public static long selectTBAT_Uncleaned_Split(String tbat_file_name,
                                                  ArrayList<String> appendix_file_names,
                                                  int num_lines_body, int line_length, int target_oid)
            throws IOException{
        long value= DataRetriever.NOT_FOUND;
        int oid_position=1;
        if(!appendix_file_names.isEmpty()){
            for(String appendix_file_name:appendix_file_names){
                BufferedReader append_reader=new BufferedReader(new FileReader(appendix_file_name));
                //no line needs to be skipped in split appendix files
                value=searchAppendedFile(append_reader, 0, line_length, oid_position, target_oid);
                append_reader.close();
                if(value!= DataRetriever.NOT_FOUND) return value;
            }
        }

        RandomAccessFile file=new RandomAccessFile(new File(tbat_file_name), "r");
        value= DataRetriever.binarySearchValue(file, num_lines_body, line_length, oid_position, target_oid);
        file.close();

        return value;
    }

    public static long selectTBAT_Uncleaned_Split2(ArrayList<String> appendix_file_names,
                                                   int num_lines_body, int line_length, int target_oid) throws IOException{
        long value = DataRetriever.NOT_FOUND;
        //int oid_position=1;
        if(!appendix_file_names.isEmpty()){
            for(String appendix_file_name:appendix_file_names){
                RandomAccessFile appendix_file = new RandomAccessFile(appendix_file_name, "r");
                value = searchAppendixByOffSet(appendix_file, 0, line_length,
                        target_oid, 1);
                if(value!= DataRetriever.NOT_FOUND) return value;

            }
        }
        return value;
    }
    /*Method by Eric Jones for Thesis--end*/

}
