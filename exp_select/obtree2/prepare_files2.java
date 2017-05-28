package exp_select.obtree2;

import basic.util.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import static java.lang.System.out;

public class prepare_files2{

    static String data_folder="data/obtree2/";

    public static void main(String[] args) throws IOException {
        String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
        long start=System.currentTimeMillis();

        long num_lines = 0, max_exp_times=0;
        ArrayList<Double> pers_update=new ArrayList();
        ArrayList<Double> pers_select=new ArrayList();

        if(args.length<5){
            out.println("Please input: num_lines " +
                    "-u update_per1 update_per2 update_per3 ... " +
                    "-s select_per1 select_per2 select_per3 ... ");
            System.exit(0);
        }else{
            //---parse arguments---
            num_lines = Long.parseLong(args[0]);
            int index=1;
            ArrayList temp_array=null;
            while(index<args.length){
                switch(args[index].toLowerCase()){
                    case "-u":
                        temp_array=pers_update;
                        break;
                    case "-s":
                        temp_array=pers_select;
                        break;
                    default:
                        temp_array.add(Double.parseDouble(args[index]));
                        break;
                }
                index++;
            }
        }

        out.println("num_lines:"+num_lines);
        out.println("pers_update:"+pers_update);
        out.println("pers_select:"+pers_select);

        for(double per:pers_update){
            out.println("per_update="+per);

            //tbat 1 for append update
            //tbat 2 for merge update (clean update)
            String tbat_file_name1=data_folder+"tbat_l"+num_lines+"_p"+per+"_1.txt";
            String tbat_file_name2=data_folder+"tbat_l"+num_lines+"_p"+per+"_2.txt";

            //create tbat 1 and 2
            DataCreator.prepareTBAT(num_lines,tbat_file_name2);
            BasicTools.copyFile(tbat_file_name2,tbat_file_name1);
            out.println("\t tbat file 1 and 2 created");
//            out.println("\t tbat file 2 created");

            //create update files
            //example update file name: "update_l1000_p0.01.txt"
            String update_file_name=data_folder+"update_l"+num_lines+"_p"+per+".txt";
            DataCreator.prepareUpdateList5(per, num_lines, update_file_name);
            out.println("\t update list created");

            //append update tbat1
//            DataUpdator.updateTBAT(tbat_file_name1, update_file_name);
//            out.println("\t tbat 1 updated (append)");

            //merge update tbat2
            int buffer_size=1_000_000;
            DataUpdator.sortMergeFileToTBAT41(tbat_file_name2, update_file_name, 0, buffer_size);
            out.println("\t tbat 2 updated (merge)");
        }

        //---create select files---
        //example select file name: "select_l1000_p0.01.txt"
        String select_file_name;
        for(double per:pers_select){
            out.println("per_select="+per);
            select_file_name=data_folder+"select_l"+num_lines+"_p"+per+".txt";
            if(per>0)
                DataCreator.prepareSelectionFile5(select_file_name,per,num_lines);
        }

        long end=System.currentTimeMillis();
        double elapsedTime=(end-start)/1000.0;
        out.println("Elapsed Time:"+elapsedTime+"s");
    }

}
