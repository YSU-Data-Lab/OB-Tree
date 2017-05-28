package exp_select.obtree2;

import basic.util.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import static java.lang.System.out;

public class exp_merge_tbat2 {
    static String creation_time_format="%-10s\t %-10s\t %-10s\n";
    static String data_folder="data/obtree2/";

	public static void main(String[] args) throws IOException {
		String program_start_date_time=new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ").format(Calendar.getInstance().getTime());
		long start=System.currentTimeMillis();

        long num_lines = 0;
        ArrayList<Double> pers_update=new ArrayList();

        if(args.length<3){
            out.println("% Please input: num_lines " +
                    "-u update_per1 update_per2 update_per3 ... ");
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
                    default:
                        temp_array.add(Double.parseDouble(args[index]));
                        break;
                }
                index++;
            }
        }

        out.println("% num_lines:"+num_lines);
        out.println("% pers_update:"+pers_update);

        for(double per:pers_update){
//            out.println("%\t per_update="+per);

            //tbat 1 for append update
            //tbat 2 for merge update (clean update)
            String tbat_file_name2=data_folder+"tbat_l"+num_lines+"_p"+per+"_2.txt";

            //create tbat 1 and 2
            DataCreator.prepareTBAT(num_lines,tbat_file_name2);
//            out.println("%\t tbat file 1 and 2 created");
//            out.println("\t tbat file 2 created");

            //create update files
            //example update file name: "update_l1000_p0.01.txt"
            String update_file_name=data_folder+"update_l"+num_lines+"_p"+per+".txt";
            DataCreator.prepareUpdateList5(per, num_lines, update_file_name);
//            out.println("%\t update list created");

            //merge update tbat2
            long tbat_merge_start=System.currentTimeMillis();
            DataUpdator.sortMergeFileToTBAT41(tbat_file_name2, update_file_name, 0);
            Double tbat_merge_time=(double)(System.currentTimeMillis()-tbat_merge_start)/1000.0d;
            out.println("%\t merge time:");
            out.format(creation_time_format,num_lines,per,tbat_merge_time);
        }
		
		long end=System.currentTimeMillis();
		double elapsedTime=(end-start)/1000.0;
		out.println("% Elapsed Time:"+elapsedTime+"s");
	}

}
