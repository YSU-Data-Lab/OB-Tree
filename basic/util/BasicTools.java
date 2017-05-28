package basic.util;
import java.io.*;
import java.lang.instrument.Instrumentation;

public class BasicTools {
	public static void copyFile(String file_in_name, String file_out_name) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(file_in_name));
	    BufferedWriter bw = new BufferedWriter(new FileWriter(file_out_name));
	    int i;
	    do {
	      i = br.read();
	      if (i != -1) {
	        if (Character.isLowerCase((char) i))
	          bw.write(Character.toUpperCase((char) i));
	        else if (Character.isUpperCase((char) i))
	          bw.write(Character.toLowerCase((char) i));
	        else
	          bw.write((char) i);
	      }
	    } while (i != -1);
	    br.close();
	    bw.close();
	}

	private static Instrumentation instrumentation;
	
	public static void premain(String args, Instrumentation inst) {
	        instrumentation = inst;
	}
	 
	public static long getObjectSize(Object o) {
	        return instrumentation.getObjectSize(o);
	}
	
	
	/**
	 * reference:https://www.cs.cmu.edu/~adamchik/15-121/lectures/Sorting%20Algorithms/code/MergeSort.java
	 */
	public static void mergeSort(Comparable [ ] a)
	{
		Comparable[] tmp = new Comparable[a.length];
		mergeSort(a, tmp,  0,  a.length - 1);
	}

	private static void mergeSort(Comparable [ ] a, Comparable [ ] tmp, int left, int right)
	{
		if( left < right )
		{
			int center = (left + right) / 2;
			mergeSort(a, tmp, left, center);
			mergeSort(a, tmp, center + 1, right);
			merge(a, tmp, left, center + 1, right);
		}
	}

    private static void merge(Comparable[ ] a, Comparable[ ] tmp, int left, int right, int rightEnd )
    {
        int leftEnd = right - 1;
        int k = left;
        int num = rightEnd - left + 1;

        while(left <= leftEnd && right <= rightEnd)
            if(a[left].compareTo(a[right]) <= 0)
                tmp[k++] = a[left++];
            else
                tmp[k++] = a[right++];

        while(left <= leftEnd)    // Copy rest of first half
            tmp[k++] = a[left++];

        while(right <= rightEnd)  // Copy rest of right half
            tmp[k++] = a[right++];

        // Copy tmp back
        for(int i = 0; i < num; i++, rightEnd--)
            a[rightEnd] = tmp[rightEnd];
    }
	
	
}
