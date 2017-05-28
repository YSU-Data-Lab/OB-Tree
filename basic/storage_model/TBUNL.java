package basic.storage_model;

/**
 * same as TBUN but use Long for oid
 */

public class TBUNL<T>extends BUNL implements Comparable <TBUNL>{
	public long timestamp;
	public static final String tbat_format = "%s,%10d,%10d";
	public TBUNL(long timestamp, long oid, T value) {
		super(oid, value);
		this.timestamp=timestamp;
	}

	public String toString(){
		String timestampstr=String.format("%d", timestamp);
		if(timestampstr.length()>=8){
			timestampstr=timestampstr.substring(timestampstr.length()-8,timestampstr.length());
		}
		return String.format(tbat_format, timestampstr, oid, value);
	}

	/**
	 * it's only for sorting, no need to get the actual comparing difference
	 */
	public int compareTo(TBUNL tbun2){
		long diff_oid=oid-tbun2.oid;
		if(diff_oid!=0){
			return (int)diff_oid;
		}else{
			return (int)(timestamp-tbun2.timestamp);
		}
	}
	
}
