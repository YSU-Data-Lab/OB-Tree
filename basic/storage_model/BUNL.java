package basic.storage_model;

/**
 * use long for oid
 */
public class BUNL<T> {
	public long oid;
	public T value;
	public BUNL(long oid, T value){
		this.oid=oid;
		this.value=value;
	}
	public String toString(){
		return "("+oid+","+value+")";
	}
}
