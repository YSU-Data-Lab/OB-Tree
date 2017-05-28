package basic.btree;

/**
 * Entry in a node. Internal nodes only use key and next while External Nodes
 * use key and value
 * 
 * @author cgavidia
 * 
 * @param <Key>
 *            Type of the Search Key
 * @param <Value>
 *            Type of the Value Stored
 */
public class Entry<Key extends Comparable<Key>, Value> {

	private Key key;
	private Value value;
	private Node<Key, Value> next; // Helper field to iterate over array entries
	//--for space calculation--beginning
	public static final int key_size=4;
	public static final int value_size=4;
	public static final int pointer_size=4;
	//--for space calculation--end
	
	public Entry(Key key, Value value, Node<Key, Value> next) {
		this.key = key;
		this.value = value;
		this.next = next;
	}

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	public Node<Key, Value> getNext() {
		return next;
	}

	public void setNext(Node<Key, Value> next) {
		this.next = next;
	}

	@Override
	public String toString() {
		return "(Key: " + key + " Value: " + value + ")";
	}

}