package dont_panic;

import java.io.Serializable;

public abstract class Ref implements Serializable{
	protected Object key ;
	public Object getKey() {
		return key;
	}
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	private static final long serialVersionUID = 1L;

	public abstract void minusPage(int deleted) ;

	
	/**
	 * @return the page at which the record is saved on the hard disk
	 */


}