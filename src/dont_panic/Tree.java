package dont_panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.awt.Polygon;

public abstract class Tree implements Serializable {

	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public abstract void insert(Object key, Ref recordReference);
	
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public abstract Ref search(Object key);
	
	
	public abstract ArrayList<Ref> greaterThanOrEqual(Object key);
	
	public abstract void shiftPage(int pageDeleted) ;
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 */
	public abstract boolean delete(Object key);
	
	public abstract ArrayList<Ref> getLessThan(Object key);
	
	
	
	public abstract ArrayList<Ref> getLessOrEqualThan(Object key);
		
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public abstract String toString();
	public abstract void shiftRef(Object key,int oldpage) ;
		
	
	public abstract void save() throws IOException ;
		
	
	public abstract int getPage(Object key) ;

	public abstract ArrayList<Ref> greaterthan(Object key);
	
	
	
	

}
