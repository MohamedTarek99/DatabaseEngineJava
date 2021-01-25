package dont_panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class BPTree<T extends Comparable<T>> extends Tree implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private BPTreeNode<T> root;
	private String tablename;
	private String coloumn;
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public BPTree(String table,String coloumn,int order) 
	{
		tablename=table;
		this.coloumn=coloumn;
		this.order = order;
		root = new BPTreeLeafNode<T>(this.order);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public void insert(T key, Ref recordReference)
	{
		System.out.println("one time");
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public Ref search(T key)
	{
		return root.search(key);
	}
	
	public BPTreeLeafNode searchGreaterThan(Object key)  
	{
		return root.searchGreaterThan((T) key);
	}
	
	public ArrayList<Ref> greaterthan(Object key){
		BPTreeLeafNode Node=searchGreaterThan( key);
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < Node.numberOfKeys; i++) {
				if(Node.getKey(i).compareTo(key)>0) {
					System.out.println(Node.getKey(i));
					System.out.println(key);
					result.add(Node.getRecord(i));
				}else {
					continue;
				}
			}
			if(Node.getNext()!=null) {
				Node=Node.getNext();
			}else {
				return result;
			}
		}
 	}
	
	public ArrayList<Ref> greaterThanOrEqual(T key){
		BPTreeLeafNode Node=searchGreaterThan(key);
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < Node.numberOfKeys; i++) {
				if(Node.getKey(i).compareTo(key)>=0) {
					result.add(Node.getRecord(i));
				}else {
					continue;
				}
			}
			if(Node.getNext()!=null) {
				Node=Node.getNext();
			}else {
				return result;
			}
		}
 	}
	public void shiftPage(int pageDeleted) {
	   BPTreeLeafNode node=(BPTreeLeafNode) getmin();
	   while(true) {
		   for (int i = 0; i <node.numberOfKeys; i++) {
			node.getRecord(i).minusPage(pageDeleted);
		}
		   if(node.getNext()!=null) {
			   node=node.getNext();
		   }else {
			   break;
		   }
	   }
	   
	   
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 */
	public boolean delete(T key)
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	
	public BPTreeNode getmin() {
		BPTreeNode min=root;
		while(!(min instanceof BPTreeLeafNode)) {
			BPTreeInnerNode parent=(BPTreeInnerNode) min;
			min=parent.getFirstChild();
		}
		return min;
	}
	
	public ArrayList<Ref> getLessThan(T key){
		System.out.println("ASd");
		BPTreeLeafNode leftMostNode=(BPTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(leftMostNode.getKey(i).compareTo(key)<0) {
					System.out.println(leftMostNode.getKey(i));
					System.out.println(key);
					result.add(leftMostNode.getRecord(i));
				}else {
					return result;
				}
			}
			if(leftMostNode.getNext()!=null) {
				leftMostNode=leftMostNode.getNext();
			}else {
				return result;
			}
		}
	}
	public ArrayList<Ref> getLessOrEqualThan(T key){
		BPTreeLeafNode leftMostNode=(BPTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(leftMostNode.getKey(i).compareTo(key)<=0) {
					result.add(leftMostNode.getRecord(i));
				}else {
					return result;
				}
			}
			if(leftMostNode.getNext()!=null) {
				leftMostNode=leftMostNode.getNext();
			}else {
				return result;
			}
		}
	}
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				BPTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof BPTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						System.out.print(parent.getChild(i).index+",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}	
		//	</For Testing>
		return s;
	}
	public void shiftRef(T key,int oldpage) {
		Ref ref=search(key);
		if(ref instanceof RefPage) {
			RefPage page=(RefPage) ref;
			page.shiftPage();
		}else {
			RefOverFlowPage page= (RefOverFlowPage) ref;
			LinkedList overflowpages=page.getPointers();
			for (int i = 0; i < overflowpages.size(); i++) {
				OverFlowPage op=(OverFlowPage) overflowpages.get(i);
				for (int j = 0; j < op.size(); j++) {
					RefPage reference=(RefPage) op.get(j);
					if(reference.getPage()==oldpage) {
						reference.shiftPage();
						return;
					}
				}
			}
		}
	}
	public void save() throws IOException {
		File file=new File("data\\"+tablename+"-"+coloumn+".class");
		FileOutputStream f = new FileOutputStream(file);        //ba3mel save lel table
		ObjectOutputStream o = new ObjectOutputStream(f);

			
				 o.writeObject(this);
				    o.close();
				    f.close();
	}
	public int getPage(T key) {

	BPTreeLeafNode leaf=searchGreaterThan(key);
	Ref ref=leaf.getPage(key);
	
	if(ref instanceof RefPage) {
		System.out.println("wadup");
		return ((RefPage) ref).getPage();
	}else {
		System.out.println("wadup");
		return ((RefOverFlowPage) ref).maxReference();
	}
	}

	@Override
	public void insert(Object key, Ref recordReference) {
		insert((T) key,recordReference);
		
	}

	@Override
	public Ref search(Object key) {
		// TODO Auto-generated method stub
		return search((T) key);
	}

	@Override
	public ArrayList<Ref> greaterThanOrEqual(Object key) {
		BPTreeLeafNode Node=searchGreaterThan((T) key);
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < Node.numberOfKeys; i++) {
				if(Node.getKey(i).compareTo(key)>=0) {
					result.add(Node.getRecord(i));
				}else {
					continue;
				}
			}
			if(Node.getNext()!=null) {
				Node=Node.getNext();
			}else {
				return result;
			}
		}
	}

	@Override
	public boolean delete(Object key) {
		boolean done = root.delete((T) key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}

	@Override
	public ArrayList<Ref> getLessThan(Object key) {
		BPTreeLeafNode leftMostNode=(BPTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(leftMostNode.getKey(i).compareTo(key)<0) {
					System.out.println(leftMostNode.getKey(i));
					System.out.println(key);
					result.add(leftMostNode.getRecord(i));
				}else {
					return result;
				}
			}
			if(leftMostNode.getNext()!=null) {
				leftMostNode=leftMostNode.getNext();
			}else {
				return result;
			}
		}
	}

	@Override
	public ArrayList<Ref> getLessOrEqualThan(Object key) {
		BPTreeLeafNode leftMostNode=(BPTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(leftMostNode.getKey(i).compareTo(key)<=0) {
					result.add(leftMostNode.getRecord(i));
				}else {
					return result;
				}
			}
			if(leftMostNode.getNext()!=null) {
				leftMostNode=leftMostNode.getNext();
			}else {
				return result;
			}
		}
	}

	@Override
	public void shiftRef(Object key, int oldpage) {
		// TODO Auto-generated method stub
		Ref ref=search(key);
		if(ref instanceof RefPage) {
			RefPage page=(RefPage) ref;
			page.shiftPage();
		}else {
			RefOverFlowPage page= (RefOverFlowPage) ref;
			LinkedList overflowpages=page.getPointers();
			for (int i = 0; i < overflowpages.size(); i++) {
				OverFlowPage op=(OverFlowPage) overflowpages.get(i);
				for (int j = 0; j < op.size(); j++) {
					RefPage reference=(RefPage) op.get(j);
					if(reference.getPage()==oldpage) {
						reference.shiftPage();
						return;
					}
				}
			}
		}
		
	}
	
	@Override
	public int getPage(Object key) {
		// TODO Auto-generated method stub

          return getPage((T) key);
		
	}
	
}