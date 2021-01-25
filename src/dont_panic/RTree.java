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

public class RTree  extends Tree implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private RTreeNode root;
	private String tablename;
	private String coloumn;
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public RTree(String table,String coloumn,int order) 
	{
		tablename=table;
		this.coloumn=coloumn;
		this.order = order;
		root =  new RTreeLeafNode(this.order);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public void insert(Object key, Ref recordReference)
	{
		System.out.println("one time");
		RPushup pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode newRoot = new RTreeInnerNode(order);
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

	
	public RTreeLeafNode searchGreaterThan(Object key)  
	{
		return root.searchGreaterThan(key);
	}
	
	public ArrayList<Ref> greaterthan(Object key){
		System.out.println("what");
		RTreeLeafNode Node=searchGreaterThan(key);
		
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < Node.numberOfKeys; i++) {
				if(DBApp.compare(Node.getKey(i),key)>0) {
					System.out.println("asdasdasdadsa");
					result.add(Node.getRecord(i));
				}else {
					continue;
				}
			}
			if(Node.getNext()!=null) {
				Node=Node.getNext();
			}else {
				System.out.println("not lying+r"+result.size());
				return result;
				
			}
		}
 	}
	
	
	public void shiftPage(int pageDeleted) {
	   RTreeLeafNode node=(RTreeLeafNode) getmin();
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

	public RTreeNode getmin() {
		RTreeNode min=root;
		while(!(min instanceof RTreeLeafNode)) {
			RTreeInnerNode parent=(RTreeInnerNode) min;
			min=parent.getFirstChild();
		}
		return min;
	}
	

	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<RTreeNode> cur = new LinkedList<RTreeNode>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<RTreeNode>();
			while(!cur.isEmpty())
			{
				RTreeNode curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof RTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					RTreeInnerNode parent = (RTreeInnerNode) curNode;
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
	public void save() throws IOException {
		File file=new File("data\\"+tablename+"-"+coloumn+".class");
		FileOutputStream f = new FileOutputStream(file);        //ba3mel save lel table
		ObjectOutputStream o = new ObjectOutputStream(f);

			
				 o.writeObject(this);
				    o.close();
				    f.close();
	}



	@Override
	public Ref search(Object key) {
		// TODO Auto-generated method stub
		return root.search(key);
	}

	@Override
	public ArrayList<Ref> greaterThanOrEqual(Object key) {
		RTreeLeafNode Node=searchGreaterThan( key);
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < Node.numberOfKeys; i++) {
				if(DBApp.compare(Node.getKey(i), key)>=0) {
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
		boolean done = root.delete( key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode) root).getFirstChild();
		return done;
	}

	@Override
	public ArrayList<Ref> getLessThan(Object key) {
		RTreeLeafNode leftMostNode=(RTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(DBApp.compare(leftMostNode.getKey(i), key)<0) {
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
		RTreeLeafNode leftMostNode=(RTreeLeafNode) this.getmin();
		ArrayList<Ref> result=new ArrayList<Ref>();
		while(true) {
			for (int i = 0; i < leftMostNode.numberOfKeys; i++) {
				if(DBApp.compare(leftMostNode.getKey(i), key)<=0) {
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


		RTreeLeafNode leaf=searchGreaterThan(key);
		Ref ref=leaf.getPage(key);
		
		if(ref instanceof RefPage) {
			System.out.println("wadup");
			return ((RefPage) ref).getPage();
		}else {
			System.out.println("wadup");
			return ((RefOverFlowPage) ref).maxReference();
		}		
	}
	
}