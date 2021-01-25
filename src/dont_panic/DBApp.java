package dont_panic;

import java.awt.List;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

public class DBApp {
	public DBApp() {
		
	}
	
	
	
	public void init() throws IOException {
		Properties properties=new Properties();
		properties.put("MaximumRowsCountinPage","20");
		properties.put("NodeSize", "15");
		 String path = "config//DBApp.properties";
	      OutputStream outputStream = new FileOutputStream(path);
	      properties.store(outputStream,"test");
	      outputStream.close();
	      
	}
	
	//el create table ba3mel file be esm el table we law fe file already mawgood be nafs el esm bathrow 
	//exception en fe already table be nafs el esm law la2 ba3mel table object we ba7oto el coloumn types
	//fel metadata.csv
	
	public void createTable(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException, IOException {
		File file=new File("data//"+strTableName+".class");
		Table table=new Table(strTableName,strClusteringKeyColumn);
		if(file.exists()) {
			throw new DBAppException("There already exists a table with this name!"); 
		}else {
			table.save();
			File meta=new File("data//metadata.csv");

			if(meta.exists()) {
				FileWriter FW = new FileWriter(meta,true);
				for(String column_name : htblColNameType.keySet()) {
					FW.append(strTableName);
					FW.append(",");
					FW.append(column_name);
					FW.append(",");
					FW.append(htblColNameType.get(column_name));
					FW.append(",");
                    FW.append(""+strClusteringKeyColumn.equals(column_name));
					FW.append(",");
                    FW.append("False");
                    FW.append("\n");

				}
				FW.flush();
				FW.close();
				
				
				
			}else {
				FileWriter FW=new FileWriter(meta);
				FW.append("Table Name");
				FW.append(",");
				FW.append("Column Name");
				FW.append(",");
				FW.append("Column Type");
				FW.append(",");
				FW.append("ClusteringKey");			
				FW.append(",");
				FW.append("Inedexed");
                FW.append("\n");
                
            	for(String column_name : htblColNameType.keySet()) {
					FW.append(strTableName);
					FW.append(",");
					FW.append(column_name);
					FW.append(",");
					FW.append(htblColNameType.get(column_name));
					FW.append(",");
                    FW.append(""+strClusteringKeyColumn.equals(column_name));
					FW.append(",");
                    FW.append("False");
                    FW.append("\n");

				}
				FW.flush();
				FW.close();
				
						

				
			}
		}
		
	}
	
	
 public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException{
			File file=new File("data//"+strTableName+".class");
		  if(!file.exists()) {
			  throw new DBAppException("Table not found");
		  }else {
			  if(checkColoumnsInsert(strTableName,htblColNameValue)==false) {
				  throw new DBAppException("Invalid inputs");
			  }
				Date date = new Date();
	       		 DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	             	  htblColNameValue.put("TouchDate",dateFormat.format(date));
			  Table table=getTable(strTableName);
			  Vector<PageInfo> pages=table.getPages();
			  if(pages.isEmpty()) {
             	  Page page=new Page(table.getKey());
             	  insertToAllIndexes(table, htblColNameValue, 0);
             	  page.add(htblColNameValue);
             	  PageInfo info=new PageInfo(page.getmax(),page.getmin(),page.save());
				  table.insertPage(info);
				  table.save();
				  
				
			  }else {
				/*
				 * Vector<PageInfo> infos=table.getPages(); PageInfo info=infos.get(0);
				 * Page<Hashtable> page=getpage(info.getData()); insertsort(htblColNameValue,
				 * page, table.getKey()); page.save(); table.save();
				 */
				  String key=table.getKey();
				  
				  if(indexed(key,strTableName)) {
					  Tree tree= getTree(strTableName, key);
					int x=  tree.getPage(htblColNameValue.get(key));
					Page target=getpage(pages.get(x).getData());
					if(target.isFull()) {
				 binaryInserttest(target, key, htblColNameValue,0,target.size()-1);
					insertToAllIndexes(table, htblColNameValue, x);
					target.save();
					shift(table.getName(),pages, x, key);
					 table.checklast();
					 table.save();
					 return;
					}else {
						 binaryInserttest(target, key, htblColNameValue,0,target.size()-1);
						insertToAllIndexes(table, htblColNameValue, x);
						pages.get(x).setMax(target.getmax());
						 pages.get(x).setMin(target.getmin());
						 pages.get(x).setfull(target.isFull());
						 target.save();
						 table.save();
						 return;
                           
					}
				  }
				for (int i = 0; i < pages.size(); i++) {
					if(compare(pages.get(i).getMax(),htblColNameValue.get(key))>=0) {
						if(pages.get(i).isfull) {
							PageInfo target=pages.get(i);
						 Page pagetarget=getpage(target.getData());
						 binaryInserttest(pagetarget, key, htblColNameValue,0,pagetarget.size()-1);
							insertToAllIndexes(table, htblColNameValue, i);
						 pagetarget.save();
						 shift(table.getName(),pages,i,key);
						 table.checklast();
						 table.save();
						 return;
						}else {
							PageInfo target=pages.get(i);
							 Page pagetarget=getpage(target.getData());
							 binaryInserttest(pagetarget, key, htblColNameValue,0,pagetarget.size()-1);
								insertToAllIndexes(table, htblColNameValue, i);
							 target.setMax(pagetarget.getmax());
							 target.setMin(pagetarget.getmin());
							 target.setfull(pagetarget.isFull());
							 pagetarget.save();
							 table.save();
							 return;
						}
					}else {
						if(i==pages.size()-1 && pages.get(i).isfull) {
							PageInfo target=pages.get(i);
							 Page pagetarget=getpage(target.getData());
							 binaryInserttest(pagetarget, key, htblColNameValue,0,pagetarget.size()-1);
								insertToAllIndexes(table, htblColNameValue, i);
							 pagetarget.save();
							 shift(table.getName(),pages,i,key);
							 table.checklast();
							 table.save();
							 return;
						}
						if(i==pages.size()-1) {
							PageInfo target=pages.get(i);
							 Page pagetarget=getpage(target.getData());
							 binaryInserttest(pagetarget, key, htblColNameValue,0,pagetarget.size()-1);
								insertToAllIndexes(table, htblColNameValue, i);
							 target.setMax(pagetarget.getmax());
							 target.setMin(pagetarget.getmin());
							 target.setfull(pagetarget.isFull());
							 pagetarget.save();
							 table.save();
							 return;
						}
						
						if(pages.get(i).isfull==false) {
 							if(compare(pages.get(i+1).getMin(),htblColNameValue.get(table.getKey()))>=0){
 								PageInfo target=pages.get(i);
 								 Page pagetarget=getpage(target.getData());
 								 binaryInserttest(pagetarget, key, htblColNameValue,0,pagetarget.size()-1);
 								insertToAllIndexes(table, htblColNameValue, i);
 								 target.setMax(pagetarget.getmax());
 								 target.setMin(pagetarget.getmin());
 								 target.setfull(pagetarget.isFull());
 								 pagetarget.save();
 								 table.save();
 								 return;							
							}
						}
						
						
					}
					
					
				} 
				  
				 
			  }
			  
			  
			  
			  
		  }
	 }
	 
	 
	 
	 
 public void shift(String table,Vector<PageInfo> infos,int shiftfrom,String key) throws IOException, ClassNotFoundException {
	 Hashtable row = null;
	 PageInfo info=infos.get(shiftfrom);
	 Page page=getpage(info.getData());
   		 row=(Hashtable) page.get(page.size()-1);
   		page.remove(page.size()-1);
   		info.setMax(page.getmax());
   		info.setMin(page.getmin());
   		page.save();
	 
	 for(int i=shiftfrom+1;i<infos.size();i++) {
		  info=infos.get(i);
		  page=getpage(info.getData());
			 shiftIndex(table, row, i-1);
		 if(info.isFull()) {
			insertsort(row, page, key); 
       		 row=(Hashtable) page.get(page.size()-1);
       		page.remove(page.size()-1);
       		info.setMax(page.getmax());
       		info.setMin(page.getmin());
       		page.save();
       		
		 }else {
			 insertsort(row,page,key);
			 info.setMax(page.getmax());
			 info.setMin(page.getmin());
			 info.setfull(page.isFull());
			 page.save();
			 return;
		 }
		 
	 }
	 shiftIndex(table, row, infos.size()-1);
	 Page newpage=new Page<Hashtable>(key);
	 newpage.add(row);
	PageInfo newinfo=new PageInfo(newpage.getmax(), newpage.getmin(),newpage.save());
	infos.add(newinfo);
 }

	 
	 public void insertsort(Hashtable row,Page<Hashtable> page,String key){
		 for (int i = 0; i < page.size(); i++) {
			 Object  tablerow=page.get(i).get(key);
			 Object insertrow=row.get(key);
		if(compare(tablerow,insertrow)>=0) {
		     page.add(i, row);
		     return;

			
		}
		
		}
			page.add(row);


		 
	 }
	 
	 public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws Exception {
		if(checkColoumns(strTableName, htblColNameValue)==false) {
			throw new DBAppException("Data inserted did not match the type of the coloumns");
		}
		 
		 boolean equal=true;
		 Table table=(Table) getTable(strTableName);
		 Vector<PageInfo> info=table.getPages();
		 ArrayList<String> indexes=table.getIndexes();
		
		 
		 if(table.getIndexes().size()!=0) {
			 if(indexes.contains(table.getKey()) && (htblColNameValue.get(table.getKey())!=null)) {
				 Tree tree= getTree(strTableName, table.getKey());
				 Ref p=tree.search(htblColNameValue.get(table.getKey()));
	               deleteWithIndex(p, true, table,htblColNameValue,table.getKey());
	               return;
			 }
			 String indexedColoumn="";
			 
			 for (int i = 0; i < indexes.size(); i++) {
				if(htblColNameValue.get(indexes.get(i))!=null) {
					indexedColoumn=indexes.get(i);
					break;
				}
			}
			 if(indexedColoumn!="") {
			 Tree tree=getTree(strTableName,indexedColoumn);
			 Ref p=tree.search( htblColNameValue.get(indexedColoumn));
			               deleteWithIndex(p, false, table,htblColNameValue, indexedColoumn);
               return;
			 }
			 
			 
			 
		 }
		 if(htblColNameValue.get(table.getKey())!=null) {
			int index= Pagebinarysearch(info, htblColNameValue.get(table.getKey()));
			if(index==-1) {
				return;
			}
			ArrayList deleted=goLeftPage(index, info, "delete", htblColNameValue.get(table.getKey()), htblColNameValue, table);
			Hashtable del=(Hashtable) deleted.get(0);
			int deletedPagesNo=(int) del.get("deleted");
			goRightPage(index-deletedPagesNo, info, "delete", htblColNameValue.get(table.getKey()), htblColNameValue,table);
               return;
		 }
		 for (int i = 0; i < info.size(); i++) {
			PageInfo pageinfo=info.get(i);
			Page<Hashtable> page=getpage(pageinfo.getData());
			for (int j = 0; j < page.size(); j++) {
				Hashtable<String,Object> row=page.get(j);
			 for(String coloumn:htblColNameValue.keySet()) {
				  equal=true;
				 if(compare(row.get(coloumn),htblColNameValue.get(coloumn))==0){
			          
				 }else {
					 equal=false;
					 break;
				 }
				 
			 }
			 if(equal==true) {
				 page.remove(j);
				 deleteFromAllIndexes(table, row, i);
				 j=j-1;
				 if(page.isEmpty()) {
					 File delete=pageinfo.getData();
					 delete.delete();
					info.remove(i);
					table.checklast();
				 }else {
					 pageinfo.setMax(page.getmax());
					 pageinfo.setMin(page.getmin());
					 pageinfo.setfull(page.isFull());
					  page.save();
					  
				 }
			 }
			}
			
			
		}
		 table.save();
		 
	 }
	 
	// htblColNameValue enteries are ANDED together
	 public void updateTable(String strTableName,String strClusteringKey, Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException, ParseException, ClassNotFoundException{
		 if(checkColoumns(strTableName, htblColNameValue)==false) {
			 throw new DBAppException("Values inserted do not match the type of the coloumns of the table");
		 }
		 File file=new File("data//"+strTableName+".class");
		 if(!file.exists()) {
			 throw new DBAppException("Table not found");
			 
		 }
			Date date = new Date();
      		 DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            	  htblColNameValue.put("TouchDate",dateFormat.format(date));
		 Table table=getTable(strTableName);
		 String type=getType(strTableName,table.getKey());
		 Object result=null;
		 switch(type) {
		 case "java.lang.Integer":
			  result=Integer.parseInt(strClusteringKey);
			 break;
		 case "java.lang.String":
			  result=strClusteringKey;
			 break;
		 case "java.lang.Double":
			 result=Double.parseDouble(strClusteringKey);
			 break;
		 case "java.lang.Boolean":
			 result=Boolean.parseBoolean(strClusteringKey);
			 break;
		 case "java.util.Date":
			 result=new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKey);
			 break;
		 case "java.awt.Polygon":
			 ArrayList<Integer> x=new ArrayList<>();
			 ArrayList<Integer> y=new ArrayList<>();
			 int indicator=0;
			 String digit="";
			 for(int i=0;i<strClusteringKey.length();i++) {
				 if(strClusteringKey.charAt(i)>='0' && strClusteringKey.charAt(i)<='9' ) {
					 digit=digit+strClusteringKey.charAt(i);
				 }else {
					 if(!digit.isEmpty()) {
						 if(indicator==0) {
							 x.add(Integer.parseInt(digit));
							 digit="";
							 indicator=1;
						 }else {
							 y.add(Integer.parseInt(digit));
							 digit="";
							 indicator=0;
						 }
					 } 
					 
				 }
			 }
			
			 int[] lx=new int[x.size()];
			 for (int i = 0; i < x.size(); i++) {
				lx[i]=x.get(i);
			}
			 int[] ly=new int[y.size()];
			 for (int i = 0; i < y.size(); i++) {
				ly[i]=y.get(i);
			}
			 for (int i = 0; i < ly.length; i++) {
				
			}
			 for (int i = 0; i < lx.length; i++) {
				
			}
		
			result=new Polygon(lx,ly,lx.length);
			
		 }
		 
		 ArrayList<String> indexes=table.getIndexes();
		 ArrayList<String> updatedIndexes=new ArrayList<String>();
		 for (String col : htblColNameValue.keySet()) {
			if(indexes.contains(col)) {
				updatedIndexes.add(col);
			}
		}
		 if(indexed(table.getKey(),strTableName)) {
			 indexedUpdate(result, htblColNameValue, table);
			 return;
		 }
		 Vector<PageInfo> infos=table.getPages();
		 for (int i = 0; i < infos.size(); i++) {
			 
			 if(compare(infos.get(i).getMax(),result)<0 ) {
				 continue;                              //may delete later
			 }
			 if(compare(infos.get(i).getMin(),result)>0) {
				 return;
			 }
			Page page=getpage(infos.get(i).getData());
			int index=binarySearch(page, result,table.getKey());
			if(index==-1) {
				continue;
			}
			goleft(page,result,index,htblColNameValue,table.getKey(),i,updatedIndexes,table);
			goright(page,result,index,htblColNameValue,table.getKey(),i,updatedIndexes,table);
			page.save();
			
		}
		 table.save();
		 
		 
		 
		 
		 
		 
		
		 
		 
		 
	 }
	 
	 public String getType(String strTableName,String coloumn) throws IOException {
		 BufferedReader csvReader = new BufferedReader(new FileReader("data//metadata.csv"));
		 String row;
		 row=csvReader.readLine();
		 boolean foundtable=false;
		 Hashtable<String, String> types=new Hashtable<String,String>();
		 while ((row = csvReader.readLine()) != null) {
		     String[] data = row.split(",");
		     // do something with the data
		     if(data[0].equals(strTableName)&&data[1].equals(coloumn)) {
		    	 return data[2];
		    	
		    	 
		     }
		 
	 }
	     return "";

	 }
		 
		 
		 
		 
		 
		 
		 
	 
		 
		 
		 
		 
		 
		 
		 
	 
	 
	 public static Integer compare(Object a,Object b) {
		 
		 if(a instanceof java.lang.Integer) {
			 Integer v1=(Integer) a;
			 Integer v2=(Integer) b;
			 return v1.compareTo(v2);
		 }
		 if(a instanceof java.lang.Double) {
			 Double v1=(Double) a;
			 Double v2=(Double) b;
			 return v1.compareTo(v2);
		 }
		 if(a instanceof java.lang.String) {
			 String v1=(String) a;
			 String v2=(String) b;
			return v1.compareTo(v2);
		 }
		 if(a instanceof java.util.Date) {
			 Date v1=(Date) a;
			 Date v2=(Date) b;
			 return v1.compareTo(v2);
		 }
		 if(a instanceof java.awt.Polygon) {
			 Polygon v1=(Polygon) a;
			 Polygon v2=(Polygon) b;
			 poly comp=new poly(v1);
          	return comp.compareTo(v2);
			 
		 }
		 if(a instanceof Boolean)	 
		 return ((Boolean) a).compareTo((Boolean) b);
		 
		 return 0;
	 
	 }
	 
	 public Page<Hashtable> getpage(File file){
		 try {
			 
	            FileInputStream fileIn = new FileInputStream(file);
	            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
	 
	            Page<Hashtable> obj = (Page<Hashtable>)objectIn.readObject();
	 
	            objectIn.close();
	            return obj;
	 
	        } catch (Exception ex) {
	            ex.printStackTrace();
	            return null;
	        } 
		 
	 }
	 
	 public Table getTable(String name) {
		 try {
			 
	            FileInputStream fileIn = new FileInputStream("data//"+name+".class");
	            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
	 
	            Table obj = (Table)objectIn.readObject();
	 
	            objectIn.close();
	            return obj;
	 
	        } catch (Exception ex) {
	            ex.printStackTrace();
	            return null;
	        } 
		 
	 }
	 
	 
	 
	 public boolean checkColoumnsInsert(String TableName,Hashtable<String, Object> coloumns) throws IOException {
		 BufferedReader csvReader = new BufferedReader(new FileReader("data//metadata.csv"));
		 String row;
		 row=csvReader.readLine();
		 boolean foundtable=false;
		 Hashtable<String, String> types=new Hashtable<String,String>();
		 while ((row = csvReader.readLine()) != null) {
		     String[] data = row.split(",");
		     // do something with the data
		     if(data[0].equals(TableName)) {
		    	 types.put(data[1], data[2]);
		    	 foundtable=true;
		    	 continue;
		    	 
		     }
		     if(foundtable) {
		    	 break;
		     }
		 }
		 csvReader.close();
		 if(types.size()!=coloumns.size()) {

			 return false;
		 }
		 for(String coloumname:types.keySet()) {
			 switch(types.get(coloumname)) {
			 case "java.lang.Integer":
				 if(coloumns.get(coloumname) instanceof java.lang.Integer==false) {
					 return false;
				 }
				 break;
			 case "java.lang.String":
			 if(coloumns.get(coloumname) instanceof java.lang.String==false) {

				 return false;
			 }
			 break;
			 case "java.lang.Double":
				 if(coloumns.get(coloumname) instanceof java.lang.Double==false) {

					 return false;
				 }
				 break;
			 case "java.lang.Boolean":
				 if(coloumns.get(coloumname) instanceof java.lang.Boolean==false) {

					 return false;
				 }
				 break;
			 case "java.util.Date" :
				 if(coloumns.get(coloumname) instanceof java.util.Date==false) {

					 return false;
				 }
				 break;
			 case "java.awt.Polygon":
				 if(coloumns.get(coloumname) instanceof java.awt.Polygon==false) {

					 return false;
				 }
				 break;
			 }			 
		 }
	return true;	 
	 }
	 
	 
	 
	 public boolean checkColoumns(String TableName,Hashtable<String, Object> coloumns) throws IOException {
		 BufferedReader csvReader = new BufferedReader(new FileReader("data//metadata.csv"));
		 String row;
		 row=csvReader.readLine();
		 boolean foundtable=false;
		 Hashtable<String, String> types=new Hashtable<String,String>();
		 while ((row = csvReader.readLine()) != null) {
		     String[] data = row.split(",");
		     // do something with the data
		     if(data[0].equals(TableName)) {
		    	 types.put(data[1], data[2]);
		    	 foundtable=true;
		    	 continue;
		    	 
		     }
		     if(foundtable) {
		    	 break;
		     }
		 }
		 csvReader.close();
		 for(String coloumname:coloumns.keySet()) {
			 switch(types.get(coloumname)) {
			 case "java.lang.Integer":
				 if(coloumns.get(coloumname) instanceof java.lang.Integer==false) {
					 return false;
				 }
				 break;
			 case "java.lang.String":
			 if(coloumns.get(coloumname) instanceof java.lang.String==false) {

				 return false;
			 }
			 break;
			 case "java.lang.Double":
				 if(coloumns.get(coloumname) instanceof java.lang.Double==false) {

					 return false;
				 }
				 break;
			 case "java.lang.Boolean":
				 if(coloumns.get(coloumname) instanceof java.lang.Boolean==false) {

					 return false;
				 }
				 break;
			 case "java.util.Date" :
				 if(coloumns.get(coloumname) instanceof java.util.Date==false) {

					 return false;
				 }
				 break;
			 case "java.awt.Polygon":
				 if(coloumns.get(coloumname) instanceof java.awt.Polygon==false) {

					 return false;
				 }
				 break;
			 }			 
		 }
	return true;	 
	 }
	 
	 
	 
	public int binarySearch(Page<Hashtable> page, Object x,String key) 
	    { 
	        int l = 0, r = page.size() - 1; 
	        while (l <= r) { 
	            int m = l + (r - l) / 2; 
	  
	            // Check if x is present at mid 
	            if (compare(page.get(m).get(key),x)==0) 
	                return m; 
	  
	            // If x greater, ignore left half 
	            if (compare(page.get(m).get(key),x)<0) 
	                l = m + 1; 
	  
	            // If x is smaller, ignore right half 
	            else
	                r = m - 1; 
	        } 
	  
	        // if we reach here, then element was 
	        // not present 
	        return -1; 
	    } 
	public void binaryInsert(Page<Hashtable> page, String key,Hashtable row) 
    { 
		Object x=row.get(key);
        int l = 0, r = page.size() - 1; 
        while (l <= r) { 
            int m = l + (r - l) / 2; 
            if(m==0) {
            	page.add(0,row);
            	return;
            }
            if(m==page.size()-1) {
            	page.add(page.size()-1,row);
            	return;
            }
  
            // Check if x is present at mid 
            if (compare(page.get(m).get(key),x)>=0 && compare(page.get(m-1).get(key),x)<=0  ) {
            	page.add(m,row);
            return;
        }  
           
  
            // If x greater, ignore left half 
            if (compare(page.get(m).get(key),x)<0) 
                l = m + 1; 
  
            // If x is smaller, ignore right half 
            else
                r = m - 1; 
        } 
  
        // if we reach here, then element was 
        // not present 
    } 
	 void goleft(Page page,Object x,int start,Hashtable<String,Object> update,String key,int pageNo,ArrayList<String> updatedIndexes,Table table) throws ClassNotFoundException, IOException {
		 for(int i= start;i>=0;i--) {
			 Hashtable row=(Hashtable) page.get(i);

				if(compare(row.get(key),x)==0&&equals(row.get(key),x)) {
					DeleteFromIndexes(updatedIndexes, table, row, pageNo);

					for(String col:update.keySet()) {
						row.put(col,update.get(col));
					}
					InsertToIndexes(updatedIndexes, table, row, pageNo);
				}else {
					if(compare(row.get(key),x)!=0&&!equals(row.get(key),x))
					break;
				}
				
		 }
		
		 		 
	 }
	 
	 void goright(Page page,Object x,int start,Hashtable<String,Object> update,String key,int pageNo,ArrayList<String> updatedIndexes,Table table) throws ClassNotFoundException, IOException {
		 for(int i= start+1;i<page.size();i++) {
			 Hashtable row=(Hashtable) page.get(i);

				if(compare(row.get(key),x)==0 && equals(row.get(key),x)) {
					DeleteFromIndexes(updatedIndexes, table, row, pageNo);

					for(String col:update.keySet()) {
						row.put(col,update.get(col));
					}
					InsertToIndexes(updatedIndexes, table, row, pageNo);
				}else {
					if(compare(row.get(key),x)!=0&&!equals(row.get(key),x))
					break;
				}
				
		 }
		
		 		 
	 }
	 
	 
	

	 
	 public Iterator selectNoIndex(SQLTerm[] arrSQLTerms,String[] strarrOperators) throws Exception {
		 if(arrSQLTerms.length==0) {
			 return null;
		 }
		 Hashtable<String,Object> test=new Hashtable<String,Object>();
		 String tableName=arrSQLTerms[0]._strTableName;
			File file=new File("data//"+tableName+".class");
			  if(!file.exists()) 
				  throw new DBAppException("Table not found");	
			  
			 if(strarrOperators.length+1!=arrSQLTerms.length) {
				 throw new DBAppException("Invalid input");
			 }
			  
		Table table=getTable(tableName);
			ArrayList result=new ArrayList<>();
				 
				
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if(!tableName.equals(arrSQLTerms[i]._strTableName)) {
				throw new DBAppException("Invalid table name!");
			}
			test.put(arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue );
      		if(!checkColoumns(tableName, test)) {
      			throw new DBAppException("Invalid Input");
      		}
		}
  		Vector<PageInfo> info=table.getPages();

      	 test.clear();
      	if(strarrOperators.length==0) {
      		if(arrSQLTerms[0]._strColumnName.equals(table.getKey())) {
      			switch(arrSQLTerms[0]._strOperator) {
      			case "=":
      			int index=Pagebinarysearch(table.getPages(), arrSQLTerms[0]._objValue);
                      result=goLeftPage(index,table.getPages(),"select",arrSQLTerms[0]._objValue,null,table);
      			ArrayList right=goRightPage(index,table.getPages(),"select",arrSQLTerms[0]._objValue,null,table);
      			result.addAll(right);
      			break;
      			
      			case ">=":
      				for (int i = info.size(); i >=0; i--) {
						if(compare(info.get(i).getMax(),arrSQLTerms[0]._objValue)<0) {
							break;
						}
						Page page=getpage(info.get(i).getData());
						for (int j = page.size(); j >=0; j--) {
							Hashtable row=(Hashtable)page.get(j);
							if(compare(row.get(table.getKey()),arrSQLTerms[0]._objValue)<0){
								i=-1;
								break;
							}
							result.add(row);
						}
					}
      				break;

      			case ">":
      				 result=new ArrayList<>();
      				for (int i = info.size(); i >=0; i--) {
						if(compare(info.get(i).getMax(),arrSQLTerms[0]._objValue)<=0) {
							break;
						}
						Page page=getpage(info.get(i).getData());
						for (int j = page.size(); j >=0; j--) {
							Hashtable row=(Hashtable)page.get(j);
							if(compare(row.get(table.getKey()),arrSQLTerms[0]._objValue)<=0){
								i=-1;
								break;
							}
							result.add(row);
						}
					}
      				
      		
      		
      	            break;
      			case "<=":
      				 result=new ArrayList<>();
      				for (int i = 0; i<info.size(); i++) {
						if(compare(info.get(i).getMax(),arrSQLTerms[0]._objValue)>0) {
							break;
						}
						Page page=getpage(info.get(i).getData());
						for (int j = 0; j <info.size(); j++) {
							Hashtable row=(Hashtable)page.get(j);
							if(compare(row.get(table.getKey()),arrSQLTerms[0]._objValue)>0){
								i=info.size();
								break;
							}
							result.add(row);
						}
					}
      				break;
      			case "<":
      				 result=new ArrayList<>();
       				for (int i = 0; i<info.size(); i++) {
 						if(compare(info.get(i).getMax(),arrSQLTerms[0]._objValue)>=0) {
 							break;
 						}
 						Page page=getpage(info.get(i).getData());
 						for (int j = 0; j <info.size(); j++) {
 							Hashtable row=(Hashtable)page.get(j);
 							if(compare(row.get(table.getKey()),arrSQLTerms[0]._objValue)>=0){
 								i=info.size();
 								break;
 							}
 							result.add(row);
 						}
 					}
      			   break;
      			case "!=":
      				 result=new ArrayList<>();
       				for (int i = 0; i<info.size(); i++) {
 						
 						Page page=getpage(info.get(i).getData());
 						for (int j = 0; j <info.size(); j++) {
 							Hashtable row=(Hashtable)page.get(j);
 							if(compare(row.get(table.getKey()),arrSQLTerms[0]._objValue)==0){
 								continue;
 							}
 							result.add(row);
 						}
 					}
       				default:throw new DBAppException("Invalid operator");
      			}
      		}else {
      			for (int i = 0; i < info.size(); i++) {
      				Page page=getpage(info.get(i).getData());
      				
					for (int j = 0; j < page.size(); j++) {
						Hashtable row=(Hashtable) page.get(j);
						switch(arrSQLTerms[0]._strOperator) {
						case "=":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))==0) {
								result.add(row);
							}
							break;
						case "!=":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))!=0) {
								result.add(row);
							}
						    break;
						case ">=":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))<=0) {
								result.add(row);
							}
							break;
						case">":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))<0) {
								result.add(row);
							}
							break;
						case"<=":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))>=0) {
								result.add(row);
							}
							break;
						
						case "<":
							if(compare(arrSQLTerms[0]._objValue,row.get(arrSQLTerms[0]._strColumnName))>0) {
								result.add(row);
							}
							break;
						default: throw new DBAppException("Invalid operator");
						}
					}
				}
      			
      			
      		}
      	}else {
      	          switch(strarrOperators[0]) {
      	          case "OR":
      	        	  result=OR(arrSQLTerms, table);
      	        	  break;
      	          case "AND":
      	        	  if(arrSQLTerms[0]._strColumnName.equals(table.getKey())) {
      	        		  result=binarySearch(table.getKey(), arrSQLTerms[0]._objValue, table.getPages(), arrSQLTerms[0]._strOperator,table);
      	        		  And(result, arrSQLTerms[1]._strColumnName, arrSQLTerms[1]._strOperator, arrSQLTerms[1]._objValue);
      	        	  }else {
      	        		  if(arrSQLTerms[1]._strColumnName.equals(table.getKey())) {
          	        		  result=binarySearch(table.getKey(), arrSQLTerms[1]._objValue, table.getPages(), arrSQLTerms[1]._strOperator,table);
          	        		  And(result, arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._strOperator, arrSQLTerms[0]._objValue);      	        	
          	        		  }else {
          	        			  result=linearSearch(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue, table.getPages(), arrSQLTerms[0]._strOperator);
          	        			  And(result, arrSQLTerms[1]._strColumnName, arrSQLTerms[1]._strOperator, arrSQLTerms[1]._objValue);;
          	        		  }
      	        	  
      	        	  }
      	        	  break;
      	          case "XOR":
      	        	 result= XOR(arrSQLTerms, table);
      	        	  break;
      	        	 default:throw new DBAppException("Invalid operator"); 
      	        	  
      	        	  
      	          }
      	          
      	          
      	          for (int i = 1; i < strarrOperators.length; i++) {
					switch(strarrOperators[i]) {
					case "AND":
						And(result,arrSQLTerms[i+1]._strColumnName,arrSQLTerms[i+1]._strOperator,arrSQLTerms[i+1]._objValue);
						break;
					
					case "OR":
						if(arrSQLTerms[i+1]._strColumnName.equals(table.getKey())) {
						ArrayList temp=binarySearch(table.getKey(),arrSQLTerms[i+1]._objValue,table.getPages(),arrSQLTerms[i+1]._strOperator,table);
						mergeNoduplicates(result, temp,table.getKey());
						
						}else {
							ArrayList temp=linearSearch(arrSQLTerms[i+1]._strColumnName, arrSQLTerms[i+1]._objValue, table.getPages(), arrSQLTerms[i+1]._strOperator);
						mergeNoduplicates(result, temp,table.getKey());
						}
						break;
					case "XOR":
						if(arrSQLTerms[i+1]._strColumnName.equals(table.getKey())) {
							ArrayList temp=binarySearch(table.getKey(),arrSQLTerms[i+1]._objValue,table.getPages(),arrSQLTerms[i+1]._strOperator,table);
							ArrayList temp1=new ArrayList<>();
			                temp1.addAll(result);
			                result.removeAll(temp);
							temp.removeAll(temp1);
							result.addAll(temp);
							
							}else {
								ArrayList temp=linearSearch(arrSQLTerms[i+1]._strColumnName, arrSQLTerms[i+1]._objValue, table.getPages(), arrSQLTerms[i+1]._strOperator);
								ArrayList temp1=new ArrayList<>();
				                temp1.addAll(result);
								result.removeAll(temp);
								temp.removeAll(temp1);
								result.addAll(temp);
				}
						break;
					 default:
						throw new DBAppException("iNVALID OPREATOR");
      	          
      		
      	}
      		
      	          
			
		}
      	}
		return result.iterator();

	 }
		 
	 
    public ArrayList<Hashtable> binarySearch(String key,Object target,Vector<PageInfo> info,String operator,Table table) throws Exception{
		ArrayList result=new ArrayList<>();
    	switch(operator) {
			case "=":
			int index=Pagebinarysearch(info, target);
			if(index==-1) {
				return result;
			}
			 result=goLeftPage(index,info,"select",target,null,table);
			ArrayList right=goRightPage(index,info,"select",target,null,table);
			result.addAll(right);
			break;
			
			case ">=":
				for (int i = info.size()-1; i >=0; i--) {
				if(compare(info.get(i).getMax(),target)<0) {
					break;
				}
				Page page=getpage(info.get(i).getData());
				for (int j = page.size()-1; j >=0; j--) {
					Hashtable row=(Hashtable)page.get(j);
					if(compare(row.get(key),target)<0){
						i=-1;
						break;
					}
					result.add(0, row);;
				}
			}
				break;

			case ">":
				 result=new ArrayList<>();
				for (int i = info.size()-1; i >=0; i--) {
				if(compare(info.get(i).getMax(),target)<=0) {
					break;
				}
				Page page=getpage(info.get(i).getData());
				for (int j = page.size()-1; j >=0; j--) {
					Hashtable row=(Hashtable)page.get(j);
					if(compare(row.get(key),target)<=0){
						i=-1;
						break;
					}
					result.add(0, row);
				}
			}
				
		
		
	            break;
			case "<=":
				 result=new ArrayList<>();
				for (int i = 0; i<info.size(); i++) {
				if(compare(info.get(i).getMax(),target)>0) {
					break;
				}
				Page page=getpage(info.get(i).getData());
				for (int j = 0; j <page.size(); j++) {
					Hashtable row=(Hashtable)page.get(j);
					if(compare(row.get(key),target)>0){
						i=info.size();
						break;
					}
					result.add(row);
				}
			}
				break;
			case "<":
				 result=new ArrayList<>();
				for (int i = 0; i<info.size(); i++) {
					if(compare(info.get(i).getMax(),target)>=0) {
						break;
					}
					Page page=getpage(info.get(i).getData());
					for (int j = 0; j <page.size(); j++) {
						Hashtable row=(Hashtable)page.get(j);
						if(compare(row.get(key),target)>=0){
							i=info.size();
							break;
						}
						result.add(row);
					}
				}
			   break;
			case "!=":
				 result=new ArrayList<>();
				for (int i = 0; i<info.size(); i++) {
					
					Page page=getpage(info.get(i).getData());
					for (int j = 0; j <page.size(); j++) {
						Hashtable row=(Hashtable)page.get(j);
						if(compare(row.get(key),target)==0){
							continue;
						}
						result.add(row);
					}
				}
				break;
					default:throw new DBAppException("Invalid operator");
			}
    	return result;
    }
    
    
    public ArrayList<Hashtable> linearSearch(String key,Object target,Vector<PageInfo> info,String operator) throws DBAppException{
		ArrayList result=new ArrayList<>();

    	for (int i = 0; i < info.size(); i++) {
				Page page=getpage(info.get(i).getData());
				
			for (int j = 0; j < page.size(); j++) {
				Hashtable row=(Hashtable) page.get(j);
				switch(operator) {
				case "=":
					if(compare(target,row.get(key))==0 &&equals(target,row.get(key))) {
						result.add(row);
					}
					break;
				case "!=":
					if(compare(target,row.get(key))!=0) {
						result.add(row);
					}
				    break;
				case ">=":
					if(compare(target,row.get(key))<=0) {
						result.add(row);
					}
					break;
				case">":
					if(compare(target,row.get(key))<0) {
						result.add(row);
					}
					break;
				case"<=":
					if(compare(target,row.get(key))>=0) {
						result.add(row);
					}
					break;
				
				case "<":
					if(compare(target,row.get(key))>0) {
						result.add(row);
					}
					break;
				default: throw new DBAppException("Invalid operator");
				}
			}
		} 
    	
    	return result;
    }
    
    
    public void And(ArrayList<Hashtable> list,String key,String operator,Object target) throws DBAppException{
    	for (int i = 0; i < list.size(); i++) {
			Hashtable row=list.get(i);
			if(list.contains(row)) {
				continue;
			}
			switch(operator) {
			case "=":
				if(compare(target,row.get(key))!=0) {
					list.remove(i);
				}
				break;
			case "!=":
				if(compare(target,row.get(key))==0) {
					list.remove(i);
				}
			    break;
			case ">=":
				if(compare(target,row.get(key))>0) {
					list.remove(i);
				}
				break;
			case">":
				if(compare(target,row.get(key))>=0) {
					list.remove(i);
				}
				break;
			case"<=":
				if(compare(target,row.get(key))<0) {
					list.remove(i);
				}
				break;
			
			case "<":
				if(compare(target,row.get(key))<=0) {
					list.remove(i);
				}
				break;
			default: throw new DBAppException("Invalid operator");
			}
		}
    }
	 public ArrayList<Hashtable> OR(SQLTerm[] term,Table table) throws Exception{
		 ArrayList result=new ArrayList<>();
		 boolean insertedb4=false;
		 Vector<PageInfo> info=table.getPages();
		   if(term[0]._strColumnName.equals(table.getKey())&& term[1]._strColumnName.equals(table.getKey())) {
			  result=binarySearch(table.getKey(), term[0]._objValue, info, term[0]._strOperator,table); 
		   }
		  for (int i = 0; i < info.size(); i++) {
			Page page=getpage(info.get(i).getData());
			for (int j = 0; j < page.size(); j++) {
				Hashtable row=(Hashtable)page.get(j);
				if(result.contains(row)) {
					continue;
				}
				switch(term[0]._strOperator) {
				case "=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))==0) {
						result.add(row);
					}
					break;
				case "!=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))!=0) {
						result.add(row);
					}
				    break;
				case ">=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))<=0) {
						result.add(row);
					}
					break;
				case">":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))<0) {
						result.add(row);
					}
					break;
				case"<=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))>=0) {
						result.add(row);
					}
					break;
				
				case "<":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))>0) {
						result.add(row);
					}
					break;
				default: throw new DBAppException("Invalid operator");
				}
				switch(term[1]._strOperator) {
				case "=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))==0) {
						if(!result.contains(row)) {
						result.add(row);
					}
					}
					break;
				case "!=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))!=0) {
						if(!result.contains(row)) {

						result.add(row);
						}					}
				    break;
				case ">=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))<=0) {
						if(!result.contains(row)) {

						result.add(row);
						}					}
					break;
				case">":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))<0) {
						if(!result.contains(row)) {

						result.add(row);
						}					}
					break;
				case"<=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))>=0) {
						if(!result.contains(row)) {

						result.add(row);
						}					}
					break;
				
				case "<":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))>0) {
						if(!result.contains(row)) {

						result.add(row);
						}					}
					break;
				default: throw new DBAppException("Invalid operator");
				}
				
				}
			}
			return result;
		}
	 
	 
	 public ArrayList<Hashtable> XOR(SQLTerm[] term,Table table) throws DBAppException{
		 ArrayList result=new ArrayList<>();
		 Vector<PageInfo> info=table.getPages();
		  for (int i = 0; i < info.size(); i++) {
			Page page=getpage(info.get(i).getData());
			for (int j = 0; j < page.size(); j++) {
				Hashtable row=(Hashtable)page.get(j);
				if(result.contains(row)) {
					continue;
				}
				switch(term[0]._strOperator) {
				case "=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))==0) {
						result.add(row);
					}
					break;
				case "!=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))!=0) {
						result.add(row);
					}
				    break;
				case ">=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))<=0) {
						result.add(row);
					}
					break;
				case">":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))<0) {
						result.add(row);
					}
					break;
				case"<=":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))>=0) {
						result.add(row);
					}
					break;
				
				case "<":
					if(compare(term[0]._objValue,row.get(term[0]._strColumnName))>0) {
						result.add(row);
					}
					break;
				default: throw new DBAppException("Invalid operator");
				}
				switch(term[1]._strOperator) {
				case "=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))==0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}
					}
					break;
				case "!=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))!=0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}
					}
				    break;
				case ">=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))<=0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}					}
					break;
				case">":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))<0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}					}
					break;
				case"<=":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))>=0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}					}
					break;
				
				case "<":
					if(compare(term[1]._objValue,row.get(term[1]._strColumnName))>0) {
						if(result.contains(row)){
							result.remove(row);

						}else {
							result.add(row);

						}					}
					break;
				default: throw new DBAppException("Invalid operator");
				}
				
				}
			}
			return result;
		}
		 
	 
	public int Pagebinarysearch(Vector<PageInfo> pages,Object value) {
			int l = 0, r = pages.size() - 1; 
	        while (l <= r) { 
	            int m = l + (r - l) / 2; 
	  
	            // Check if x is present at mid 
	            if (compare(pages.get(m).getMax(),value)>=0 && compare(pages.get(m).getMin(),value)<=0) 
	                return m; 
	  
	            // If x greater, ignore left half 
	            if (compare(pages.get(m).getMax(),value)<0) {
	                l = m + 1; 
	            }
	  
	            // If x is smaller, ignore right half 
	            else {
	                r = m - 1; 
	            }
	        } 
	  
	        // if we reach here, then element was 
	        // not present 
	        return -1; 
			
		
	
	 
	
	}
	
	
	public ArrayList<Hashtable<String, Object>> goLeftPage(int index,Vector<PageInfo> pages,String method,Object value,Hashtable<String,Object> update,Table table) throws Exception {
         int deleted=0;
		String key=table.getKey();
		ArrayList result=new ArrayList<Hashtable<String,Object>>();
		for (int i = index-1; i >= 0; i--) {
			if(!(compare(pages.get(i).getMax(),value)>=0 && compare(pages.get(i).getMin(),value)<=0)) {
				break;
			}
			int pageIndex=binarySearch(getpage(pages.get(i).getData()),value, key);
            if(pageIndex==-1) {
            	continue;
            }
		
		switch(method) {

		
		case "select":
			 ArrayList left=goleftSelect(getpage(pages.get(i).getData()),pageIndex,value,key);
			ArrayList right=gorightSelect(getpage(pages.get(i).getData()),pageIndex,value,key);
			left.addAll(right);
			result.addAll(left);
			break;
		case "delete":
			Page page=getpage(pages.get(i).getData());
			 int deletedRows= goLeftdelete(page, pageIndex, update, key, value,i,table);
			goRightdelete(page, pageIndex-deletedRows, update, key, value,i,table);
			 if(page.isEmpty()) {
				 File delete=pages.get(i).getData();
				 delete.delete();
				pages.remove(i);
				deleted=deleted+1;
				shiftdownIndex(table.getName(), i);
				
			 }else {
				 pages.get(i).setMax(page.getmax());
				 pages.get(i).setMin(page.getmin());
				 pages.get(i).setfull(page.isFull());
				  page.save();
				  
			 }
			 table.save();
		}
	}
		if(method.equals("delete")) {
			Hashtable del=new Hashtable();
			del.put("deleted", deleted);
			result.add(del);
		}
	
	return result;
	}
	
	
	
	
	public ArrayList<Hashtable<String, Object>> goRightPage(int index,Vector<PageInfo> pages,String method,Object value,Hashtable<String,Object> update,Table table) throws Exception {
String key=table.getKey();
		ArrayList result=new ArrayList<Hashtable<String,Object>>();
		for (int i = index; i <pages.size(); i++) {
			if(!(compare(pages.get(i).getMax(),value)>=0 && compare(pages.get(i).getMin(),value)<=0)) {
				break;
			}
			int pageIndex=binarySearch(getpage(pages.get(i).getData()),value, key);

		
		switch(method) {

		
		case "select":
			 ArrayList left=goleftSelect(getpage(pages.get(i).getData()),pageIndex,value,key);
			ArrayList right=gorightSelect(getpage(pages.get(i).getData()),pageIndex,value,key);
			left.addAll(right);
			result.addAll(left);
			break;
		case "delete":
			Page page=getpage(pages.get(i).getData());
			int deleted=goLeftdelete(page, pageIndex, update, key, value,i,table);
			goRightdelete(page, pageIndex-deleted, update, key, value,i,table);
			 if(page.isEmpty()) {
				 shiftdownIndex(table.getName(), i);
				 File delete=pages.get(i).getData();
				 delete.delete();
				pages.remove(i);
				i=i-1;
			 }else {
				 pages.get(i).setMax(page.getmax());
				 pages.get(i).setMin(page.getmin());
				 pages.get(i).setfull(page.isFull());
				  page.save();
				  
			 }	
		table.save();	 
		}
	}
		
	
	return result;
	}
	
	
	public ArrayList<Hashtable<String,Object>> goleftSelect(Page page,int index,Object value,String key){
		ArrayList result=new ArrayList<>();
		for (int i = index-1; i >=0; i--) {
			 Hashtable row=(Hashtable) page.get(i);

         		if(compare(value,row.get(key))==0 &&equals(value,row.get(key))) {
             		result.add(page.get(i));

         		}
         		if(compare(value,row.get(key))!=0) {
         			break;
         		}
         		
		}
		return result;

	}
	
	public ArrayList<Hashtable<String,Object>> gorightSelect(Page page,int index,Object value,String key){
		ArrayList result=new ArrayList<>();
	
		for (int i = index; i <page.size(); i++) {
			 Hashtable row=(Hashtable) page.get(i);

			 if(compare(value,row.get(key))==0 && equals(value,row.get(key))) {
          		result.add(page.get(i));

      		}
      		if(compare(value,row.get(key))!=0) {
      			break;
      		}
         		
		}
		return result;

	}
	
	public void mergeNoduplicates(ArrayList<Hashtable> result,ArrayList<Hashtable> list,String key) {
		 ArrayList<Hashtable> listTwoCopy = new ArrayList<Hashtable>(list);
	        listTwoCopy.removeAll(result);
	        for (int i = 0; i < listTwoCopy.size(); i++) {
				insertSortSelect(result, key, listTwoCopy.get(i), 0, result.size()-1);
			}
	}
	
	
	public int goLeftdelete(Page page,int index,Hashtable<String,Object> coloumns,String key,Object value,int pageNo,Table table) throws IOException, Exception {
		boolean equal=true; 
		int deleted=0;
		for(int i= index-1;i>=0;i--) {
			 Hashtable row=(Hashtable) page.get(i);

				if(compare(row.get(key),value)!=0 && !equals(row.get(key),value)) {
					return deleted;
				}
			
				 for(String coloumn:coloumns.keySet()) {
					  equal=true;
					 if(compare(row.get(coloumn),coloumns.get(coloumn))==0&&equals(row.get(coloumn),coloumns.get(coloumn))){
				          
					 }else {
						 equal=false;
						 break;
					 }
					 
				 }
				 if(equal==true) {
					
					deleted=deleted+1; 
					 page.remove(i);
				     deleteFromAllIndexes(table, row, pageNo);
				     if(page.isEmpty()) {
				    	 return deleted;
				     }
				     
				 }
		}
		return deleted;
				
	}
	public void goRightdelete(Page page,int index,Hashtable<String,Object> coloumns,String key,Object value,int pageNo,Table table) throws IOException, Exception {
		boolean equal=true; 
	
		
		for(int i= index;i<page.size();i++) {
			 Hashtable row=(Hashtable) page.get(i);

				if(compare(row.get(key),value)!=0 && !equals(row.get(key),value)) {
					return;
				}
				 for(String coloumn:coloumns.keySet()) {
					  equal=true;
					 if(compare(row.get(coloumn),coloumns.get(coloumn))==0&&equals(row.get(coloumn),coloumns.get(coloumn))){
				          
					 }else {
						 equal=false;
						 break;
					 }
					 
				 }
				 if(equal==true) {
					 page.remove(i);
					 i=i-1;
				     deleteFromAllIndexes(table, row, pageNo);
				 }
		}
				
	}
	
	
	public ArrayList<Hashtable> Linearsearch(SQLTerm [] term,String [] operators){
		Table table=getTable(term[0]._strTableName);
		Vector<PageInfo> infos=table.getPages();
		ArrayList result=new ArrayList<>();
		boolean Indicator=false; 
		for (int i = 0; i < infos.size(); i++) {
			Page page=getpage(infos.get(i).getData());
			for (int j = 0; j < page.size(); j++) {
			 Hashtable row=(Hashtable) page.get(j);
			 for (int k = 0; k < term.length; k++) {
				Indicator=false;
				Object target=term[k]._objValue;
				String key=term[k]._strColumnName;
				switch(term[k]._strOperator) {
				case "=":
					if(compare(target,row.get(key))==0 &&equals(target,row.get(key))) {
                         Indicator=true;
					}
					break;
				case "!=":
					if(compare(target,row.get(key))!=0 &&!equals(target,row.get(key))) {
                        Indicator=true;
					}
				    break;
				case ">=":
					if(compare(target,row.get(key))<=0) {
                        Indicator=true;
					}
					break;
				case">":
					if(compare(target,row.get(key))<0) {
                        Indicator=true;
					}
					break;
				case"<=":
					if(compare(target,row.get(key))>=0) {
                        Indicator=true;
					}
					break;
				
				case "<":
					if(compare(target,row.get(key))>0) {
                        Indicator=true;
					}
				
			}
				if(Indicator && k==0) {
					result.add(row);
					continue;
				}
				
				if(k==0) {
					continue;
				}
				switch(operators[k-1]) {
				case "OR":
					if(Indicator) {
					if(!result.contains(row)) {
						result.add(row);
					}
					}
				break;
				case "AND":
					if(Indicator) {
						if(!result.contains(row)) {
						}
						}else {
						result.remove(row);	
					}
				break;
				case "XOR":
					if(Indicator) {
						if(result.contains(row)) {
							result.remove(row);
						}else {
							result.add(row);
						}
					}
					
				}
					
					
				
			}
			
		}
	}
	return result;
	}
	






public boolean Binary(SQLTerm[] terms,String[] operators) throws DBAppException, IOException {
	boolean result=false;
	Table table=getTable(terms[0]._strTableName);
	if(operators.length==0) {
		if(terms[0]._strColumnName.equals(table.getKey())||indexed(terms[0]._strColumnName,table.getName())) {
			return true;
		}else {
			return false;
		}
	}
	for (int i = 0; i < operators.length; i++) {
		if(i==0) {
			switch(operators[i]){
			case "OR":
				if(!IndexKey(table, terms[0]._strColumnName)||!IndexKey(table, terms[1]._strColumnName)) {
				   result=false;
				}else {
					result=true;
				}
				   break;

			case "AND":
				if(IndexKey(table, terms[0]._strColumnName)|| IndexKey(table, terms[1]._strColumnName)) {
					   result=true;
					}
				break;
			case "XOR":
				if(!IndexKey(table, terms[0]._strColumnName)|| !IndexKey(table, terms[0]._strColumnName)) {
					   result=false;
					   
					}else {
						result=true;
					}
				break;
				default :throw new DBAppException("Invalid operator!");
			}
		}else {
			switch(operators[i]){
			case "OR":
				if(!IndexKey(table, terms[i+1]._strColumnName)) {
				   result=false;
				}
				   break;

			case "AND":
				if(IndexKey(table, terms[i+1]._strColumnName)) {
					   result=true;
					}
				break;
			case "XOR":
				if(!IndexKey(table, terms[i+1]._strColumnName)) {
					   result=false;
					   
					};
					break;
				default :throw new DBAppException("Invalid operator!");
			}
			
		}
	}
	return result;
}

public ArrayList<Hashtable> XOR1(ArrayList list1,ArrayList<Hashtable> list2,String key){
	ArrayList result=new ArrayList<>();
	for (int i = 0; i < list2.size(); i++) {
	if(list1.contains(list2.get(i))) {
      list1.remove(list2.get(i))	;
      
	}else {
		insertSortSelect(list1, key, list2.get(i), 0, list1.size()-1);
	}
	}
	return list1;
}

public ArrayList<Hashtable> BinaryAnd(BinaryInfo info,SQLTerm[] term,String[] operators) throws Exception{
	ArrayList result=new ArrayList<>();
	Table table=getTable(term[0]._strTableName);
	Vector<PageInfo> infos=table.getPages();
	boolean Indicator=false;
	if(indexed(term[info.index]._strColumnName,table.getName())) {
		 result=selectIndex(term[info.index], term[info.index]._strColumnName);

	}else {
		 result=binarySearch(table.getKey(), term[info.index]._objValue, infos, term[info.index]._strOperator,table);

	}
	
	if(info.end==0) {
		return result;
	}
	ArrayList finalResult=new ArrayList<>();
	for (int i = 0; i < result.size(); i++) {
		Hashtable row=(Hashtable) result.get(i);
		int o=0;
		 for (int k = 0; k <= info.end; k++) {
			    
				Indicator=false;
				Object target=term[k]._objValue;
				String key=term[k]._strColumnName;
				switch(term[k]._strOperator) {
				case "=":
					if(compare(target,row.get(key))==0) {
                      Indicator=true;
					}
					break;
				case "!=":
					if(compare(target,row.get(key))!=0) {
                     Indicator=true;
					}
				    break;
				case ">=":
					if(compare(target,row.get(key))<=0) {
                     Indicator=true;
					}
					break;
				case">":
					if(compare(target,row.get(key))<0) {
                     Indicator=true;
					}
					break;
				case"<=":
					if(compare(target,row.get(key))>=0) {
                     Indicator=true;
					}
					break;
				
				case "<":
					if(compare(target,row.get(key))>0) {
                     Indicator=true;
					}
				
			}
				
				
			if(k==0 && Indicator) {
				insertSortSelect(finalResult, table.getKey(), row, 0, finalResult.size()-1);
				continue;
			}
			if(k==0) {
				continue;
			}
			    
				switch(operators[k-1]) {
				
			    case "OR":
			    	if(Indicator) {
					if(!finalResult.contains(row)) {
						insertSortSelect(finalResult, table.getKey(), row, 0, finalResult.size()-1);
					}
			    	}
				break;
				case "AND":
					if(Indicator) {
						
						if(!finalResult.contains(row)) {
						}
						}else {
				
						finalResult.remove(i);	
					}
				break;
				case "XOR":
					
					
					
					if(Indicator) {
						if(finalResult.contains(row)) {
							finalResult.remove(row);
						}else {
							insertSortSelect(finalResult, table.getKey(), row, 0, finalResult.size()-1);
							
						}
					}
					
				}
					
					
				
			}
			
		}
	return finalResult;
	}



public BinaryInfo findBinary(SQLTerm[] terms,String[] operators) throws IOException, DBAppException {
	BinaryInfo binary=null;
	Table table=getTable(terms[0]._strTableName);
	String key =table.getKey();
	boolean andChain=false;
	boolean indexed=false;
	if(operators.length==0) {
	BinaryInfo in=new BinaryInfo();
	in.end=0;
	in.index=0;
	return in;
	}
	
	if(operators[0].equals("AND")) {
		if(indexed(terms[0]._strColumnName,terms[0]._strTableName)){
			
			binary=new BinaryInfo();
			binary.index=0;
			binary.end=1;
			andChain=true;
			indexed=true;
			
		
		}
		
		if(terms[0]._strColumnName.equals(key)){
			if(binary==null) {
				binary=new BinaryInfo();
				binary.index=0;
				binary.end=1;
				andChain=true;
			}else {
				if(!indexed) {
					binary=new BinaryInfo();
					binary.index=0;
					binary.end=1;
					andChain=true;
				}
			}
		
			
		}
		
		if(indexed(terms[1]._strColumnName,terms[1]._strTableName)){
			if(binary==null)
			binary=new BinaryInfo();
			
			binary.index=1;
			binary.end=1;
			andChain=true;
			indexed=true;
			
		
		}
		if(terms[1]._strColumnName.equals(key)){
			if(!indexed) {
			binary=new BinaryInfo();
			binary.index=1;
			binary.end=1;
			andChain=true;
			}
		}
	}
	for (int i =1; i < operators.length; i++) {
		switch (operators[i]) {
		case "AND":
			if(indexed(terms[i+1]._strColumnName,terms[i+1]._strTableName)) {
				if(binary==null || !indexed) {
				binary=new BinaryInfo();
				binary.index=i+1;
				}
				binary.end=i+1;
				andChain=true;
				indexed=true;
				continue;
			}
			if(terms[i+1]._strColumnName.equals(key)) {
				if(indexed) {
					binary.end=i+1;
					continue;
				}else {
					binary=new BinaryInfo();
					binary.index=i+1;
					binary.end=i+1;
					andChain=true;
					continue;
				
			}
			}
				if(andChain) {
					if(binary!=null) {
                   binary.end=i+1;
					}
                   continue;
				}
				
			
			break;
		

		case "OR":
			andChain=false;
			indexed=false;
			break;
		case "XOR":
			andChain=false;
			indexed=false;
			break;
			default :throw new DBAppException("Invalid operation");
		}
		
	}
	return binary;
}

public boolean indexed(String coloumn,String table) throws IOException {
	Table t=getTable(table);
	if(t.getIndexes().contains(coloumn)) {
		return true;
	}
	return false;
}


public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators) throws Exception {

	 if(arrSQLTerms.length==0) {
		 return null;
	 }
	 Hashtable<String,Object> test=new Hashtable<String,Object>();
	 String tableName=arrSQLTerms[0]._strTableName;
		File file=new File("data//"+tableName+".class");
		  if(!file.exists()) 
			  throw new DBAppException("Table not found");	
		  
		 if(strarrOperators.length+1!=arrSQLTerms.length) {
			 throw new DBAppException("Invalid input");
		 }
		  
	Table table=getTable(tableName);
		ArrayList result=new ArrayList<>();
			
			
	for (int i = 0; i < arrSQLTerms.length; i++) {
		if(!tableName.equals(arrSQLTerms[i]._strTableName)) {
			throw new DBAppException("Invalid table name!");
		}
		test.put(arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue );
  		if(!checkColoumns(tableName, test)) {
  			throw new DBAppException("Invalid Input");
  		}
	}
	 if(arrSQLTerms.length==1) {
		 if(indexed(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._strTableName)) {
			return selectIndex(arrSQLTerms[0], arrSQLTerms[0]._strTableName).iterator();
			 
		 }else {if(arrSQLTerms[0]._strColumnName.equals(table.getKey())) {
			 return binarySearch(table.getKey(), arrSQLTerms[0]._objValue, table.getPages(), arrSQLTerms[0]._strOperator, table).iterator();
		 }else {
			 return Linearsearch(arrSQLTerms, strarrOperators).iterator();
		 }
			 
		 }
	 }
		Vector<PageInfo> info=table.getPages();

  	 test.clear();
  	 
  	 if(Binary(arrSQLTerms,strarrOperators)){
  		 BinaryInfo binary=findBinary(arrSQLTerms, strarrOperators);
  		   result=BinaryAnd(binary, arrSQLTerms, strarrOperators) ;
  		   for (int i = binary.end+1; i < arrSQLTerms.length; i++) {
  			   switch(strarrOperators[i-1]) {
  			   case "AND":
  				   And(result, tableName, arrSQLTerms[i]._strOperator, arrSQLTerms[i]._objValue);
  			   break;
  			   case "OR":
  				   if(indexed(arrSQLTerms[i]._strColumnName, tableName)) {
  					 mergeNoduplicates(result,selectIndex(arrSQLTerms[i], arrSQLTerms[i]._strColumnName),table.getKey());
  				   }else {
  					  mergeNoduplicates(result,binarySearch(tableName, arrSQLTerms[i]._objValue, info, arrSQLTerms[i]._strOperator,table),table.getKey());
  					   
  				   }
  				   break;
  			   case "XOR":
  				 if(indexed(arrSQLTerms[i]._strColumnName, tableName)) {
					   result=XOR1(result,selectIndex(arrSQLTerms[i], arrSQLTerms[i]._strColumnName),table.getKey());

				   }else {
					  result=XOR1(result, binarySearch(tableName, arrSQLTerms[i]._objValue, info, arrSQLTerms[i]._strOperator,table),table.getKey()); 			   }
		}
  	 }
  	 
  	 
  	 


}else {
	Linearsearch(arrSQLTerms, strarrOperators);
}

return result.iterator();


}

public String findIndex(String table,Hashtable<String,Object> row) throws IOException {
	for(String column_name : row.keySet()) {
		if(indexed(table,column_name)) {
			return column_name;
		}
	}
	return null;
}
public void shiftIndex(String tableName,Hashtable<String,Object> row,int oldpage) throws IOException, ClassNotFoundException {
      Table table=getTable(tableName);
       ArrayList<String> indexes=table.getIndexes();
       for (int i = 0; i < indexes.size(); i++) {
		String coloumn=indexes.get(i);
	
    	  Tree tree=  getTree(tableName,coloumn);
    	  tree.shiftRef(row.get(coloumn), oldpage);
    	  tree.save();
       }
}
public Tree getTree(String table,String coloumn) throws IOException, ClassNotFoundException {
	   Tree tree;
         // Reading the object from a file 
	FileInputStream file=new FileInputStream("data\\"+table+"-"+coloumn+".class");   
	ObjectInputStream in = new ObjectInputStream(file); 
           
         // Method for deserialization of object 
         tree =  (Tree) in.readObject(); 
           
         in.close(); 
         file.close(); 
           return tree;
      
    

 } 
public void deleteFromAllIndexes(Table table,Hashtable<String,Object> row,int page) throws Exception, IOException {
	ArrayList<String> indexes=table.getIndexes();
	for (int i = 0; i < indexes.size(); i++) {
		Tree tree= getTree(table.getName(), indexes.get(i));
		Ref reference=tree.search( row.get(indexes.get(i)));
		if(reference instanceof RefPage) {
			tree.delete(row.get(indexes.get(i)));
			
		}else {
			RefOverFlowPage overflow=(RefOverFlowPage) reference;
			if(overflow.deleteDuplicate(page)) {
				tree.delete( row.get(indexes.get(i)));
			}
		}
		tree.save();
	}
}

	

public void deleteWithIndex(Ref p,boolean clustered,Table table,Hashtable<String,Object> target,String col) throws IOException, Exception{
	Vector<PageInfo> infos=table.getPages();
	String key=table.getKey();
	Object value=target.get(col);
	if(clustered) {
		if(p instanceof RefOverFlowPage) {
			ArrayList<Integer> pagesdone=new ArrayList<Integer>(); 
			LinkedList<OverFlowPage> overflow=((RefOverFlowPage) p).getPointers();
			for (int i = 0; i < overflow.size(); i++) {
				int oldsize=overflow.size();
				OverFlowPage op=overflow.get(i);
				for (int j = 0; j < op.size(); j++) {
					int oldsizeop=op.size();
				    RefPage reference=(RefPage) op.get(j);
				    int index=reference.getPage();
				  
				    Page page=getpage(infos.get(index).getData());
				   int binary= binarySearch(page, target.get(key), key);
				  if(binary==-1) {
					  
					  continue;
				  }
				  int deleted= goLeftdelete(page, binary, target, key, target.get(key), index, table);
				   goRightdelete(page, binary-deleted, target, key, target.get(key), index, table);
                   if(page.isEmpty()) 
                    {
                	   infos.get(index).getData().delete();
                	   infos.remove(index);
                	   shiftdownIndex(table.getName(), index);
				  	    	table.checklast();
				  		  Tree tree=getTree(table.getName(),col);
				    		 if(tree.search(value)==null) {
				    			 i=overflow.size();
				    			 break;
				    		 }
				  	    	overflow=((RefOverFlowPage)tree.search(value)).pointers;
				  	    	op=overflow.get(i);
                   }else {
                	   infos.get(index).setMax(page.getmax());
                	   infos.get(index).setMin(page.getmin());
                	   infos.get(index).setfull(page.isFull());
                	   page.save();
                   }
                   int newsizeop=op.size();
                		   if(newsizeop!=oldsizeop) {
                			   j=j-1;
                			   oldsizeop=newsizeop;
                		   }
				}
				int newsize=overflow.size();
				if(newsize!=oldsize) {

					i=i-1;
					oldsize=newsize;
					}
			}
			
		}else {
			RefPage reference=(RefPage) p;
			int index=reference.getPage();
		    Page page=getpage(infos.get(index).getData());
		    int binary=binarySearch(page, target.get(key), key);
		    Hashtable<String,Object> row=(Hashtable<String, Object>) page.get(binary);
		    boolean equal=true;
		    for (String coloumn : target.keySet()) {
				if(compare(row.get(coloumn),target.get(coloumn))!=0 || !equals(row.get(coloumn),target.get(coloumn))) {
					equal=false;
				}
			}
		    if(equal) {
		    	deleteFromAllIndexes(table, row, index);
		    page.remove(binary);
		    if(page.isEmpty()) {
		    	infos.get(index).getData().delete();
		    	infos.remove(index);
		  	  shiftdownIndex(table.getName() ,index);
	  	    	table.checklast();
		    }else {
		    	infos.get(index).setMax(page.getmax());
         	   infos.get(index).setMin(page.getmin());
         	   infos.get(index).setfull(page.isFull());
         	   page.save();
		    }
		    }
		}
	}else {
		if(p instanceof RefOverFlowPage) {
			ArrayList<Integer> pagesdone=new ArrayList<Integer>(); 
			LinkedList<OverFlowPage> overflow=((RefOverFlowPage) p).getPointers();
			for (int i = 0; i < overflow.size(); i++) {
				int oldsize=overflow.size();
				OverFlowPage op=overflow.get(i);
				for (int j = 0; j < op.size(); j++) {
					int oldsizeop=op.size();
				           
				    RefPage reference=(RefPage) op.get(j);
				    int index=reference.getPage();
				    Page page=getpage(infos.get(index).getData());
				 
;
				    for (int k = 0; k < page.size(); k++) {
						   Hashtable<String,Object> row=(Hashtable<String,Object>) page.get(k);
					        if(compare(row.get(col),reference.getKey())==0&& equals(row.get(col),reference.getKey())) {
					       
					        boolean equal=true;
					        for (String coloumnName : target.keySet()) {
								if(compare(row.get(coloumnName),target.get(coloumnName))!=0||!equals(row.get(coloumnName),target.get(coloumnName))){
									equal=false;
									break;
								}
							}
					      if(equal) {  
					    	  page.remove(k);
					    	  k=k-1;
					    	  if(page.isEmpty()) {
					    		  infos.get(index).getData().delete();
					    		  infos.remove(index);
					    		  shiftdownIndex(table.getName(), index);
					    		  Tree tree=getTree(table.getName(),col);
					    		  
					    			 if(tree.search(value)==null) {
						    			 i=overflow.size();
						    			 j=op.size();
						    			 break;
						    		 }
					    		 
					  	    	overflow=((RefOverFlowPage)tree.search(value)).pointers;
					  	    	op=overflow.get(i);
					  	    	table.checklast();

					    	  }else {
					    		  infos.get(index).setMax(page.getmax());
					    		  infos.get(index).setMin(page.getmin());
					    		  infos.get(index).setfull(page.isFull());;
					    		  page.save();
					    	  }
					    	 table.save();
					    	 deleteFromAllIndexes(table, row, index);
					      }
					        }
					        
					     
				}
					int newsizeop=op.size();
					if(newsizeop!=oldsizeop) {
						j=j-1;
						oldsizeop=newsizeop;
					}
				}
				int newsize=overflow.size();
				if(newsize!=oldsize) {
					i=i-1;
					oldsize=newsize;
					}
			}
		}else {RefPage reference=(RefPage) p;
		int index=reference.getPage();
	    Page page=getpage(infos.get(index).getData());
	    boolean equal=true;
    	Hashtable<String,Object> row=null;
       int found=0;
	    for (int i = 0; i < page.size(); i++) {
	    	 row=(Hashtable) page.get(i);

	    	if(compare(row.get(col),reference.getKey())==0&&equals(row.get(col),reference.getKey())) {
	    		found=i;
	    		break;
	    }
	    }
	    for (String coloumn : row.keySet()) {
			if(compare(row.get(coloumn),target.get(coloumn))!=0||!equals(row.get(coloumn),target.get(coloumn))) {
				equal=false;
			}
		}
	    if(equal) {
	    	deleteFromAllIndexes(table, row, index);
	    page.remove(found);
	    if(page.isEmpty()) {
	    	infos.get(index).getData().delete();
	    	infos.remove(index);
	    	table.checklast();
	    	table.save();
  		  shiftdownIndex(table.getName(), index);
	    }else {
	    	infos.get(index).setMax(page.getmax());
     	   infos.get(index).setMin(page.getmin());
     	   infos.get(index).setfull(page.isFull());
     	   page.save();
	    }
	    }
			
		}
	}
	
	table.save();
}



public ArrayList selectIndex(SQLTerm term,String index) throws ClassNotFoundException, IOException, DBAppException {
	Tree tree=getTree(term._strTableName, term._strColumnName);
	ArrayList<Ref> references=new ArrayList<Ref>();
	Table table=getTable(term._strTableName);
	Vector<PageInfo> infos=table.getPages();
	ArrayList<Hashtable> results=new ArrayList<Hashtable>();
	Object key=term._objValue;
	if(tree.search(key)==null) {
		return new ArrayList<>();
	}
	switch(term._strOperator) {
	case ">=":
		if(tree.greaterThanOrEqual(key)!=null)
		references=tree.greaterThanOrEqual( key);
		break;
	case ">":
		if(tree.greaterthan(key)!=null)
		references=tree.greaterthan(key);
		break;
	case "<=":
		if(tree.getLessOrEqualThan(key)!=null)
		references=tree.getLessOrEqualThan(key);
		break;
	case "<":
		references=tree.getLessThan( key);
		break;
	case "=":
		references.add(tree.search( key));
		break;
	case "!=":
		return linearSearch(term._strColumnName, term._objValue, infos, "!=");
	default:
	}
	if(table.getKey().equals(term._strColumnName)) {
		for (int i = 0; i < references.size(); i++) {
			Ref ref=references.get(i);
			if(ref instanceof RefOverFlowPage) {
				ArrayList<Integer> pagesDone=new ArrayList<Integer>();
				LinkedList<OverFlowPage> overflow=((RefOverFlowPage) ref).getPointers();
				for (int j = 0; j < overflow.size(); j++) {
					OverFlowPage<RefPage> page=overflow.get(j);
					for (int k = 0; k < page.size(); k++) {
						RefPage reference=page.get(k);
						if(pagesDone.contains(reference.getPage())) {
							continue;
						}else {
							pagesDone.add(reference.getPage());
						}
						if(!equals(reference.getKey(), term._objValue) && term._strOperator.equals("=")) {
							continue;
						}
						Page rowPage=getpage(infos.get(reference.getPage()).getData());
						int ind=binarySearch(rowPage,reference.getKey() , term._strColumnName);
					ArrayList left=goleftSelect(rowPage, ind, reference.getKey(),table.getKey());
					ArrayList right=	gorightSelect(rowPage, ind, reference.getKey(),table.getKey());
                   left.addAll(right);
					results.addAll(left);

					}
				}
			}else {
				RefPage reference=(RefPage) ref;
				Page rowPage=getpage(infos.get(reference.getPage()).getData());
				int ind=binarySearch(rowPage,ref.getKey() , term._strColumnName);
			results.add((Hashtable) rowPage.get(ind));
			}
			}
		}else {for (int i = 0; i < references.size(); i++) {
			Ref ref=references.get(i);
			if(ref instanceof RefOverFlowPage) {
				ArrayList<Integer> pagesdone=new ArrayList<Integer>(); 
				LinkedList<OverFlowPage> overflow=((RefOverFlowPage) ref).getPointers();
				for (int j = 0; j < overflow.size(); j++) {
					OverFlowPage<RefPage> page=overflow.get(j);
					
					for (int k = 0; k < page.size(); k++) {
						RefPage reference=page.get(k);
						if(term._strOperator.equals("="))
						if(!equals(reference.getKey(),term._objValue)) {
							continue;
						}
						Page rowPage=getpage(infos.get(reference.getPage()).getData());
                         for (int l = 0; l < rowPage.size(); l++) {
							Hashtable row=(Hashtable) rowPage.get(l);
							if(compare(row.get(term._strColumnName),reference.getKey())==0&&!pagesdone.contains(reference.getPage())&&equals(row.get(term._strColumnName),reference.getKey())) {
						results.add(row);
						
							}
						}
                         pagesdone.add(reference.getPage());
	}
				}
			}else {
				RefPage reference=(RefPage) ref;
				Page rowPage=getpage(infos.get(reference.getPage()).getData());
				for (int j = 0; j < rowPage.size(); j++) {
					Hashtable row=(Hashtable) rowPage.get(j);
					if(compare(row.get(term._strColumnName),reference.getKey())==0&&equals(row.get(term._strColumnName),reference.getKey())) {
				results.add(row);
				break;
				}
			}
			
	}
}
		}
       
	return results;

}




public void insertToAllIndexes(Table table,Hashtable row,int page) throws ClassNotFoundException, IOException {
	ArrayList<String> indexes=table.getIndexes();
	for (int i = 0; i <indexes.size(); i++) {
		Tree tree=getTree(table.getName(), indexes.get(i));
		
			RefPage ref=new RefPage(page, row.get(indexes.get(i)));
			tree.insert(row.get(indexes.get(i)),ref );
			 tree.save();
		}
	}
	
	


public void indexedUpdate(Object target,Hashtable<String,Object> update,Table table) throws ClassNotFoundException, IOException {
	ArrayList<String> indexes=table.getIndexes();
	ArrayList<String> updatedindexes=new ArrayList<String>();
	Vector<PageInfo> infos=table.getPages();
	for (int i = 0; i < indexes.size(); i++) {
		if(update.get(indexes.get(i))!=null) {
			updatedindexes.add(indexes.get(i));
		}
	}
	
	Tree tree= getTree(table.getName(), table.getKey());
	Ref ref=tree.search( target);
	if(ref==null) {
		return;
	}
	if(ref instanceof RefOverFlowPage) {
		ArrayList<Integer> pagesdone=new ArrayList<Integer>(); 
		LinkedList<OverFlowPage> overflow=((RefOverFlowPage) ref).getPointers();
		for (int i = 0; i < overflow.size(); i++) {
			OverFlowPage op=overflow.get(i);
			for (int j = 0; j < op.size(); j++) {
			    RefPage reference=(RefPage) op.get(j);
			    int index=reference.getPage();
			    if(pagesdone.contains(index)) {
			    	continue;
			    }
			    Page page=getpage(infos.get(index).getData());
			   int binary= binarySearch(page, target, table.getKey());
			   goleft(page,target , binary, update, table.getKey(),index,updatedindexes,table);
			   goright(page,target , binary, update, table.getKey(),index,updatedindexes,table);

              
            	   infos.get(index).setMax(page.getmax());
            	   infos.get(index).setMin(page.getmin());
            	   page.save();
               
               pagesdone.add(index);
			}
		}
	}else {
		RefPage reference=(RefPage) ref;
		int index=reference.getPage();
	    Page page=getpage(infos.get(index).getData());
	    int binary=binarySearch(page, target, table.getKey());
	    Hashtable<String,Object> row=(Hashtable<String, Object>) page.get(binary);
		DeleteFromIndexes(updatedindexes, table, row, index);
					for(String col:update.keySet()) {
						row.put(col,update.get(col));
					}	
					InsertToIndexes(updatedindexes, table, row, index);

					infos.get(index).setMax(page.getmax());
     	   infos.get(index).setMin(page.getmin());
     	   page.save();
	    }
	table.save();
	    }


public void DeleteFromIndexes(ArrayList<String> indexes,Table table,Hashtable row,int page ) throws IOException, ClassNotFoundException {
	for (int i = 0; i < indexes.size(); i++) {
		Tree tree= getTree(table.getName(), indexes.get(i));
		Ref reference=tree.search(row.get(indexes.get(i)));
		if(reference==null) {
			return;
		}
		
		if(reference instanceof RefPage) {
			tree.delete( row.get(indexes.get(i)));
			
		}else {
			RefOverFlowPage overflow=(RefOverFlowPage) reference;
			if(overflow.deleteDuplicate(page)) {
				tree.delete( row.get(indexes.get(i)));
			}
		}
		tree.save();
	}
	}
public void shiftdownIndex(String table,int pageDeleted) throws ClassNotFoundException, IOException {
	
	Table t=getTable(table);
	ArrayList<String> index=t.getIndexes();
	for (int i = 0; i < index.size(); i++) {
		Tree tree=getTree(table, index.get(i));
		tree.shiftPage(pageDeleted);
		tree.save();
	}
}
public void InsertToIndexes(ArrayList<String> indexes,Table table,Hashtable row,int page ) throws IOException, ClassNotFoundException {
	for (int i = 0; i < indexes.size(); i++) {
		Tree tree=getTree(table.getName(), indexes.get(i));
			
			RefPage ref=new RefPage(page, row.get(indexes.get(i)));
			tree.insert(row.get(indexes.get(i)),ref );
			tree.save();
		
		
	}
	
	}


public void createBTreeIndex(String strTableName,  String strColName) throws DBAppException, IOException {
	Hashtable test=new Hashtable();
	test.put(strColName, new Polygon());
	if(checkColoumns(strTableName, test)) {
	 throw new DBAppException("Cannot create a BTree on a polygon");
	}
	changeIndex(strTableName, strColName);
Table table=getTable(strTableName);
table.getIndexes().add(strColName);
Vector<PageInfo> infos=table.getPages();FileReader reader=new FileReader("config//DBApp.properties");  

Properties p=new Properties();  
p.load(reader); 
int size=Integer.parseInt((String) p.get("NodeSize"));
BPTree tree=new BPTree (strTableName, strColName,size);
for (int i = 0; i < infos.size(); i++) {
	Page<Hashtable> page=getpage(infos.get(i).getData());
	for (int j = 0; j < page.size(); j++) {
		RefPage ref=new RefPage(i,(Comparable) page.get(j).get(strColName));
		tree.insert((Comparable) page.get(j).get(strColName), ref);
	}
}

table.save();
tree.save();
}
public void createRTreeIndex(String strTableName,String strColName) throws IOException, DBAppException {
	Hashtable test=new Hashtable();
	test.put(strColName, new Polygon());
	if(checkColoumns(strTableName, test)) {
		changeIndex(strTableName, strColName);
		Table table=getTable(strTableName);
		table.getIndexes().add(strColName);
		Vector<PageInfo> infos=table.getPages();
		FileReader reader=new FileReader("config//DBApp.properties");  
	      
	    Properties p=new Properties();  
	    p.load(reader); 
	   int size=Integer.parseInt((String) p.get("NodeSize"));
		RTree tree=new RTree (strTableName, strColName,size);
		for (int i = 0; i < infos.size(); i++) {
			Page<Hashtable> page=getpage(infos.get(i).getData());
			for (int j = 0; j < page.size(); j++) {
				RefPage ref=new RefPage(i, page.get(j).get(strColName));
				tree.insert( page.get(j).get(strColName), ref);
			}
		}
		

table.save();
tree.save();
	}else {
		throw new DBAppException("Only polygons can have an RTree index");
	}

}



public  void binaryInserttest(Page<Hashtable> page, String key,Hashtable row, int left , int right) { 
	int mid = left + (right - left)/2;
	Object value = row.get(key);
	
	if (page.isEmpty()) {
		page.add(row);
		return;
	}
	
	else if (((mid==page.size() -1) || compare(value, page.get(mid+1).get(key))<0)
			& compare(value, page.get(mid).get(key)) >= 0){
		page.add(mid+1, row);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) < 0 && mid==0) {
		page.add(0,row);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) < 0) {
		binaryInserttest(page, key, row, left, mid -1);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) >= 0) {
		binaryInserttest(page, key, row, mid +1, right);
		return;
	}
	
		page.add(0,row);
}

public  void insertSortSelect(ArrayList<Hashtable> page, String key,Hashtable row, int left , int right) { 
	int mid = left + (right - left)/2;
	Object value = row.get(key);
	
	if (page.isEmpty()) {
		page.add(row);
		return;
	}
	
	else if (((mid==page.size() -1) || compare(value, page.get(mid+1).get(key))<0)
			& compare(value, page.get(mid).get(key)) >= 0){
		page.add(mid+1, row);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) < 0 && mid==0) {
		page.add(0,row);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) < 0) {
		insertSortSelect(page, key, row, left, mid -1);
		return;
	}
	else if (compare(value, page.get(mid).get(key)) >= 0) {
		insertSortSelect(page, key, row, mid +1, right);
		return;
	}
	
		page.add(0,row);
}

public boolean exist(ArrayList<Hashtable> list,Hashtable row) {
	for (int i = 0; i < list.size(); i++) {
		Hashtable ro=list.get(i);
		if(ro==row) {
			return true;
		}
	}
	return false;
	
}

public BPTree treeType(BPTree tree) {
	return tree;
}

public static boolean equals(Polygon a,Polygon b) {
	if (a == null) {
        return (b == null);
    }
    if (b == null) {
        return false;
    }
    if (a.npoints != b.npoints) {
        return false;
    }
    if (!Arrays.equals(a.xpoints, b.xpoints)) {
        return false;
    }
    if (!Arrays.equals(a.ypoints, b.ypoints)) {
        return false;
    }
    return true;
}


public static boolean equals(Object a,Object b) {
	 
	 if(a instanceof Polygon) {
		Polygon p1=(Polygon) a;
		Polygon p2=(Polygon) b;
		  if (p1 == null) {
	          return (p2 == null);
	      }
	      if (p2 == null) {
	          return false;
	      }
	      if (p1.npoints != p2.npoints) {
	          return false;
	      }
	      if (!Arrays.equals(p1.xpoints, p2.xpoints)) {
	          return false;
	      }
	      if (!Arrays.equals(p1.ypoints, p2.ypoints)) {
	          return false;
	      }
	      return true;
	  }else {
		  return a.equals(b);
	 }
}
public void changeIndex(String table,String coloumn) throws IOException {
	
	File file=new File("data//metadata.csv");
	String temp="data//temp.csv";
	File fileTemp=new File(temp);

	 BufferedReader csvReader = new BufferedReader(new FileReader("data//metadata.csv"));
	 String row;
	 boolean foundtable=false;
	 Hashtable<String, String> types=new Hashtable<String,String>();
	 String strTableName="";
	 String coloumn_name="";
	 String value="";
	 String clustered="";
	 String indexed="";
	 FileWriter FW = new FileWriter(temp,true);

	 while ((row = csvReader.readLine()) != null) {
	     String[] data = row.split(",");
	     if(data[0].equals(table)&& data[1].equals(coloumn)) {
	    		
	    			FW.append(data[0]);
	    			FW.append(",");
	    			FW.append(data[1]);
	    			FW.append(",");
	    			FW.append(data[2]);
	    			FW.append(",");
	    	        FW.append(data[3]);
	    			FW.append(",");
	    	        FW.append("True");
	    	        FW.append("\n");

	    		}else {
	    			FW.append(data[0]);
	    			FW.append(",");
	    			FW.append(data[1]);
	    			FW.append(",");
	    			FW.append(data[2]);
	    			FW.append(",");
	    	        FW.append(data[3]);
	    			FW.append(",");
	    	        FW.append(data[4]);
	    	        FW.append("\n");
	     }
	 }
	
	
	csvReader.close();
		FW.flush();
	FW.close();
	file.delete();
	File n=new File("data//metadata.csv");
     fileTemp.renameTo(n);
     
	
}
public boolean IndexKey(Table table,String col) throws IOException {
	
	return table.getKey().equals(col)||indexed(col, table.getName());
}
}