package dont_panic;

import java.awt.Polygon;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;

public class DBAppTest {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		
		  String strTableName = "a"; DBApp dbApp = new DBApp();
		  
		 /* 
		  dbApp.init(); Hashtable htblColNameType = new Hashtable();
		  htblColNameType.put("poly", "java.awt.Polygon"); htblColNameType.put("name",
		  "java.lang.String"); dbApp.createTable(strTableName, "poly",
		  htblColNameType);
		  
		  */
		  
		  
		  
		  
		/*  dbApp.createRTreeIndex(strTableName, "poly");
		  
		  
		  Hashtable htbl = new Hashtable();
		  
		  
		  int[] p3x = {400}; int[] p3y = {1}; Polygon p1 = new Polygon(p3x, p3y,
		  p3x.length); Polygon p2 = new Polygon(p3y, p3y, p3x.length); Polygon p3 = new
		  Polygon(p3y, p3x, p3x.length); htbl.put("poly", p1); htbl.put("name","a");
		  
		  
		  String[] namespolygon = { "p1", "p2", "p3" }; int[] p1x = { 10, 20, 30 };
		  int[] p1y = { 10, 20, 30 }; int[] p2x = { 10, 20 }; int[] p2y = { 10, 20 };
		  int[] p3x = { 30, 20, 10 }; int[] p3y = { 30, 20, 10 };
		  
		  String p3string="(30,30),(20,20),(10,10)";
		  
		  Polygon one = new Polygon(p1x, p1y, p1x.length); Polygon two = new
		  Polygon(p2x, p2y, p2x.length); Polygon three = new Polygon(p3x, p3y,
		  p3x.length);
		  
		  
		  System.out.println(dbApp.compare(three, one));
		  
		  
		  
		  Hashtable htblColNameValue =new Hashtable();
		  
		  
		  
		  
		  for (int i = 0; i < 200; i++) { String nam=namespolygon[(int)(Math.random() *
		  (namespolygon.length-1 - 0 + 1) + 0)]; switch(nam) { case "p1":
		  htblColNameValue.put("name",nam); htblColNameValue.put("poly",one);
		  dbApp.insertIntoTable(strTableName, htblColNameValue); break; case "p2":
		  htblColNameValue.put("name",nam); htblColNameValue.put("poly",two);
		  dbApp.insertIntoTable(strTableName, htblColNameValue); break; case "p3":
		  htblColNameValue.put("name",nam); htblColNameValue.put("poly",three);
		  dbApp.insertIntoTable(strTableName, htblColNameValue); break; }
		  htblColNameValue.clear(); }
		  
		  
		  
		  dbApp.insertIntoTable(strTableName, htbl);
		  
		  
		  htbl.clear(); htbl.put("poly", p2); htbl.put("name","are");
		  dbApp.insertIntoTable(strTableName, htbl); htbl.clear(); htbl.put("poly",
		  p3); htbl.put("name","doing"); dbApp.insertIntoTable(strTableName, htbl);
		  
		  */
		  
		  String[] names = { "ahmed tarek", "mohamed tarek", "nassar", "sam", "samir",
		  "adham madinet nasr", "Mcdonalds", "Rick", "Shane", "Negan",
		  "Elizabeth baioumy" }; Double[] gpas = { new Double(0.1), new Double(0.2),
		  new Double(0.3), new Double(0.4), new Double(0.5), new Double(0.6), new
		  Double(0.7), new Double(0.8), new Double(0.9), new Double(1), 1.1, 1.2, 1.3,
		  1.4, 1.5, 1.6 };
		  
		  
		  dbApp.init(); ; Hashtable htblColNameType = new Hashtable();
		  htblColNameType.put("id", "java.lang.Integer"); htblColNameType.put("name",
		  "java.lang.String"); htblColNameType.put("gpa", "java.lang.double");
		  dbApp.createTable(strTableName, "id", htblColNameType);
		  
		  
		  
		/*  
		  dbApp.createBTreeIndex(strTableName, "id");*/
		  
		  
		  for (int i = 0; i < 200; i++) { Integer id = (int) (Math.random() * (50 - 0 +
		  1) + 0); Double gpa = gpas[(int) (Math.random() * (gpas.length - 1 - 0 + 1) +
		  0)]; String name = names[(int) (Math.random() * (names.length - 1 - 0 + 1) +
		  0)]; Hashtable htblColNameValue = new Hashtable(); htblColNameValue.put("id",
		  id); htblColNameValue.put("name", name); htblColNameValue.put("gpa", gpa);
		  dbApp.insertIntoTable(strTableName, htblColNameValue);
		  htblColNameValue.clear();
		  
		  }
		  
		  
		/*  
		  Hashtable htblColNameValue = new Hashtable( ); htblColNameValue.put("id",
		  18); htblColNameValue.put("name", "Puss" ); htblColNameValue.put("gpa", 0.7
		  ); dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear();
		  
		  
		  
		  
		  SQLTerm[] terms=new SQLTerm[4]; terms[0]=new SQLTerm();
		  terms[0]._strTableName="a"; terms[0]._strOperator="<";
		  terms[0]._strColumnName="gpa"; terms[0]._objValue=1.0; terms[1]=new
		  SQLTerm(); terms[1]._strTableName="a"; terms[1]._strOperator="<";
		  terms[1]._objValue=30; terms[1]._strColumnName="id"; terms[2]=new SQLTerm();
		  terms[2]._objValue="mohamed tarek"; terms[2]._strColumnName="name";
		  terms[2]._strOperator="="; terms[2]._strTableName="a"; terms[3]=new
		  SQLTerm(); terms[3]._objValue="ahmed tarek"; terms[3]._strColumnName="name";
		  terms[3]._strOperator="="; terms[3]._strTableName="a"; String[] op=new
		  String[3]; op[0]="OR"; op[1]="AND"; op[2]="XOR";
		  
		  dbApp.selectNew(terms, op);
		  
		  
		  
		  
		  SQLTerm[] terms=new SQLTerm[2]; terms[0]=new SQLTerm(); terms[1]=new
		  SQLTerm(); terms[0]._objValue="mohamed tarek";
		  terms[0]._strColumnName="name"; terms[0]._strOperator="=";
		  terms[0]._strTableName="a"; terms[1]._objValue=36;
		  terms[1]._strColumnName="id"; terms[1]._strOperator="=";
		  terms[1]._strTableName="a"; dbApp.selectNew(terms, new String[]{"AND"});
		  
		  
		  
		  
		  Hashtable htblColNameValue = new Hashtable( ); htblColNameValue.put("id", new
		  Integer(1)); htblColNameValue.put("name", new String("Ahmed Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.95 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue );
		  
		  
		  htblColNameValue.clear();
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  htblColNameValue.put("id", new Integer( 1 )); htblColNameValue.put("name",
		  new String("Ahmed Noor" ) ); htblColNameValue.put("gpa", new Double( 0.95 )
		  ); dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 2 ));
		  htblColNameValue.put("name", new String("Ahmed Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.95 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer( 3 )); htblColNameValue.put("name",
		  new String("Dalia Noor" ) ); htblColNameValue.put("gpa", new Double( 1.25 )
		  ); dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 4 ));
		  htblColNameValue.put("name", new String("John Noor" ) );
		  htblColNameValue.put("gpa", new Double( 1.5 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer( 5 )); htblColNameValue.put("name",
		  weeeeqdaZZ `* new String("Zaky Noor" ) ); htblColNameValue.put("gpa", new
		  Double( 0.88 ) ); dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 6 ));
		  htblColNameValue.put("name", new String("John Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.95 ) );
		  System.out.println(htblColNameValue.toString()); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer( 6 )); htblColNameValue.put("name",
		  new String("Ahmed Noor" ) ); htblColNameValue.put("gpa", new Double( 0.95 )
		  ); dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 1 ));
		  htblColNameValue.put("name", new String("Dalia Noor" ) );
		  htblColNameValue.put("gpa", new Double( 1.25 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer( 3 )); htblColNameValue.put("name",
		  new String("John Noor" ) ); htblColNameValue.put("gpa", new Double( 1.5 ) );
		  dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 8 ));
		  htblColNameValue.put("name", new String("Zaky Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.88 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue );
		  
		  
		  htblColNameValue.clear(); htblColNameValue.put("id", new Integer( 3 ));
		  htblColNameValue.put("name", new String(" Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.95 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer( 3 )); htblColNameValue.put("name",
		  new String("Ahmed " ) ); htblColNameValue.put("gpa", new Double( 3.95 ) );
		  dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 3 ));
		  htblColNameValue.put("name", new String("Dalia " ) );
		  htblColNameValue.put("gpa", new Double( 1.25 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue ); htblColNameValue.clear( );
		  htblColNameValue.put("id", new Integer(3 )); htblColNameValue.put("name", new
		  String("John " ) ); htblColNameValue.put("gpa", new Double( 1.5 ) );
		  dbApp.insertIntoTable( strTableName , htblColNameValue );
		  htblColNameValue.clear( ); htblColNameValue.put("id", new Integer( 3));
		  htblColNameValue.put("name", new String("Zaky " ) );
		  htblColNameValue.put("gpa", new Double( 0.88 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue );*/
		  
		  
		  /*
		  
		  Hashtable htbl=new Hashtable(); htbl.put("gpa", new Double(1));
		  dbApp.updateTable(strTableName, "2", htbl);
		  
		  
		  
		  dbApp.createBTreeIndex(strTableName, "id");
		  dbApp.createBTreeIndex(strTableName, "name");
		  
		  
		  
		  BPTree sa=(BPTree) dbApp.getTree(strTableName, "id");
		  System.out.println(sa.toString()); System.out.println("tree"+sa.getPage(2));
		  System.out.println();
		  
		  dbApp.createBTreeIndex("a", "name");
		  
		  */
		/*  
		  Hashtable htblColNameValue = new Hashtable();
		  
		  
		  htblColNameValue.clear(); htblColNameValue.put("id", new Integer( 4 ));
		  htblColNameValue.put("name", new String("Ahmed Noor" ) );
		  htblColNameValue.put("gpa", new Double( 0.95 ) ); dbApp.insertIntoTable(
		  strTableName , htblColNameValue );
		  
		  
		  
		  
		  htblColNameValue = new Hashtable();
		  
		  htblColNameValue.clear(); htblColNameValue.put("name", new String("rey"));
		  dbApp.updateTable(strTableName, p3string, htblColNameValue);
		  
		  
		  
		  
		  
		  */
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		/*  
		  htblColNameValue = new Hashtable();
		  
		  htblColNameValue.clear(); htblColNameValue.put("poly",one);
		  dbApp.deleteFromTable(strTableName, htblColNameValue);
		  */
		  
		  
		  FileInputStream fileIn = new FileInputStream("data//" + "a" + ".class");
		  ObjectInputStream objectIn = new ObjectInputStream(fileIn);
		  
		  Table obj = (Table) objectIn.readObject(); Vector<PageInfo> in =
		  obj.getPages();
		  
		  for (int i = 0; i < in.size(); i++) {
		  
		  FileInputStream fileI = new FileInputStream(in.get(i).getData());
		  ObjectInputStream objectI = new ObjectInputStream(fileI); Page asda = (Page)
		  objectI.readObject();
		  
		  System.out.println(asda.toString() + asda.size()); }
		  
		/*
		 * SQLTerm[] arrSQLTerms; arrSQLTerms = new SQLTerm[5]; arrSQLTerms[0]=new
		 * SQLTerm(); arrSQLTerms[0]._strTableName = "test";
		 * arrSQLTerms[0]._strColumnName = "gpa"; arrSQLTerms[0]._strOperator = "!=";
		 * arrSQLTerms[0]._objValue = new Double(10); arrSQLTerms[1]=new SQLTerm();
		 * arrSQLTerms[1]._strTableName = "test"; arrSQLTerms[1]._strColumnName = "gpa";
		 * arrSQLTerms[1]._strOperator = "!="; arrSQLTerms[1]._objValue = new Double(6);
		 * arrSQLTerms[2]=new SQLTerm(); arrSQLTerms[2]._strTableName = "test";
		 * arrSQLTerms[2]._strColumnName = "gpa"; arrSQLTerms[2]._strOperator = "=";
		 * arrSQLTerms[2]._objValue = new Integer(3); arrSQLTerms[3]=new SQLTerm();
		 * arrSQLTerms[3]._strTableName = "test"; arrSQLTerms[3]._strColumnName = "id";
		 * arrSQLTerms[2]._strOperator = "="; arrSQLTerms[3]._objValue = new Integer(3);
		 * arrSQLTerms[4]=new SQLTerm(); arrSQLTerms[4]._strTableName = "test";
		 * arrSQLTerms[4]._strColumnName = "gpa"; arrSQLTerms[2]._strOperator = "=";
		 * arrSQLTerms[4]._objValue = new Integer(3);
		 * 
		 * String[] strarrOperators = new String[4]; strarrOperators[0]="OR";
		 * strarrOperators[1]="OR"; strarrOperators[2]="AND"; strarrOperators[3]="OR";
		 * 
		 * 
		 * BinaryInfo te=dbApp.findBinary(arrSQLTerms, strarrOperators);
		 * System.out.println(te.index+" "+te.end);
		 * 
		 * 
		 * dbApp.createBTreeIndex(strTableName, "name");
		 * 
		 * 
		 * 
		 * Tree t = dbApp.getTree("a", "poly");
		 * 
		 * 
		 * RefOverFlowPage ref=(RefOverFlowPage) t.search(one)
		 * 
		 * System.out.println(t.toString());
		 * 
		 * 
		 * 
		 * RefOverFlowPage wassup= (RefOverFlowPage) t.search("manga");
		 * wassup.getInfo();
		 * 
		 * 
		 * 
		 * RefPage wassup=(RefPage) t.search("Ahmed ");
		 * System.out.println(wassup.getPage());
		 * 
		 * 
		 * 
		 * RefOverFlowPage ref=(RefOverFlowPage) t.search(new Integer(3)); LinkedList
		 * a=ref.getPointers(); for (int i = 0; i <a.size(); i++) {
		 * OverFlowPage<RefPage> overflow=(OverFlowPage<RefPage>) a.get(i); for (int j =
		 * 0; j < overflow.size(); j++) { RefPage er=overflow.get(j);
		 * System.out.println(er.getKey()); } }
		 * 
		 * 
		 * 
		 * System.out.println(t.toString());
		 * 
		 * SQLTerm[] terms = new SQLTerm[2]; terms[0] = new SQLTerm();
		 * terms[0]._objValue = new Integer(3); terms[0]._strColumnName = "id";
		 * terms[0]._strOperator = "="; terms[0]._strTableName = strTableName; terms[1]
		 * = new SQLTerm(); terms[1]._objValue = "manga"; terms[1]._strColumnName =
		 * "name"; terms[1]._strOperator = "="; terms[1]._strTableName = strTableName;
		 * String[] op = new String[1]; op[0] = "AND";
		 * 
		 * dbApp.selectNew(terms, op);
		 * 
		 * 
		 * 
		 * 
		 * SQLTerm[] terms = new SQLTerm[1]; terms[0] = new SQLTerm();
		 * terms[0]._objValue=three; terms[0]._strColumnName="poly";
		 * terms[0]._strOperator="="; terms[0]._strTableName="a"; String[] op=new
		 * String[0];
		 * 
		 * 
		 * 
		 * 
		 * dbApp.selectFromTable(terms, op);
		 * 
		 * 
		 * 
		 * 
		 * SQLTerm[] term=new SQLTerm[1]; term[0]=new SQLTerm();
		 * term[0]._objValue="manga" + ""; term[0]._strColumnName="name";
		 * term[0]._strTableName="a"; term[0]._strOperator="="; String[] test=new
		 * String[0]; dbApp.selectNew(term, test);
		 */

	}
}