// // This sample uses the Apache HTTP client from HTTP Components (http://hc.apache.org/httpcomponents-client-ga/)
package edu.ovgu.coddsgraph.CoddsGraph;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class CrawlAndIndex 
{
	private static CrawlAndIndex jsonreqobj=new CrawlAndIndex();
	private  String vertexFileName;
	private  String edgeFileName;
	private  String authorFileName;
	private  String UpstreamFileName;
	private  StringBuilder sb;
	private static boolean ft=true;
	DateTimeFormatter uniqueId_ts = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
	DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");
	private  FileWriter vertexFW;
	private  FileWriter edgeFW;
	private  FileWriter upstreamFW;
	private  FileWriter authorFW;
	private static int TOTAL_ID_COUNT_TOQUERY=100;
	private int counter=0;
	//private Iterator it ;
	private static int NUM_HOPS=0;
	private static int TOTAL_HOPS=1;
	private static Map<Object, Object> idsToVisitofCurrentHop;
	private static Map<Object, Object> idsToVisitofNextHop;
	
	private static Map<Object, Object> idsVisited;
	static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm");
	static LocalDateTime now = LocalDateTime.now();
	
    public static void main(String[] args) 
    {
    	String temp="";
    	int count=0;
    	idsToVisitofCurrentHop = new HashMap<>();
    	idsToVisitofNextHop = new HashMap<>();
        idsVisited=new HashMap<>();
    	String JSONResult_seed="";
    	String JSONResult="";	
    	try {
			JSONResult_seed= jsonreqobj.getData("And(And(Ti='a relational model of data for large shared data banks',Composite(AA.AuN=='e f codd')),Y=1970)" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN","5");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			try {
				Thread.sleep(36000000);
				JSONResult_seed= jsonreqobj.getData("And(And(Ti='a relational model of data for large shared data banks',Composite(AA.AuN=='e f codd')),Y=1970)" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN","5");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		}
    	System.out.println("Indexing started");
    	JsonObject root = new JsonParser().parse(JSONResult_seed).getAsJsonObject();
   		JsonArray jsonarray = root.getAsJsonArray("entities");
   		for(JsonElement json:jsonarray){
    	idsToVisitofCurrentHop.put(json.getAsJsonObject().get("Id").toString(), NUM_HOPS);
   		}
   		jsonreqobj.addIdsToList(JSONResult_seed);
    	jsonreqobj.indexEdges(JSONResult_seed); 	
    	jsonreqobj.indexVertex(JSONResult_seed); 		

    
        //idsToVisitCopy.putAll(idsToVisit);
	   	 Iterator it = idsToVisitofCurrentHop.entrySet().iterator();
	   	 
	   	 while (it.hasNext()) {
	   	 Map.Entry pair = (Map.Entry)it.next();
		 temp= temp+"Id="+ pair.getKey().toString()+",";
	   	 count++;
	   	 if (count==TOTAL_ID_COUNT_TOQUERY|| !(it.hasNext())){ 
	   		try {
				JSONResult= jsonreqobj.getData("OR("+temp.substring(0, temp.length()-1)+")" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN",Integer.toString(TOTAL_ID_COUNT_TOQUERY));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				try {
					Thread.sleep(36000000);
					JSONResult= jsonreqobj.getData("OR("+temp.substring(0, temp.length()-1)+")" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN",Integer.toString(TOTAL_ID_COUNT_TOQUERY));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
	   
	   		System.out.println("Before removing items from the list :"+ idsToVisitofCurrentHop.size());
	   		jsonreqobj.indexEdges(JSONResult);
		   	 jsonreqobj.indexVertex(JSONResult);
		   	
		   	 it = idsToVisitofCurrentHop.entrySet().iterator();	   	
		     count=0;
		     temp="";
	   	 }
	    
	    }
     System.out.println("Indexing ends");
    }


	private  String getData(String expression, String attributes, String count) throws Exception {
		HttpClient httpclient = HttpClients.createDefault();       
		String JSONResult=null;
        try
        {
        	System.out.println(expression);
        	  URIBuilder builder = new URIBuilder("https://westus.api.cognitive.microsoft.com/academic/v1.0/evaluate");
              builder.setParameter("expr", expression);
              builder.setParameter("model", "latest");
              builder.setParameter("attributes", attributes );
              builder.setParameter("count", count);
              //builder.setParameter("from", from);
        	  System.out.println(builder.toString());
              URI uri = builder.build();
              HttpGet request = new HttpGet(uri);
              request.setHeader("Ocp-Apim-Subscription-Key", "dbe029f01ce145f5a41390c981f3bfc5"); // dbe029f01ce145f5a41390c981f3bfc5
            // Request body
            HttpUriRequest reqEntity = request;    
            HttpResponse response = httpclient.execute(reqEntity);
            HttpEntity entity = response.getEntity();

            if (entity != null) 
            {
              JSONResult=EntityUtils.toString(entity);
              System.out.println(JSONResult);
       		 
            }
            
        }
        catch (Exception e)
        {
        	throw e;
        }
        return JSONResult;
	}
     	
    
	 public void indexVertex(String jsonArray){	
			sb = new StringBuilder();
			String uniqueId_paper;
			int from=0;
			String RId;
			String ReferenceIds = "";
			String JSONResult_edges="";
			int removeCnt=0;
			
			
		   if (vertexFileName == null) {
				vertexFileName = "papers" + dtf.format(now) + ".csv";
				
				try {
					vertexFW = new FileWriter(new File(vertexFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("paperID:ID");
					sb.append(',');
					sb.append("Title");
					sb.append(',');
					sb.append("PublishInYear:int");
					sb.append(',');
					sb.append("Journal Id");
					sb.append(',');
					sb.append("Journal Name");
					sb.append(',');
					sb.append("Conference Series Id");
					sb.append(',');
					sb.append("Conference Series Name");
					sb.append(',');
					sb.append("Source URL");
					sb.append(',');
					sb.append("Venue Short Name");
					sb.append(',');
					sb.append("TimestampAdded");
					sb.append(',');
					sb.append("TimestampLastVisited");
					sb.append(',');		
					sb.append("TimestampMod");
					sb.append(',');		
					sb.append(":LABEL");
					vertexFW.write(System.getProperty("line.separator"));
					vertexFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   		
		   		JsonObject root = new JsonParser().parse(jsonArray).getAsJsonObject();
		   		JsonArray jsonarray = root.getAsJsonArray("entities");
		   		for(JsonElement json:jsonarray){		   		
		   		try {
		   			uniqueId_paper=getUniqueId();
			   		LocalDateTime now = LocalDateTime.now();
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_paper);
			   		sb.append(',');
		   			sb.append(json.getAsJsonObject().get("Id"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("Ti").toString().replaceAll("^\"|\"$", ""));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("Y"));
			   		sb.append(','); 
			 		sb.append(json.getAsJsonObject().get("J.JId"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("J.JN"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("C.CId"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("C.CN"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("S.U"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("VSN"));
			   		sb.append(',');
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append("paper");
			   		vertexFW.write(System.getProperty("line.separator"));
			   		vertexFW.write(sb.toString());	
			   		
			   		//write log after every write to papers.csv file
			   		writeToUpstream(operation.CREATE, subject.PAPERS,uniqueId_paper, root);
			   		
			   		//write author details to author.csv
			   		jsonreqobj.indexAuthor(json.getAsJsonObject().get("Id").toString(), jsonArray);
			   		
			   		idsVisited.put(json.getAsJsonObject().get("Id").toString(), NUM_HOPS);
			   		
					if(!idsToVisitofCurrentHop.isEmpty()){
						idsToVisitofCurrentHop.remove(json.getAsJsonObject().get("Id").toString());
					//	System.out.println(json.getAsJsonObject().get("Id").toString());
						removeCnt++;
						/*if(idsToVisitofCurrentHop.containsKey(json.getAsJsonObject().get("Id").toString())){
							System.out.println("contains after removing");
						}
						else{
							System.out.println(removeCnt + "Doesn't contain after removing");
						}*/
						
					
					}
					
						RId= "RId="+json.getAsJsonObject().get("Id").toString() ;
						ReferenceIds=ReferenceIds+ RId+",";		
						
						//String citationCount=json.getAsJsonObject().get("CC").toString();
							
						
						//JSONResult_edges=getData(ReferenceIds, "Id,RId", "5",Integer.toString(from));
						//JSONResult_edges.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "");
				   	
						//add queried id  toVisited list and remove id from toVisit list
					
						
						
						//write edge data to cited-by.csv file
					/*	jsonreqobj.indexEdges(JSONResult_edges);
						
						if(idsToVisitofCurrentHop.size()>0){
							//do nothing
						}else{
							NUM_HOPS++;
							idsToVisitofCurrentHop.putAll(idsToVisitofNextHop);
							idsToVisitofNextHop.clear();
						}*/
						
						//To get all records for citations >1000
						/*int citedCount=Integer.parseInt(citationCount);
						int quotient;
						int remainder;
						if (citedCount>1000){
							quotient=citedCount/1000; 
							remainder=citedCount%1000;
							for(int i=0;i<quotient;i++){					
								JSONResult_edges=getData(RId, "Id", citationCount,Integer.toString(from));
								jsonreqobj.indexEdges(json.getAsJsonObject().get("Id").toString(),JSONResult_edges);
								from=from+1000;
							}
							if (remainder>0){
								from=from+remainder;
								JSONResult_edges=getData(RId, "Id", citationCount,Integer.toString(from));
								jsonreqobj.indexEdges(json.getAsJsonObject().get("Id").toString(),JSONResult_edges);
							}
						}
						else{
							JSONResult_edges=getData(RId, "Id", citationCount,Integer.toString(from));
							jsonreqobj.indexEdges(json.getAsJsonObject().get("Id").toString(),JSONResult_edges);
						}*/
						
						 			  		
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			  } 
		   		/*ReferenceIds=ReferenceIds.substring(0, ReferenceIds.length()-1);
		   		JSONResult_edges=getData(ReferenceIds, "Id,RId", "100",Integer.toString(from));
		   			jsonreqobj.indexEdges(JSONResult_edges);*/
		   		System.out.println(ReferenceIds);
		   		System.out.println("Count of removed items : "+ removeCnt);
				System.out.println("After removing 100 elements : "+idsToVisitofCurrentHop.size());

		   		if(NUM_HOPS<TOTAL_HOPS){
		   		try {
		   			
					JSONResult_edges=getData("OR("+ReferenceIds.substring(0, ReferenceIds.length()-1)+")", "Id,RId", Integer.toString(TOTAL_ID_COUNT_TOQUERY));
				} catch (Exception e) {
					try {
						Thread.sleep(3600000);
						JSONResult_edges=getData("OR("+ReferenceIds.substring(0, ReferenceIds.length()-1)+")", "Id,RId", Integer.toString(TOTAL_ID_COUNT_TOQUERY));
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}	
				}	
		   			jsonreqobj.addIdsToList(JSONResult_edges);
		   		}
		   		
				
				if(idsToVisitofCurrentHop.size()>0){
					//do nothing
				}else{
					NUM_HOPS++;
					idsToVisitofCurrentHop.putAll(idsToVisitofNextHop);
					idsToVisitofNextHop.clear();
				}
	 }
	 
	 /**
	  * Adds From and To paper id's to the cited_by file, updates idsVisited list, removes visited id's from idsToVisit list
	  * @param paperIds
	  * @param citedbyPaperId
	  */
	 public void indexEdges(String paperIds){	 
		 sb = new StringBuilder();	
		 String[] referenceIds;
		 String uniqueId_citedby;
		   if (edgeFileName == null) {
				edgeFileName = "cites" + dtf.format(now) + ".csv";
				try {
					edgeFW = new FileWriter(new File(edgeFileName), true);
					
					sb.append(":START_ID");
					sb.append(',');
					sb.append(":END_ID");
					sb.append(',');
					sb.append("UniqueId");
					sb.append(',');
					sb.append("TimeStamp");
					sb.append(',');
					sb.append(":TYPE");
					edgeFW.write(System.getProperty("line.separator"));
					edgeFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   
		   JsonObject root = new JsonParser().parse(paperIds).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		for(JsonElement json:jsonarray){
	   			if(json.getAsJsonObject().toString().contains("RId")){				
	   			referenceIds=json.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "").split(",");			
	   			for(int i=0;i<referenceIds.length;i++){
	   				try {
		   				uniqueId_citedby=getUniqueId();
			   			sb=new StringBuilder();
			   			
			   			sb.append(json.getAsJsonObject().get("Id"));
				   		sb.append(',');
				   		sb.append(referenceIds[i]);
				   		sb.append(',');
				   		sb.append(uniqueId_citedby);
				   		sb.append(',');
				   		DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				   		LocalDateTime now = LocalDateTime.now();
				   		sb.append(dtf_ts.format(now));
				   		sb.append(',');
				   		sb.append("Cites");
				   		edgeFW.write(System.getProperty("line.separator"));
				   		edgeFW.write(sb.toString());						
						writeToUpstream(operation.CREATE, subject.CITED_BY, uniqueId_citedby, json.getAsJsonObject());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	   			}  		
   			}
	   	} 
	 }
	 
	 public void addIdsToList(String paperIds){
		 String[] paperToVisitIds;
		  JsonObject root = new JsonParser().parse(paperIds).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		for(JsonElement json:jsonarray){ 			
	   			if(NUM_HOPS<TOTAL_HOPS){
 					if(!(idsToVisitofCurrentHop.containsKey(json.getAsJsonObject().get("Id").toString()))&&
 							!(idsVisited.containsKey(json.getAsJsonObject().get("Id").toString()))&&
 							!(idsToVisitofNextHop.containsKey(json.getAsJsonObject().get("Id").toString()))){
 						idsToVisitofNextHop.put( json.getAsJsonObject().get("Id").toString(),NUM_HOPS+1);
 					}
	   			}
	   			if(json.getAsJsonObject().toString().contains("RId")){				
	   			paperToVisitIds=json.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "").split(",");			
	   			for(int i=0;i<paperToVisitIds.length;i++){   			
		   				if(NUM_HOPS<TOTAL_HOPS){
		   					if(!(idsToVisitofCurrentHop.containsKey(paperToVisitIds[i]))&&
		   							!(idsVisited.containsKey(paperToVisitIds[i]))&&
		   							!(idsToVisitofNextHop.containsKey(paperToVisitIds[i]))){
		   						idsToVisitofNextHop.put( paperToVisitIds[i],NUM_HOPS+1);
		   					}
		   				}
	   			}
	   		}
	   	}

	 }
	 
	 public String getUniqueId(){
		return  (uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((counter++)%99));
	 }

	 public void indexAuthor(String fromPaperId,String jsonArray){
		 sb = new StringBuilder();	
		 String authorName=new String();
		 String authorId= new String();
		
		 String uniqueId_author;

		   if (authorFileName == null) {
			   authorFileName = "author" + dtf.format(now) + ".csv";
				try {
					authorFW = new FileWriter(new File(authorFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("PaperId");
					sb.append(',');
					sb.append("AuthorId");
					sb.append(',');
					sb.append("Author Name");
					authorFW.write(System.getProperty("line.separator"));
					authorFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   
		   JsonObject root = new JsonParser().parse(jsonArray).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		for(JsonElement json:jsonarray){
	   			
	   			JsonObject author = json.getAsJsonObject();
	   			JsonArray authorarray = author.getAsJsonArray("AA");
		   		
			   	for(JsonElement author_json:authorarray){
			   			authorName=author_json.getAsJsonObject().get("AuN").toString().replaceAll("^\"|\"$", "");
			   			authorId=author_json.getAsJsonObject().get("AuId").toString().replaceAll("^\"|\"$", "");
			   			//authorNames=authorNames+",";
			   		
			   	//authorNames=authorNames.substring(0,authorNames.length()-1);
	   			try {
	   				uniqueId_author=getUniqueId();		
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_author);
			   		sb.append(',');
		   			sb.append(fromPaperId);
			   		sb.append(',');
			   		sb.append(authorId);
			   		sb.append(',');
			   		sb.append(authorName);
			   		authorFW.write(System.getProperty("line.separator"));
			   		authorFW.write(sb.toString());						
					writeToUpstream(operation.CREATE, subject.AUTHORS, uniqueId_author, author);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}	
		   }
	 }
	 
	 public void writeToUpstream(operation operation_value, subject sub_value, String uniqueId, JsonObject jsonObj){	  
		 sb = new StringBuilder();			
		   if (UpstreamFileName == null) {
			   UpstreamFileName = "upstream" + dtf.format(now) + ".csv";
				try {
					upstreamFW = new FileWriter(new File(UpstreamFileName), true);
					sb.append("TimeStamp");
					sb.append(',');
					sb.append("Operation");
					sb.append(',');
					sb.append("Subject");
					sb.append(',');
					sb.append("Target");
					sb.append(',');
					sb.append("Details");
					upstreamFW.write(System.getProperty("line.separator"));
					upstreamFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}		
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((counter++)%99));
			   		sb.append(',');
		   			sb.append(operation_value);
			   		sb.append(',');
			   		sb.append(sub_value);
			   		sb.append(',');
			   		sb.append(uniqueId);
			   		sb.append(',');
			   		sb.append(jsonObj);
			   		try {
						upstreamFW.write(System.getProperty("line.separator"));
				   		upstreamFW.write(sb.toString());	
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
								
						
					} 
		    
	 public enum operation{
		 CREATE,
		 UPDATE,
		 DELETE;
	 }
	 
	 public enum subject{
		 PAPERS,
		 CITED_BY,
		 AUTHORS;
	 }
}