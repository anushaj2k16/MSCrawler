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
	DateTimeFormatter uniqueId_ts = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
	DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");
	private  FileWriter vertexFW;
	private  FileWriter edgeFW;
	private  FileWriter upstreamFW;
	private  FileWriter authorFW;
	private static int TOTAL_ID_COUNT_TOQUERY=5;
	private int counter=0;
	//private Iterator it ;
	private static int NUM_HOPS=0;
	private static int TOTAL_HOPS=3;
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
    	String JSONResult_seed;
    	String JSONResult;
    	
    	JSONResult_seed= jsonreqobj.getData("And(And(Ti='a relational model of data for large shared data banks',Composite(AA.AuN=='e f codd')),Y=1970)" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN","10","0");
    	System.out.println("Indexing started");
    	JsonObject root = new JsonParser().parse(JSONResult_seed).getAsJsonObject();
   		JsonArray jsonarray = root.getAsJsonArray("entities");
   		for(JsonElement json:jsonarray){
    	idsToVisitofCurrentHop.put(json.getAsJsonObject().get("Id").toString(), NUM_HOPS);
   		}
    	jsonreqobj.indexEdges(JSONResult_seed); 	
    	jsonreqobj.indexVertex(JSONResult_seed); 		

    
        //idsToVisitCopy.putAll(idsToVisit);
	   	 Iterator it = idsToVisitofCurrentHop.entrySet().iterator();
	   	 
	   	 while (it.hasNext()) {
	   
	   	 Map.Entry pair = (Map.Entry)it.next();
		 temp= temp+"Id="+ pair.getKey().toString()+",";
	   	 count++;
	   	 if (count==TOTAL_ID_COUNT_TOQUERY|| !(it.hasNext())){ 
	   		JSONResult= jsonreqobj.getData("OR("+temp.substring(0, temp.length()-1)+")" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN","5","0");
	   		System.out.println(idsToVisitofCurrentHop.size());
	   		jsonreqobj.indexEdges(JSONResult);
		   	 jsonreqobj.indexVertex(JSONResult);
		   	 it = idsToVisitofCurrentHop.entrySet().iterator();
		   	System.out.println("After removing 5 elements"+idsToVisitofCurrentHop.size());
		     count=0;
		     temp="";
	   	 }
	    
	    }
     System.out.println("Indexing ends");
    }


	private  String getData(String expression, String attributes, String count, String from) {
		HttpClient httpclient = HttpClients.createDefault();       
		String JSONResult=null;
        try
        {
        	  URIBuilder builder = new URIBuilder("https://westus.api.cognitive.microsoft.com/academic/v1.0/evaluate");
              builder.setParameter("expr", expression);
              builder.setParameter("model", "latest");
              builder.setParameter("attributes", attributes );
              builder.setParameter("count", count);
              builder.setParameter("from", from);
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
            System.out.println(e.getMessage());
        }
        return JSONResult;
	}
     	
    
	 public void indexVertex(String jsonArray){	
			sb = new StringBuilder();
			String uniqueId_paper;
			int from=0;
			
		   if (vertexFileName == null) {
				vertexFileName = "papers" + dtf.format(now) + ".csv";
				try {
					vertexFW = new FileWriter(new File(vertexFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("PaperId");
					sb.append(',');
					sb.append("Title");
					sb.append(',');
					sb.append("PublishInYear");
					sb.append(',');
/*					sb.append("Journal Id");
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
					sb.append(',');*/
					sb.append("TimestampAdded");
					sb.append(',');
					sb.append("TimestampLastVisited");
					sb.append(',');				
					sb.append("TimestampMod");
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
			 /*  		sb.append(json.getAsJsonObject().get("J.JId"));
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
			   		sb.append(',');*/
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		vertexFW.write(System.getProperty("line.separator"));
			   		vertexFW.write(sb.toString());	
			   		
			   		//write log after every write to papers.csv file
			   		writeToUpstream(operation.CREATE, subject.PAPERS,uniqueId_paper, root);
			   		
			   		//write author details to author.csv
			   		jsonreqobj.indexAuthor(json.getAsJsonObject().get("Id").toString(), jsonArray);
			   		
						String JSONResult_edges;
						String RId= "RId="+json.getAsJsonObject().get("Id").toString() ;
						String citationCount=json.getAsJsonObject().get("CC").toString();
						JSONResult_edges=getData(RId, "Id,RId", "5",Integer.toString(from));
						//JSONResult_edges.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "");
				   	
						//add queried id  toVisited list and remove id from toVisit list
						idsVisited.put(json.getAsJsonObject().get("Id").toString(), NUM_HOPS);
						if(!idsToVisitofCurrentHop.isEmpty()){
							idsToVisitofCurrentHop.remove(json.getAsJsonObject().get("Id").toString());
						
						}
						
						
						//write edge data to cited-by.csv file
						jsonreqobj.indexEdges(JSONResult_edges);
						
						if(idsToVisitofCurrentHop.size()>0){
							//do nothing
						}else{
							NUM_HOPS++;
							idsToVisitofCurrentHop.putAll(idsToVisitofNextHop);
							idsToVisitofNextHop.clear();
						}
						
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
				edgeFileName = "cited_by" + dtf.format(now) + ".csv";
				try {
					edgeFW = new FileWriter(new File(edgeFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("FromPaperId");
					sb.append(',');
					sb.append("CitedbyPaperId");
					sb.append(',');
					sb.append("TimeStamp");
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
	   			
	   			if(NUM_HOPS<TOTAL_HOPS){
   					if(!(idsToVisitofCurrentHop.containsKey(json.getAsJsonObject().get("Id").toString()))&&
   							!(idsVisited.containsKey(json.getAsJsonObject().get("Id").toString()))&&
   							!(idsToVisitofNextHop.containsKey(json.getAsJsonObject().get("Id").toString()))){
   						idsToVisitofNextHop.put( json.getAsJsonObject().get("Id"),NUM_HOPS+1);
   					}
	   			}
	   			if(json.getAsJsonObject().toString().contains("RId")){				
	   			referenceIds=json.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "").split(",");			
	   			for(int i=0;i<referenceIds.length;i++){
	   				try {
		   				if(NUM_HOPS<TOTAL_HOPS){
		   					if(!(idsToVisitofCurrentHop.containsKey(referenceIds[i]))&&
		   							!(idsVisited.containsKey(referenceIds[i]))&&
		   							!(idsToVisitofNextHop.containsKey(referenceIds[i]))){
		   						idsToVisitofNextHop.put( referenceIds[i],NUM_HOPS+1);
		   					}
			   				
		   				}
		   				uniqueId_citedby=getUniqueId();
			   			sb=new StringBuilder();
			   			sb.append(uniqueId_citedby);
				   		sb.append(',');
			   			sb.append(referenceIds[i]);
				   		sb.append(',');
				   		sb.append(json.getAsJsonObject().get("Id"));
				   		sb.append(',');
				   	
				   		DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				   		LocalDateTime now = LocalDateTime.now();
				   		sb.append(dtf_ts.format(now));
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