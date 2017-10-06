// // This sample uses the Apache HTTP client from HTTP Components (http://hc.apache.org/httpcomponents-client-ga/)
package edu.ovgu.coddsgraph.CoddsGraph;

import java.io.BufferedReader;
import java.io.File;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
	private String backUpFileName;
	private String idsToVisitFN;
	private String dummyFN;
	private String subscriptionKeyMaintainingFN;
	private String idsToVisitCurrentHopFN;
	private String idsToVisitNextHopFN;
	private String idsVisitedFN;
	private  StringBuilder sb;
	DateTimeFormatter uniqueId_ts = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
	DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");
	private static FileWriter vertexFW;
	private static FileWriter edgeFW;
	private static FileWriter upstreamFW;
	private static FileWriter authorFW;
	private static FileWriter idsToVisitFW;
	private FileWriter idsToVisitCurrentHopFW;
	private FileWriter idsToVisitNextHopFW;
	private FileWriter idsVisitedFW;
	private FileWriter subscriptionKeyMaintainingFW;
	private FileWriter dummyFW;
	private static int TOTAL_ID_COUNT_TOQUERY=100; //change it to 100
	private int uniqueIdCounter=0;
	private static String[] subscriptionKeys={"dbe029f01ce145f5a41390c981f3bfc5","2","3"};
	private static String subKey=subscriptionKeys[0];
	private int posSKey=0;
	private static int NUM_HOPS;
	private static int TOTAL_HOPS=2;
	private static Map<String, Integer> idsToVisitofCurrentHop;
	private static Map<String, Integer> idsToVisitofNextHop;
	private static int subscriptionKeyLimit;
	private static Map<String, Integer> idsVisited;
	static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm");
	static LocalDateTime now = LocalDateTime.now();
	private static LocalDateTime startTime=LocalDateTime.now();
	private static String beforeFailOver;
	private static Long startTimeInMilSec;
	final static Logger logger = Logger.getLogger(CrawlAndIndex.class);

	
    public static void main(String[] args) 
    {  	
    	startTimeInMilSec = System.currentTimeMillis();	
    	beforeFailOver=args[0];
    	subscriptionKeyLimit=Integer.parseInt(args[1]);
    	NUM_HOPS=Integer.parseInt(args[2]);
    	String temp="";
    	int count=0;
    	idsToVisitofCurrentHop = new HashMap<>();
    	idsToVisitofNextHop = new HashMap<>();
        idsVisited=new HashMap<>();
    	String JSONResult_seed="";
    	String JSONResult="";	
    	
    	try {
   
    		Properties properties = new Properties();
    		properties.load(CrawlAndIndex.class.getResourceAsStream("log4j.properties"));
    		PropertyConfigurator.configure(properties);
    		
    		if(beforeFailOver.toUpperCase().equals("TRUE")){	
				JSONResult_seed= jsonreqobj.getData("And(And(Ti='a relational model of data for large shared data banks',Composite(AA.AuN=='e f codd')),Y=1970)" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN","5","0");			
		    	
				logger.debug("Indexing started..Execution before failover");
		    	
				JsonObject root = new JsonParser().parse(JSONResult_seed).getAsJsonObject();
		   		JsonArray jsonarray = root.getAsJsonArray("entities");
		   		
		   		for(JsonElement json:jsonarray){
		   			idsToVisitofCurrentHop.put(json.getAsJsonObject().get("Id").toString(), NUM_HOPS);
		   		}
		   		jsonreqobj.addIdsToList(JSONResult_seed);
		   		jsonreqobj.indexVertex(JSONResult_seed); 
		    	jsonreqobj.indexEdges(JSONResult_seed); 
	
		    	
    		}
    		else{
    			logger.debug("Indexing started..Execution after failover");
    			jsonreqobj.loadToListFromFiles("idsToVisitInCurrentHop.csv", idsToVisitofCurrentHop);
    			jsonreqobj.loadToListFromFiles("idsToVisitNextHop.csv", idsToVisitofNextHop);
    			jsonreqobj.loadToListFromFiles("idsVisited.csv", idsVisited);
    			
    			
    			if(idsToVisitofCurrentHop.isEmpty()){
    				if(!idsToVisitofNextHop.isEmpty()){
    					idsToVisitofCurrentHop.putAll(idsToVisitofNextHop);
    					idsToVisitofNextHop.clear();
    				}
    			}
    		}
    		
		  	Iterator it = idsToVisitofCurrentHop.entrySet().iterator();
		   	 
		   	while (it.hasNext()) {
		   		if(!jsonreqobj.checkForTimeOut()){
		   		Map.Entry pair = (Map.Entry)it.next();
				temp= temp+"Id="+ pair.getKey().toString()+",";
			   	count++;
			   	if (count==TOTAL_ID_COUNT_TOQUERY|| !(it.hasNext())){ 
			   		JSONResult= jsonreqobj.getData("OR("+temp.substring(0, temp.length()-1)+")" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId,J.JN,J.JId,C.CN,C.CId,S.U,VSN",Integer.toString(TOTAL_ID_COUNT_TOQUERY),"0");	   
			   		printListSizes();
			   		jsonreqobj.indexVertex(JSONResult); 
			   		jsonreqobj.indexEdges(JSONResult);		   	
				   	it = idsToVisitofCurrentHop.entrySet().iterator();	   	
				    count=0;
				    temp="";
			   	 }
		   		}
		   		else{
		   			printListSizes();
		   			jsonreqobj.backUp();
					logger.info("Transaction count during termination - "+subscriptionKeyLimit);
					logger.info("Exiting after 45 minutes");
					 exitCrawl();  
					System.exit(0);
		   		}
		    }
		} 
    	catch (Exception e1) {
			    printListSizes();
				jsonreqobj.backUp();
				e1.printStackTrace();
			}	
     exitCrawl();    	
     logger.debug("Indexing ends");

	}

	private static void printListSizes() {
		logger.debug("Current Hop list :"+ idsToVisitofCurrentHop.size());
		logger.debug("Next Hop list :"+ idsToVisitofNextHop.size());
		logger.debug("Visited list :"+ idsVisited.size());
	}

	private static void exitCrawl() {
		try {
			vertexFW.close();
	    	edgeFW.close();
	    	upstreamFW.close();
	    	authorFW.close();
	    	if(idsToVisitFW!=null){
	    	idsToVisitFW.close();
	    	}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private  String getData(String expression, String attributes, String count,String from) throws Exception {
		HttpClient httpclient = HttpClients.createDefault();       
		String JSONResult=null;


			if(subscriptionKeyCountLimitReached()){		
				jsonreqobj.backUp();
				exitCrawl();
				/*posSKey+=1;
				if(posSKey<subscriptionKeys.length+1){
					subKey=subscriptionKeys[posSKey];*/
				//}
			}else{
				subscriptionKeyLimit++;
				logger.info("Subscriprion key count - "+ subscriptionKeyLimit);
			}
		
	        try
	        {
	        	  URIBuilder builder = new URIBuilder("https://westus.api.cognitive.microsoft.com/academic/v1.0/evaluate");
	              builder.setParameter("expr", expression);
	              builder.setParameter("model", "latest");
	              builder.setParameter("attributes", attributes );
	              builder.setParameter("count", count);
	              builder.addParameter("offset", from);
	              //builder.setParameter("from", from);
	        	  logger.debug("Query - "+builder.toString());
	              URI uri = builder.build();
	              HttpGet request = new HttpGet(uri);
	              request.setHeader("Ocp-Apim-Subscription-Key",subKey ); // dbe029f01ce145f5a41390c981f3bfc5
	            // Request body
	            HttpUriRequest reqEntity = request;    
	            HttpResponse response = httpclient.execute(reqEntity);
	            HttpEntity entity = response.getEntity();
	
	            if (entity != null) 
	            {
	              JSONResult=EntityUtils.toString(entity);
	              //logger.debug(JSONResult);
	              if (dummyFN == null) {
	            	  dummyFN = "dummy" + dtf.format(now) + ".csv";
	              }
	              else{
	            	  
	            		try {
	      					dummyFW = new FileWriter(new File(dummyFN), true);
	      					dummyFW.write(System.getProperty("line.separator"));
	      					dummyFW.write(JSONResult);
	                }
	      				
	      			  catch (Exception e)
	      		        {
	      				  logger.info("Exception occured inside getData() - "+ e.getMessage());
	      		        	throw e;
	      		        }
	              }
	  			
	        }
	       
	     }
	        catch (Exception e)
	        {
	        	logger.info("Exception occured inside getData() - "+ e.getMessage());
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
			int sumCitationCount=0;
			
		   if (vertexFileName == null) {
				vertexFileName = "papers"+".csv";
				
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
		   			
		   			if(NUM_HOPS<=TOTAL_HOPS){
						RId= "RId="+json.getAsJsonObject().get("Id").toString() ;
						String citationCount=json.getAsJsonObject().get("CC").toString();
																			
						//To get all records for citations >1000
						int citedCount=Integer.parseInt(citationCount);
						int quotient;
						//int remainder;
						if (citedCount>1000){
							if(!ReferenceIds.isEmpty()&&sumCitationCount>0){
								try {
									JSONResult_edges=getData("OR("+ReferenceIds.substring(0, ReferenceIds.length()-1)+")", "Id,RId", "1000","0");
									if(idsToVisitofNextHop.size()<=12000000){
										jsonreqobj.addIdsToList(JSONResult_edges);
									}
									else{
										jsonreqobj.addIdsToFile(JSONResult_edges);
									}
									
									sumCitationCount=0;
									ReferenceIds="";
								} catch (Exception e) {
									printListSizes();
									jsonreqobj.backUp();
									e.printStackTrace();
								}
							
							}
							
							quotient=citedCount/1000; 
							//remainder=citedCount%1000;
							for(int i=0;i<=quotient;i++){				
								try {
									JSONResult_edges=getData(RId, "Id,RId", citationCount,Integer.toString(from));
									if(idsToVisitofNextHop.size()<=12000000){
										jsonreqobj.addIdsToList(JSONResult_edges);
									}
									else{
										jsonreqobj.addIdsToFile(JSONResult_edges);
									}
								} catch (Exception e) {
									printListSizes();
									jsonreqobj.backUp();
									e.printStackTrace();
								}
								from=from+1000;
							}
						}
						else{
							//make a batch of 1000 and place a call to the API
							sumCitationCount=sumCitationCount+citedCount;
							if(sumCitationCount<1000){
								ReferenceIds=ReferenceIds+ RId+",";	
							}
							else{
								try {
									JSONResult_edges=getData("OR("+ReferenceIds.substring(0, ReferenceIds.length()-1)+")", "Id,RId", "1000","0");
									if(idsToVisitofNextHop.size()<=12000000){
										jsonreqobj.addIdsToList(JSONResult_edges);
									}
									else{
										jsonreqobj.addIdsToFile(JSONResult_edges);
									}
									sumCitationCount=0;
									ReferenceIds="";
									sumCitationCount=citedCount;
									ReferenceIds=ReferenceIds+ RId+",";
								} catch (Exception e) {
									printListSizes();
									jsonreqobj.backUp();
									e.printStackTrace();
								}
								
							}
						}
		   			}		
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
							}
					} catch (IOException e) {
						logger.info("Exception occured in indexVertex() - "+e.getMessage());
						printListSizes();
						jsonreqobj.backUp();
						logger.info("BackUp done due to exception in indexVertex()");	
					}   		
			  } 
		   		logger.debug("Out of for loop, Ref Id - "+ReferenceIds);

		   		if(!ReferenceIds.isEmpty()&&sumCitationCount>0){
		   		try {
					JSONResult_edges=getData("OR("+ReferenceIds.substring(0, ReferenceIds.length()-1)+")", "Id,RId", "1000","0"); //We are putting 50k here. Will it work? //change it to 1000
					if(idsToVisitofNextHop.size()<=12000000){
						jsonreqobj.addIdsToList(JSONResult_edges);
					}
					else{
						jsonreqobj.addIdsToFile(JSONResult_edges);
					}
				} catch (Exception e) {
					logger.info("Exception occured in indexVertex() - "+e.getMessage());
					printListSizes();
					jsonreqobj.backUp();
					logger.info("BackUp done due to exception in indexVertex()");
					}	
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
				edgeFileName = "cites"+".csv";
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
	   			if(idsToVisitofNextHop.size()<=12000000){   				
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
	   			
	   	else{
	   	 if(idsToVisitFN==null){
		   		idsToVisitFN="idsToVisit"+".csv";
		    	 
		    	 try {
		    		 idsToVisitFW= new FileWriter(new File(idsToVisitFN),true);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	 
		    	 try {
	   					idsToVisitFW.write(json.getAsJsonObject().get("Id").toString());
						idsToVisitFW.write(System.getProperty("line.separator"));
					} catch (IOException e) {
						e.printStackTrace();
					}
		    	 
		 			if(json.getAsJsonObject().toString().contains("RId")){				
			   			paperToVisitIds=json.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "").split(",");			
			   			for(int i=0;i<paperToVisitIds.length;i++){   			
				   				if(NUM_HOPS<TOTAL_HOPS){
				   				
									try {
										idsToVisitFW.write(paperToVisitIds[i]);
										idsToVisitFW.write(System.getProperty("line.separator"));
									} catch (IOException e) {
										e.printStackTrace();
									}
				   				}
			   			}
			   		}
		    	
		   	 }
		 }
   	}
	   	
	 }
	 
	 

	 public void addIdsToFile(String paperIds){
		 String[] paperToVisitIds;
		  JsonObject root = new JsonParser().parse(paperIds).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		
	   	 if(idsToVisitFN==null){
	   		idsToVisitFN="idsToVisit"+".csv";
	    	 
	    	 try {
	    		 idsToVisitFW= new FileWriter(new File(idsToVisitFN),true);
			} catch (IOException e1) {
				e1.printStackTrace();
			}	
	   	 }
	   		for(JsonElement json:jsonarray){ 			
	   			if(NUM_HOPS<TOTAL_HOPS){
	   				try {
	   					idsToVisitFW.write(json.getAsJsonObject().get("Id").toString());
						idsToVisitFW.write(System.getProperty("line.separator"));
					} catch (IOException e) {
						logger.info(e.getMessage());
					}
 				
	   			}
	   			if(json.getAsJsonObject().toString().contains("RId")){				
	   			paperToVisitIds=json.getAsJsonObject().get("RId").toString().replace("[", "").replace("]", "").split(",");			
	   			for(int i=0;i<paperToVisitIds.length;i++){   			
		   				if(NUM_HOPS<TOTAL_HOPS){
		   				
							try {
								idsToVisitFW.write(paperToVisitIds[i]);
								idsToVisitFW.write(System.getProperty("line.separator"));
							} catch (IOException e) {
								logger.info(e.getMessage());
							}
		   				}
	   			}
	   		}
	   	}

	 }
	 
	 public String getUniqueId(){
		return  (uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((uniqueIdCounter++)%99));
	 }

	 public void indexAuthor(String fromPaperId,String jsonArray){
		 sb = new StringBuilder();	
		 String authorName=new String();
		 String authorId= new String();
		
		 String uniqueId_author;

		   if (authorFileName == null) {
			   authorFileName = "author"+".csv";
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
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((uniqueIdCounter++)%99));
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
	 
	 public static boolean subscriptionKeyCountLimitReached(){
			if(subscriptionKeyLimit>=200000){
				logger.info("Ran out of Money!!!, You have completed 200K transactions");
				return true;
			}
			return false;
		}
	 
	 public void backUp(){		 
		 logger.info("NUM_HOPS during Backup -" + NUM_HOPS);
		 logger.info("Subscription key count during backup - " + subscriptionKeyLimit);
		 
	     Iterator itCurrentHop = idsToVisitofCurrentHop.entrySet().iterator();   
	     if(idsToVisitCurrentHopFN==null){
	    	 idsToVisitCurrentHopFN="idsToVisitInCurrentHop"+".csv";
	    	 try {
				idsToVisitCurrentHopFW= new FileWriter(new File(idsToVisitCurrentHopFN),true);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
	    	 
	       	 while (itCurrentHop.hasNext()) {
		   		 	Map.Entry pair = (Map.Entry)itCurrentHop.next();
		   		 try {	
		   			idsToVisitCurrentHopFW.write(pair.getKey().toString()+":"+pair.getValue().toString());
		   			idsToVisitCurrentHopFW.write(System.getProperty("line.separator"));	
		   			
					} catch (Exception e) {
						logger.info("Exception during backup - "+ e.getMessage());
					}
		   	 }
	       	 
	       	try {
				idsToVisitCurrentHopFW.close();
			} catch (IOException e) {
				logger.info("Exception during backup - "+ e.getMessage());
			}
	     }
	
	     Iterator itNextHop = idsToVisitofNextHop.entrySet().iterator();   
	     if(idsToVisitNextHopFN==null){
	    	 idsToVisitNextHopFN="idsToVisitNextHop"+".csv";
	    	 
	    	 try {
	    		 idsToVisitNextHopFW= new FileWriter(new File(idsToVisitNextHopFN),true);
			} catch (IOException e1) {
				logger.info("Exception during backup - "+ e1.getMessage());
			}
	    	 
	       	 while (itNextHop.hasNext()) {
		   		 	Map.Entry pair = (Map.Entry)itNextHop.next();
		   		 try {
		   			
		   			idsToVisitNextHopFW.write(pair.getKey().toString()+":"+pair.getValue().toString());
		   			idsToVisitNextHopFW.write(System.getProperty("line.separator"));	
		   			
					} catch (Exception e) {
						logger.info("Exception during backup - "+ e.getMessage());
					}
		   	 }
	       	try {
				idsToVisitNextHopFW.close();
			} catch (IOException e) {
				logger.info("Exception during backup - "+ e.getMessage());
			}
	     }

	   	 
	     Iterator itVisited = idsVisited.entrySet().iterator();   	 
	     if(idsVisitedFN==null){
	    	 idsVisitedFN="idsVisited"+".csv";
	    	 
	    	 try {
	    		 idsVisitedFW= new FileWriter(new File(idsVisitedFN),true);
				} catch (IOException e1) {
					logger.info("Exception during backup - "+ e1.getMessage());
				}
	       	 while (itVisited.hasNext()) {
		   		 	Map.Entry pair = (Map.Entry)itVisited.next();
		   		 try {
		   			
		   			idsVisitedFW.write(pair.getKey().toString()+":"+pair.getValue().toString());
		   			idsVisitedFW.write(System.getProperty("line.separator"));		   			
					} catch (Exception e) {
						logger.info("Exception during backup - "+ e.getMessage());
					}
		   	 }
	       	 
	       	try {
				idsVisitedFW.close();
			} catch (IOException e) {
				logger.info("Exception during backup - "+ e.getMessage());
			}
	     }

	     if(subscriptionKeyMaintainingFN==null){
	    	 subscriptionKeyMaintainingFN="subscriptionKeyMaintaining"+".csv";
	    	 try {
	    		 subscriptionKeyMaintainingFW= new FileWriter(new File(subscriptionKeyMaintainingFN),true);
	    		 subscriptionKeyMaintainingFW.write("Subscription during backup - " +subscriptionKeyLimit);
	    		 subscriptionKeyMaintainingFW.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
	    	
	     }
	     
	 }
	 
	 public void loadToListFromFiles(String backUpFile, Map<String, Integer> ids){
		 String filePath = backUpFile;
		    String line;
		    BufferedReader reader;
				logger.debug("Loading of Id's from"+ backUpFile + "file started..");
		    try {
		    	reader = new BufferedReader(new FileReader(filePath));
				while ((line = reader.readLine()) != null)
				{
				    String[] parts = line.split(":", 2);
				    if (parts.length >= 2)
				    {
				        String key = parts[0];
				        int value = Integer.parseInt(parts[1]);
				        ids.put(key, value);
				    } else {
				        logger.debug("ignoring line: " + line);
				    }
				}
						
			    reader.close();
				
			} catch (IOException e) {
				logger.info("Exception during Loading to lists - "+ e.getMessage());
			}
		    logger.debug("Id's from file is loaded to the list.");
	 }
		
	 public boolean checkForTimeOut(){		 
		 long currentTimeInMilSec = System.currentTimeMillis();
		 long tDelta = currentTimeInMilSec - startTimeInMilSec;
		 long elapsedSeconds = tDelta / 1000;
		 int min =(int) (elapsedSeconds/60);
		 if(min>45){
			 logger.info("TimeOut - Reached 45 mins since the start");
			 return true;
		 }
		 return false;
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