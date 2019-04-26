package org.jboss.perf.mannequin;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetClient;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Path("/")
public class Service {

   private static final int NET_PORT = Integer.getInteger("mannequin.netPort",5432);
   private final String name = System.getenv("NAME") == null ? "<unknown>" : System.getenv("NAME");
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);

   @Inject
   Vertx vertx;

   private WebClient webClient;
   private NetClient netClient;
   private Buffer savedBuffer;

   @PostConstruct
   void initialize() {
      this.webClient = WebClient.create(vertx,new WebClientOptions().setFollowRedirects(true));
      this.netClient = vertx.createNetClient();
      this.savedBuffer = Buffer.buffer();
      for(int i=0; i<10_000; i++){
         savedBuffer.appendInt(i);
      }
      vertx.createNetServer().connectHandler(netSocket -> {
         netSocket.handler(buffer->{
            int limit = buffer.length();
            for(int i=0; i<limit; i++){
               netSocket.write(savedBuffer);
            }
         });
      }).listen(NET_PORT,result->{
         if(result.failed()){
            System.err.printf("Cannot listen on port %d%n",NET_PORT);
         }else{
            System.out.printf("Net Server listening on port %d%n",NET_PORT);
         }
      });
   }

   @GET
   @Path("/name")
   public String name(){
      return name;
   }

   @GET
   @Path("/batch")
   public CompletionStage<JsonArray> batchMersennePrime(@QueryParam("host") String host,@QueryParam("port") int port,@QueryParam("p") List<Integer> p){
      CompletableFuture<JsonArray> rtrn = new CompletableFuture<>();
      int clientPort = port == 0 ? 8080 : port;
      String clientHost = host == null || host.isEmpty() ? "localhost": host;
      CompositeFuture.all(p.stream().map(number->{
         Future f = Future.future();
         webClient.get(clientPort,clientHost,"http://"+clientHost+":"+clientPort+"/prime?p="+number)
         .send(f);
         return f;
      }).collect(Collectors.toList())).setHandler(asyncResult->{
         if(asyncResult.succeeded()){
            List list = asyncResult.result().list();
            JsonArray json = new JsonArray();
            asyncResult.result().list().forEach(v -> {
               if (v instanceof HttpResponse) {
                  HttpResponse httpResponse = (HttpResponse) v;
                  json.add(httpResponse.bodyAsJsonObject());
               } else {
               }
            });
            rtrn.complete(json);
         }
      });
      return rtrn;
   }

   @GET
   @Path("/prime")
   public JsonObject mersennePrime(@QueryParam("p") int p){
      JsonObject json = new JsonObject();
      json.put("startMillis", System.currentTimeMillis());
      JsonArray steps = new JsonArray();
      json.put("input",p);
      BigInteger s = FOUR;
      steps.add(s.toString());
      BigInteger M = BigInteger.valueOf(2).pow(p).add(MINUS_ONE);
      json.put("M",M);

      for (int i = 0; i < p - 2; ++i) {
         s = s.multiply(s).add(MINUS_TWO).mod(M);
         steps.add(s.toString());
      }
      //to ensure responses cannot be memoized
      json.put("stopMillis", System.currentTimeMillis());
      json.put("steps", steps);
      json.put("result", s.compareTo(BigInteger.ZERO) == 0);
      return json;
   }

   @GET
   @Path("/db")
   public CompletionStage<String> db(@QueryParam("size") int size,@QueryParam("host") String host,@QueryParam("port") int port){
      CompletableFuture<String> rtrn = new CompletableFuture<>();
      if(port == 0){
         port = NET_PORT;
      }
      if(host == null || host.isEmpty()){
         host = "localhost";
      }
      int newSize = size <= 0 ? 10 : size;
      long expected = savedBuffer.length() * newSize;
      LongAdder adder = new LongAdder();
      netClient.connect(port,host,result->{
         if(result.failed()){
            rtrn.complete("{\"failed:\":\""+result.cause().getMessage()+"\"");
            return;
         }
         NetSocket netSocket = result.result();
         netSocket.handler(buffer->{
            adder.add(buffer.length());
            if(adder.longValue() >= expected){
               rtrn.complete("{\"sent\":"+newSize+",\"received\":"+adder.longValue()+"}");
            }
         });
         netSocket.write(Buffer.buffer(new byte[newSize]));
      });
      return rtrn;
   }


   @GET
   @Path("/proxy")
   public CompletionStage<String> proxy(@QueryParam("url") List<String> urls){
      CompletableFuture<String> rtrn = new CompletableFuture<>();
      CompositeFuture.all(urls.stream().map(urlString->{
         try{
            URL url = new URL(urlString);
            return url;
         }catch (MalformedURLException e){
            return null;
         }
      }).filter(v->v!=null).map(url->{
         Future f = Future.future();
         webClient.get(url.getPort() < 0 ? url.getDefaultPort() : url.getPort(), url.getHost(), url.toString())
            .send(f);
         return f;
      }).collect(Collectors.toList())).setHandler(asyncResult->{
         if(asyncResult.succeeded()){
            List list = asyncResult.result().list();
            if(list.size()==1){
               if(list.get(0) instanceof HttpResponse){
                  rtrn.complete(((HttpResponse)list.get(0)).bodyAsString());
               }
            }else {
               JsonArray json = new JsonArray();
               asyncResult.result().list().forEach(v -> {
                  if (v instanceof HttpResponse) {
                     HttpResponse httpResponse = (HttpResponse) v;
                     json.add(httpResponse.bodyAsString());
                  } else {
                  }
               });
               rtrn.complete(json.toString());
            }
         }
      });
      return rtrn;
   }
}
