package org.jboss.perf.mannequin;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetClient;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.logging.Logger;

@Path("/")
public class Service {
   private static final Logger log = Logger.getLogger(Service.class);
   private static final boolean trace = log.isTraceEnabled();

   // HTTP port set by quarkus.http.port property
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);
   private static final String X_PROXIED_BY = "x-proxied-by";
   private static final String X_PROXY_SERVICE_TIME = "x-proxy-service-time";
   private static final String X_DB_SERVICE_TIME = "x-db-service-time";

   @Inject
   Vertx vertx;

   private String name;
   private int netPort;
   private WebClient http1xClient;
   private WebClient http2Client;
   private NetClient tcpClient;
   private Map<String, SimplePool> tcpConnectionPools = new HashMap<>();
   private Buffer savedBuffer;
   private BusyThreads busyThreads = new BusyThreads();

   private static int getIntFromEnv(String var, int def) {
      String env = System.getenv(var);
      if (env == null || env.isEmpty()) return def;
      return Integer.parseInt(env);
   }

   @PostConstruct
   void initialize() {
      int maxConnections = getIntFromEnv("MAX_CONNECTIONS", 20);
      WebClientOptions options = new WebClientOptions().setFollowRedirects(true).setMaxPoolSize(maxConnections);
      http1xClient = WebClient.create(vertx, new WebClientOptions(options).setProtocolVersion(HttpVersion.HTTP_1_1));
      http2Client = WebClient.create(vertx, new WebClientOptions(options).setProtocolVersion(HttpVersion.HTTP_2));
      tcpClient = vertx.createNetClient();

      savedBuffer = Buffer.buffer(40_000);
      for (int i = 0; i < 10_000; i++){
         savedBuffer.appendInt(i);
      }

      netPort = Integer.getInteger("mannequin.netPort", 5432);
      vertx.createNetServer().connectHandler(netSocket -> {
         if (trace) {
            log.tracef("Incoming connection from to %s", netSocket.remoteAddress());
            netSocket.closeHandler(nil -> log.tracef("Connection from %s closed.", netSocket.remoteAddress()));
         }
         AtomicReference<Buffer> queryHolder = new AtomicReference<>(Buffer.buffer());
         netSocket.handler(buffer -> {
            if (trace) {
               log.tracef("%s sent %d bytes", netSocket.remoteAddress(), buffer.length());
            }
            Buffer query = queryHolder.get();
            query.appendBuffer(buffer);
            if (query.length() >= 8) {
               int querySize = query.getInt(0);
               int responseSize = query.getInt(4);
               if (query.length() >= querySize) {
                  for (int bytesToSend = responseSize; bytesToSend > 0; bytesToSend -= savedBuffer.length()) {
                     if (bytesToSend > savedBuffer.length()) {
                        if (trace) {
                           log.tracef("Written full buffer");
                        }
                        netSocket.write(savedBuffer);
                     } else {
                        if (trace) {
                           log.tracef("Written %d bytes", bytesToSend);
                        }
                        netSocket.write(savedBuffer.slice(0, bytesToSend));
                     }
                  }
                  if (trace) {
                     log.tracef("Responded %d bytes to %s", responseSize, netSocket.remoteAddress());
                  }
                  if (query.length() == querySize) {
                     queryHolder.set(Buffer.buffer());
                  } else {
                     log.warnf("Request longer (%d) than expected (%d)?", query.length(), querySize);
                     queryHolder.set(query.slice(querySize, query.length()));
                  }
               }
            }
         });
      }).listen(netPort, result->{
         if (result.failed()){
            System.err.printf("Cannot listen on port %d%n",netPort);
            vertx.close();
         } else {
            NetServer server = result.result();
            System.out.printf("Mannequin net server listening on port %d%n",server.actualPort());
         }
      });
   }

   @GET
   @Path("/")
   public Response rootGet() {
      return Response.status(Response.Status.NO_CONTENT).header("server", "Vert.x").build();
   }

   @POST
   @Path("/")
   public Response rootPost(Object body) {
      return Response.ok().entity(body).build();
   }

   @GET
   @Path("/name")
   public String name(){
      if (name == null) {
         name = System.getenv("NAME");
         name = name == null ? "<unknown>" : name;
      }
      return name;
   }

   @GET
   @Path("/batch")
   public CompletionStage<JsonArray> batchMersennePrime(
         @QueryParam("host") String host,
         @QueryParam("port") int port,
         @QueryParam("p") List<Integer> p) {
      CompletableFuture<JsonArray> rtrn = new CompletableFuture<>();
      int clientPort = port == 0 ? 8080 : port;
      String clientHost = host == null || host.isEmpty() ? "localhost": host;
      ArrayList<Future> futures = new ArrayList<>();
      for (int number : p) {
         Promise promise = Promise.promise();
         http1xClient.get(clientPort, clientHost, "http://" + clientHost + ":" + clientPort + "/prime?p=" + number)
               .send(promise);
         futures.add(promise.future());
      }
      CompositeFuture.all(futures).setHandler(asyncResult->{
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
   @Path("/mersennePrime")
   public CompletionStage<JsonObject> mersennePrime(@QueryParam("p") int p,
                                   @QueryParam("addSteps") @DefaultValue("false") boolean addSteps){
      CompletableFuture<JsonObject> future = new CompletableFuture<>();
      vertx.<JsonObject>executeBlocking(f -> f.complete(Computation.isMersennePrime(p, addSteps)), result -> {
         if (result.succeeded()) {
            future.complete(result.result());
         } else {
            future.completeExceptionally(result.cause());
         }
      });
      return future;
   }

   @GET
   @Path("/db")
   public CompletionStage<Response> db(@HeaderParam("user-agent") String userAgent,
                                       @QueryParam("querySize") @DefaultValue("100") int querySize,
                                       @QueryParam("resultSize") @DefaultValue("1000") int resultSize,
                                       @QueryParam("host") @DefaultValue("localhost") String host,
                                       @QueryParam("port") @DefaultValue("0") int port,
                                       @QueryParam("p") int p){
      CompletableFuture<Response> future = new CompletableFuture<>();
      if (trace) {
         // TODO: log remote IP - how?
         log.tracef("Handling request from %s", userAgent);
      }
      if (port == 0) {
         port = netPort;
      }

      String authority = host + ":" + port;
      SimplePool pool = tcpConnectionPools.computeIfAbsent(authority, a -> new SimplePool());
      NetSocket netSocket = pool.connections.poll();
      long startTime = System.nanoTime();
      if (netSocket != null) {
         log.tracef("%s Reusing connection %s", userAgent, netSocket.localAddress());
         sendDbRequest(userAgent, tcpConnectionPools.computeIfAbsent(authority, h -> new SimplePool()), querySize, resultSize, startTime, netSocket, p, future);
      } else {
         pool.created++;
         tcpClient.connect(port, host, result -> {
            if (result.failed()) {
               log.tracef("%s Connection failed", result.cause(), userAgent);
               if (!future.isDone()) {
                  future.complete(Response.status(504).entity(result.cause().toString()).build());
               }
               return;
            }
            log.tracef("%s Connection succeeded", userAgent);
            NetSocket netSocket2 = result.result();
            netSocket2.exceptionHandler(t -> {
               log.tracef("%s error", userAgent);
               netSocket2.close();
            });
            sendDbRequest(userAgent, pool, querySize, resultSize, startTime, netSocket2, p, future);
         });
      }
      return future;
   }

   private void sendDbRequest(String userAgent, SimplePool pool, int querySize, int resultSize, long startTime, NetSocket netSocket, int p, CompletableFuture<Response> future) {
      AtomicLong adder = new AtomicLong();
      if (trace) {
         log.tracef("%s Got request %d -> %d, using %s", userAgent, querySize, resultSize, netSocket.localAddress());
      }
      long timerId = vertx.setTimer(15_000, timer -> {
         log.tracef("%s timed out, %d/%d bytes", userAgent, adder.longValue(), resultSize);
         if (!future.isDone()) {
            future.complete(Response.status(504).header("x-db-timeout", "true")
                  .entity("Received " + adder.longValue() + "/" + resultSize).build());
         }
         netSocket.close();
      });
      netSocket.closeHandler(nil -> {
         pool.created--;
         log.tracef("Connection %s closed, response sent? %s", netSocket.localAddress(), future.isDone());
         if (!future.isDone()) {
            future.complete(Response.status(504).header("x-db-closed", "true").entity("TCP connection closed.").build());
         }
      });
      netSocket.handler(buffer -> {
         long total = adder.addAndGet(buffer.length());
         if (trace) {
            log.tracef("%s Received %d bytes (%d/%d) from TCP socket", userAgent, buffer.length(), total, resultSize);
         }
         if (total >= resultSize) {
            long endTime = System.nanoTime();
            pool.connections.push(netSocket);
            if (trace) {
               log.tracef("%s Released connection %s , %d/%d available", userAgent, netSocket.localAddress(), pool.connections.size(), pool.created);
            }
            long dbServiceTime = endTime - startTime;
            vertx.executeBlocking(f -> f.complete(Computation.isMersennePrime(p, false)), ignored -> {
               vertx.cancelTimer(timerId);
               if (trace) {
                  log.tracef("%s Completing request", userAgent);
               }
               if (!future.isDone()) {
                  future.complete(Response.ok("{\"sent\":" + querySize + ",\"received\":" + adder.longValue() + "}")
                        .header(X_DB_SERVICE_TIME, String.valueOf(dbServiceTime)).build());
               }
            });
         }
      });
      try {
         Buffer requestBuffer = Buffer.buffer(new byte[Math.max(querySize, 8)]);
         requestBuffer.setInt(0, querySize);
         requestBuffer.setInt(4, resultSize);
         netSocket.write(requestBuffer);
      } catch (Throwable t) {
         log.tracef("%s Failed writing request", userAgent);
         // this can throw error when the connection is closed from the other party
         if (!future.isDone()) {
            future.complete(Response.status(504).header("x-db-write-failed", "true").entity("TCP connection failed.").build());
         }
      }
   }

   private String userAgent(HttpHeaders headers) {
      return headers.getHeaderString("user-agent");
   }

   @GET
   @Path("/proxy")
   public CompletionStage<Response> proxy(@Context HttpHeaders headers,
                                          @QueryParam("url") List<String> urls,
                                          @QueryParam("version") @DefaultValue("http1x") String version,
                                          @QueryParam("p") @DefaultValue("0") int p) {
      WebClient httpClient = "http2".equals(version) ? http2Client : http1xClient;
      CompletableFuture<Response> future = new CompletableFuture<>();
      long startTime = System.nanoTime();
      ArrayList<Future> futures = new ArrayList<>();
      for (String urlString : urls) {
         if (trace) {
            log.tracef("Proxying request to %s", urlString);
         }
         URL url;
         try {
            url = new URL(urlString);
         } catch (MalformedURLException e){
            future.complete(Response.status(Response.Status.BAD_REQUEST.getStatusCode(), "Malformed URL: " + urlString).build());
            break;
         }
         HttpRequest<Buffer> request = httpClient.get(url.getPort() < 0 ? url.getDefaultPort() : url.getPort(), url.getHost(), url.getFile());
         for (Map.Entry<String, List<String>> entry : headers.getRequestHeaders().entrySet()) {
            String header = entry.getKey();
            if (!"host".equalsIgnoreCase(header)) {
               for (String value: entry.getValue()) {
                  request.putHeader(header, value);
               }
            }
         }
         Promise promise = Promise.promise();
         request.send(promise);
         futures.add(promise.future());
      }
      CompositeFuture.all(futures).setHandler(asyncResult -> {
         if (asyncResult.succeeded()){
            long endTime = System.nanoTime();
            Response.ResponseBuilder responseBuilder;
            List<HttpResponse> list = asyncResult.result().list();
            if (list.size() == 1){
               HttpResponse httpResponse = list.get(0);
               if (trace) {
                  log.tracef("Received response proxied invocation, status is %d", httpResponse.statusCode());
               }
               responseBuilder = Response.status(httpResponse.statusCode(), httpResponse.statusMessage());
               httpResponse.headers().forEach(entry -> {
                  // We may have received the response as chunked but unless we return StreamingOutput
                  // RESTEasy will automatically add Content-Length, ergo we have to drop this header.
                  if (!entry.getKey().equalsIgnoreCase("transfer-encoding")) {
                     responseBuilder.header(entry.getKey(), entry.getValue());
                  }
               });
               responseBuilder.entity(httpResponse.bodyAsBuffer().getBytes());
            } else {
               if (trace) {
                  log.trace("Received response for all proxied invocations");
               }
               JsonArray json = new JsonArray();
               asyncResult.result().<HttpResponse>list().forEach(httpResponse -> {
                  json.add(httpResponse.bodyAsString());
               });
               responseBuilder = Response.ok().entity(json);
            }
            responseBuilder.header(X_PROXIED_BY, name())
                  .header(X_PROXY_SERVICE_TIME, String.valueOf(endTime - startTime));
            if (p <= 0) {
               future.complete(responseBuilder.build());
            } else {
               vertx.executeBlocking(f -> f.complete(Computation.isMersennePrime(p, false)),
                     result -> future.complete(responseBuilder.build()));
            }
         } else {
            log.trace("Proxy invocation failed", asyncResult.cause());
            future.complete(Response.serverError().entity(asyncResult.cause()).build());
         }
      });
      return future;
   }

   @GET
   @Path("/busy/{busy}")
   public void busy(@PathParam("busy") int busy) {
      busyThreads.set(busy);
   }

   @GET
   @Path("/env")
   public String env(@QueryParam("env") List<String> envs) {
      StringBuilder sb = new StringBuilder();
      if (envs.isEmpty()) {
         for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
         }
      } else {
         for (String var : envs) {
            String value = System.getenv(var);
            sb.append(var).append(": ").append(value == null ? "<undefined>" : value).append('\n');
         }
      }
      return sb.toString();
   }

   private static class SimplePool {
      int created;
      Deque<NetSocket> connections = new ArrayDeque<>();
   }
}
