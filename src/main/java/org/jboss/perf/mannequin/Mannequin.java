package org.jboss.perf.mannequin;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.BodyHandler;

public class Mannequin extends AbstractVerticle {
   private static final Logger log = LoggerFactory.getLogger(Mannequin.class);

   private static final int HTTP_PORT = Integer.getInteger("mannequin.port", 8080);
   private static final int NET_PORT = Integer.getInteger("mannequin.netPort",5432);
   private static final String NAME;
   public static final String X_PROXIED_BY = "x-proxied-by";

   private static LongAdder inflight = new LongAdder();
   private static BusyThreads busyThreads = new BusyThreads();

   private WebClient client;
   private NetClient tcpClient;
   private Buffer savedBuffer;

   static {
      String name = System.getenv("NAME");
      NAME = name == null ? "<unknown>" : name;
   }

   @Override
   public void start(Future<Void> startFuture) {
      client = WebClient.create(vertx, new WebClientOptions().setFollowRedirects(false));
      tcpClient = vertx.createNetClient();

      savedBuffer = Buffer.buffer(10000);
      for(int i=0; i<10_000; i++) {
         savedBuffer.appendInt(i);
      }

      Router router = Router.router(vertx);
      router.route(HttpMethod.GET, "/").handler(this::handleRootGet);
      router.route(HttpMethod.POST, "/").handler(BodyHandler.create()).handler(this::handleRootPost);
      router.route(HttpMethod.GET, "/name").handler(ctx -> ctx.response().end(NAME + "\n"));
      // Adjust worker pool size using -Dvertx.options.workerPoolSize=xxx
      router.route(HttpMethod.GET, "/mersenneprime").handler(this::handleMersennePrime);
      router.route(HttpMethod.GET, "/inflight").handler(ctx -> ctx.response().end(inflight.longValue() + "\n"));
      router.route(HttpMethod.GET, "/busy/:busy").handler(this::handleBusy);
      router.route(HttpMethod.GET, "/proxy").handler(this::handleProxy);
      router.route(HttpMethod.POST, "/proxy").handler(BodyHandler.create()).handler(this::handleProxy);
      router.route(HttpMethod.PUT, "/proxy").handler(BodyHandler.create()).handler(this::handleProxy);
      router.route(HttpMethod.GET, "/env").handler(this::handleEnv);
      router.route(HttpMethod.GET, "/db").handler(this::handleDb);
      router.route(HttpMethod.POST, "/printrequest").handler(BodyHandler.create()).handler(this::handlePrintRequest);


      vertx.createHttpServer().requestHandler(router::handle).listen(HTTP_PORT, result -> {
         if (result.failed()) {
            System.err.printf("Cannot listen on port %d%n", HTTP_PORT);
            vertx.close();
         } else {
            HttpServer server = result.result();
            System.out.printf("Mannequin http server listening on port %d%n", server.actualPort());
         }
      });
      vertx.createNetServer().connectHandler(netSocket->{
         netSocket.handler(buffer->{
            int limit = buffer.length();
            for(int i=0; i < limit; i++){
               netSocket.write(savedBuffer);
            }
         });
      }).listen(NET_PORT,result->{
         if(result.failed()){
            System.err.printf("Cannot listen on port %d%n",NET_PORT);
            vertx.close();
         }else{
            NetServer server = result.result();
            System.out.printf("Mannequin net server listening on port %d%n",server.actualPort());
         }
      });
   }

   private synchronized void handlePrintRequest(RoutingContext ctx) {
      SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.S");
      System.out.printf("%s request from %s to %s%n", format.format(new Date()), ctx.request().remoteAddress(), ctx.request().uri());
      for (Map.Entry<String, String> header : ctx.request().headers()) {
         System.out.printf("%s: %s%n", header.getKey(), header.getValue());
      }
      System.out.println();
      System.out.println(ctx.getBodyAsString());
      ctx.response().end();
   }

   private String getString(String param,RoutingContext ctx,String defaultValue){
      String rtrn = defaultValue;
      List<String> values = ctx.queryParam(param);
      if(values != null && values.size()>0){
         rtrn = values.get(0);
      }
      return rtrn;
   }

   private int getInt(String param,RoutingContext ctx,int defaultValue){
      int rtrn = defaultValue;
      List<String> values = ctx.queryParam(param);
      if (values != null && values.size() > 0) {
         try {
            rtrn = Integer.parseInt(values.get(0));
         } catch(NumberFormatException e) {
            log.trace("Invalid {}: {} is not a number", param,values.get(0));
         }
      }
      return rtrn;
   }

   private void handleDb(RoutingContext ctx) {
      int size = getInt("size", ctx, 10);
      String host = getString("host", ctx, "localhost");
      int port = getInt("port", ctx, 5432);
      long expected = savedBuffer.length() * size;
      AtomicLong adder = new AtomicLong();
      tcpClient.connect(port, host, result -> {
         if (result.failed()) {
            log.trace("Connection failed", result.cause());
            ctx.response().setStatusCode(504).end(result.cause().toString());
            return;
         }
         log.trace("Connection succeeded");
         long timerId = vertx.setTimer(15_000, timer -> {
            if (!ctx.response().ended()) {
               ctx.response().setStatusCode(504).end("Received " + adder.longValue() + "/" + expected);
            }
         });
         NetSocket netSocket = result.result();
         netSocket.handler((buffer) -> {
            long total = adder.addAndGet(buffer.length());
            log.trace("Received {} bytes ({} total) from TCP socket", buffer.length(), total);
            if (total >= expected) {
               executeMersennePrime(ctx, ignored -> {
                  vertx.cancelTimer(timerId);
                  log.trace("Completing request");
                  ctx.response().setStatusCode(HttpResponseStatus.OK.code()).end("{\"sent\":" + size + ",\"received\":" + adder.longValue() + "}");
               });
            }
         });
         netSocket.write(Buffer.buffer(new byte[size]));
      });
   }

   private void handleRootGet(RoutingContext ctx) {
      HttpServerResponse response = ctx.response();
      response.putHeader(HttpHeaderNames.SERVER, "Vert.x");
      response.setStatusCode(HttpResponseStatus.NO_CONTENT.code()).end();
   }

   private void handleRootPost(RoutingContext ctx) {
      HttpServerResponse response = ctx.response();
      response.setStatusCode(HttpResponseStatus.OK.code()).end(ctx.getBody());
   }

   private void executeMersennePrime(int number, Consumer<String> callback){
         inflight.increment();
         vertx.executeBlocking(future -> future.complete(Computation.isMersennePrime(number)), false, result -> {
            inflight.decrement();
            if(callback != null) {
               callback.accept(String.valueOf(result.result()));
            }
         });
   }

   private void executeMersennePrime(RoutingContext ctx, Consumer<String> callback){
      String pStr = ctx.request().getParam("p");
      if (pStr == null) {
         callback.accept(null);
         return;
      }
      int p;
      try {
         p = Integer.parseInt(pStr);
         executeMersennePrime(p, callback);
      } catch (NumberFormatException e) {
         log.error("{} failed to parse parameter 'p': {}", ctx.request().uri(), pStr);
      }
   }

   private void handleMersennePrime(RoutingContext ctx) {
      String pStr = ctx.request().getParam("p");
      int p;
      try {
         p = Integer.parseInt(pStr);
         executeMersennePrime(p,result->{
            if (ctx.response().ended()) {
               // connection has been closed before we calculated the result
               return;
            }
            if (result != null && result.length()>0) {
               ctx.response().end(result);
            } else {
               ctx.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
            }
         });
      } catch (NumberFormatException e) {
         ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
      }
   }

   private void handleBusy(RoutingContext ctx) {
      try {
         int busy = Integer.valueOf(ctx.pathParam("busy"));
         if (busy < 0) {
            ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return;
         }
         busyThreads.set(busy);
         ctx.response().setStatusCode(HttpResponseStatus.OK.code()).end();
      } catch (NumberFormatException e) {
         ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end("Cannot parse: " + ctx.pathParam("busy"));
      }
   }

   private void handleProxy(RoutingContext ctx) {
      List<String> urls = ctx.queryParam("url");
      URL url = validateUrl(ctx, urls);
      if (url == null) {

         return;
      }

      int port = url.getPort() < 0 ? url.getDefaultPort() : url.getPort();
      log.trace("Proxying {} call to {}:{} {}", ctx.request().method(), url.getHost(), port, urls.get(0));
      HttpRequest<Buffer> request = client.request(ctx.request().method(), port, url.getHost(), url.getFile());
      copyRequestHeaders(ctx.request().headers(), request.headers());
      request.sendBuffer(ctx.getBody(), result -> handleReply(result, ctx));
   }

   private void handleReply(AsyncResult<HttpResponse<Buffer>> result, RoutingContext ctx) {
      HttpServerResponse myResponse = ctx.response();
      if (result.succeeded()) {
         log.trace("Proxy call returned {}: ", result.result().statusCode(), result.result().statusMessage());

         for (Map.Entry<String, String> header : result.result().headers()) {
            myResponse.headers().add(header.getKey(), header.getValue());
         }
         myResponse.headers().add(X_PROXIED_BY, NAME);
         Buffer body = result.result().body();
         myResponse
               .setStatusCode(result.result().statusCode())
               .setStatusMessage(result.result().statusMessage());

         executeMersennePrime(ctx, ignored -> {
            if (body != null) {
               myResponse.end(body);
            } else {
               myResponse.end();
            }
         });
      } else {
         log.trace("Proxy call failed", result.cause());
         myResponse.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code())
               .end(result.cause().toString());
      }
   }

   private void copyRequestHeaders(MultiMap serverHeaders, MultiMap clientHeaders) {
      for (Map.Entry<String, String> header : serverHeaders) {
         if (!"host".equalsIgnoreCase(header.getKey())) {
            clientHeaders.add(header.getKey(), header.getValue());
         }
      }
   }

   private URL validateUrl(RoutingContext ctx, List<String> urls) {
      if (urls.isEmpty() || urls.size() > 1) {
         ctx.response()
               .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
               .setStatusMessage("Single URL required.").end();
      }
      URL url;
      try {
         url = new URL(urls.get(0));
      } catch (MalformedURLException e) {
         log.error("Error parsing {}\n  {}",urls.get(0),e.getMessage());
         ctx.response()
               .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
               .setStatusMessage("Invalid URL").end();
         return null;
      }
      return url;
   }

   private void handleEnv(RoutingContext ctx) {
      StringBuilder sb = new StringBuilder();
      for (String var : ctx.queryParam("var")) {
         String value = System.getenv(var);
         sb.append(var).append(": ").append(value == null ? "<undefined>" : value).append('\n');
      }
      ctx.response().end(sb.toString());
   }
}
