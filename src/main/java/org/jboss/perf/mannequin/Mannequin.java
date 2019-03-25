package org.jboss.perf.mannequin;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

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
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.BodyHandler;

public class Mannequin extends AbstractVerticle {
   private static final Logger log = LoggerFactory.getLogger(Mannequin.class);

   private static final int PORT = Integer.getInteger("mannequin.port", 8080);
   private static final String NAME;
   public static final String X_PROXIED_BY = "x-proxied-by";

   private static LongAdder inflight = new LongAdder();
   private static BusyThreads busyThreads = new BusyThreads();

   private WebClient client;

   static {
      String name = System.getenv("NAME");
      NAME = name == null ? "<unknown>" : name;
   }

   @Override
   public void start(Future<Void> startFuture) {
      client = WebClient.create(vertx, new WebClientOptions().setFollowRedirects(false));

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

      vertx.createHttpServer().requestHandler(router::handle).listen(PORT, result -> {
         if (result.failed()) {
            System.err.printf("Cannot listen on port %d%n", PORT);
            vertx.close();
         } else {
            HttpServer server = result.result();
            System.out.printf("Mannequin listening on port %d%n", server.actualPort());
         }
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

   private void handleMersennePrime(RoutingContext ctx) {
      String pStr = ctx.request().getParam("p");
      int p;
      try {
         p = Integer.parseInt(pStr);
         inflight.increment();
         vertx.executeBlocking(future -> future.complete(Computation.isMersennePrime(p)), false, result -> {
            inflight.decrement();
            if (ctx.response().ended()) {
               // connection has been closed before we calculated the result
               return;
            }
            if (result.succeeded()) {
               ctx.response().end(String.valueOf(result.result()));
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
      if (url == null) return;
      int port = url.getPort() < 0 ? url.getDefaultPort() : url.getPort();
      log.trace("Proxying {} call to {}:{} {}", ctx.request().method(), url.getHost(), port, urls.get(0));
      HttpRequest<Buffer> request = client.request(ctx.request().method(), port, url.getHost(), urls.get(0));
      copyRequestHeaders(ctx.request().headers(), request.headers());
      request.sendBuffer(ctx.getBody(), result -> handleReply(result, ctx.response()));
   }

   private void handleReply(AsyncResult<HttpResponse<Buffer>> result, HttpServerResponse myResponse) {
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
         if (body != null) {
            myResponse.end(body);
         } else {
            myResponse.end();
         }
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
