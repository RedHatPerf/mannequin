package org.jboss.perf.mannequin;

import java.util.concurrent.atomic.LongAdder;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class Mannequin extends AbstractVerticle {
   private static final Logger log = LoggerFactory.getLogger(Mannequin.class);

   private static final int PORT = Integer.getInteger("mannequin.port", 8080);
   private static final String NAME;

   private static LongAdder inflight = new LongAdder();
   private static BusyThreads busyThreads = new BusyThreads();

   static {
      String name = System.getenv("NAME");
      NAME = name == null ? "<unknown>" : name;
   }

   @Override
   public void start(Future<Void> startFuture) {
      Router router = Router.router(vertx);
      router.route(HttpMethod.GET, "/").handler(ctx -> {
         HttpServerResponse response = ctx.response();
         response.putHeader(HttpHeaderNames.SERVER, "Vert.x");
         response.setStatusCode(HttpResponseStatus.NO_CONTENT.code()).end();
      });
      router.route(HttpMethod.POST, "/").handler(BodyHandler.create()).handler(ctx -> {
         HttpServerResponse response = ctx.response();
         response.setStatusCode(HttpResponseStatus.OK.code()).end(ctx.getBody());
      });
      router.route(HttpMethod.GET, "/name").handler(ctx -> ctx.response().end(NAME + "\n"));
      // Adjust worker pool size using -Dvertx.options.workerPoolSize=xxx
      router.route(HttpMethod.GET, "/mersenneprime").handler(ctx -> {
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
      });
      router.route(HttpMethod.GET, "/inflight").handler(ctx -> {
         ctx.response().end(inflight.longValue() + "\n");
      });
      router.route(HttpMethod.GET, "/busy/:busy").handler(ctx -> {
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
      });

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
}
