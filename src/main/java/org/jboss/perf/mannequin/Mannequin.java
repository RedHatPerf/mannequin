package org.jboss.perf.mannequin;

import java.math.BigInteger;
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
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);

   private static final Logger log = LoggerFactory.getLogger(Mannequin.class);

   private int port = Integer.getInteger("mannequin.port", 8080);
   private static LongAdder inflight = new LongAdder();

   @Override
   public void start(Future<Void> startFuture) {
      Router router = Router.router(vertx);
      router.route(HttpMethod.GET, "/inflight").handler(ctx -> {
         ctx.response().end(inflight.longValue() + "\n");
      });
      router.route(HttpMethod.GET, "/").handler(ctx -> {
         HttpServerResponse response = ctx.response();
         response.putHeader(HttpHeaderNames.SERVER, "Vert.x");
         response.setStatusCode(HttpResponseStatus.NO_CONTENT.code()).end();
      });
      router.route(HttpMethod.POST, "/").handler(BodyHandler.create()).handler(ctx -> {
         HttpServerResponse response = ctx.response();
         response.setStatusCode(HttpResponseStatus.OK.code()).end(ctx.getBody());
      });
      // Adjust worker pool size using -Dvertx.options.workerPoolSize=xxx
      // See https://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
      router.route(HttpMethod.GET, "/mersenneprime").handler(ctx -> {
         String pStr = ctx.request().getParam("p");
         int p;
         try {
            p = Integer.parseInt(pStr);
            inflight.increment();
            vertx.executeBlocking(future -> {
               BigInteger s = FOUR;
               BigInteger M = BigInteger.valueOf(2).pow(p).add(MINUS_ONE);
               for (int i = 0; i < p - 2; ++i) {
                  s = s.multiply(s).add(MINUS_TWO).mod(M);
               }
               future.complete(s.compareTo(BigInteger.ZERO) == 0);
            }, false, result -> {
               inflight.decrement();
               if (ctx.response().ended()) {
                  // connection has been closed before we calculated the result
                  return;
               }
               if (result.succeeded()) {
                  ctx.response().end(String.valueOf(result.result()));
               } else {
                  ctx.response().setStatusCode(500).end();
               }
            });
         } catch (NumberFormatException e) {
            ctx.response().setStatusCode(400).end();
         }
      });
      vertx.createHttpServer().requestHandler(router::handle).listen(port, result -> {
         if (result.failed()) {
            System.err.printf("Cannot listen on port %d%n", port);
            vertx.close();
         } else {
            HttpServer server = result.result();
            System.out.printf("Mannequin listening on port %d%n", server.actualPort());
         }
      });
   }
}
