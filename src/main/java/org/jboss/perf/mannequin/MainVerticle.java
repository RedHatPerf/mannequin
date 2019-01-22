package org.jboss.perf.mannequin;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

public class MainVerticle extends AbstractVerticle {
   private final String instances = System.getProperty("mannequin.instances", "1");

   @Override
   public void start() {
      DeploymentOptions options = new DeploymentOptions().setInstances(Integer.parseInt(instances));
      vertx.deployVerticle(Mannequin.class, options);
   }
}
