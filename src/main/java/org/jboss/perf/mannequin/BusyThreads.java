package org.jboss.perf.mannequin;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class BusyThreads {
   private volatile ExecutorService executors;
   private int current;

   public synchronized void set(int busy) {
      if (current == busy) {
         return;
      }
      current = busy;
      if (executors != null) {
         executors.shutdown();
      }
      if (busy > 0) {
         executors = Executors.newFixedThreadPool(busy);
         for (int i = 0; i < busy; ++i) {
            executors.submit(() -> busyTask(executors));
         }
      } else {
         executors = null;
      }
   }

   private int busyTask(ExecutorService myExecutors) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      int add = 0;
      while (executors == myExecutors) {
         add = Computation.isMersennePrime(random.nextInt(255) + add) ? 1 : 0;
      }
      return add;
   }
}
