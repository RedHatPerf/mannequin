package org.jboss.perf.mannequin;

import java.math.BigInteger;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class Computation {
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);

   // See https://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
   public static JsonObject isMersennePrime(int p, boolean addSteps) {
      JsonObject output = new JsonObject();
      JsonArray steps = new JsonArray();
      output.put("input", p);
      //to ensure responses cannot be memoized
      output.put("startMillis", System.currentTimeMillis());
      BigInteger s = FOUR;
      steps.add(s);
      BigInteger M = BigInteger.valueOf(2).pow(p).add(MINUS_ONE);
      output.put("M", M);
      for (int i = 0; i < p - 2; ++i) {
         s = s.multiply(s).add(MINUS_TWO).mod(M);
         if (addSteps) {
            steps.add(s.toString());
         }
      }
      //to ensure responses cannot be memoized
      output.put("stopMillis", System.currentTimeMillis());
      output.put("steps", steps);
      output.put("result", s.compareTo(BigInteger.ZERO) == 0);
      return output;
   }
}
