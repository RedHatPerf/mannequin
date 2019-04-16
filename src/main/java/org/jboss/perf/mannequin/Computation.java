package org.jboss.perf.mannequin;


import javax.json.*;
import java.math.BigInteger;

public class Computation {
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);

   // See https://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
   public static JsonObject isMersennePrime(int p) {
      JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
      JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
      objectBuilder.add("input", p);
      //to ensure responses cannot be memoized
      objectBuilder.add("startMillis", System.currentTimeMillis());
      BigInteger s = FOUR;
      arrayBuilder.add(s);
      BigInteger M = BigInteger.valueOf(2).pow(p).add(MINUS_ONE);
      objectBuilder.add("M",M);
      for (int i = 0; i < p - 2; ++i) {
         s = s.multiply(s).add(MINUS_TWO).mod(M);
         arrayBuilder.add(s.toString());
      }
      //to ensure responses cannot be memoized
      objectBuilder.add("stopMillis", System.currentTimeMillis());
      objectBuilder.add("steps", arrayBuilder.build());
      objectBuilder.add("result", s.compareTo(BigInteger.ZERO) == 0);
      return objectBuilder.build();
   }
}
