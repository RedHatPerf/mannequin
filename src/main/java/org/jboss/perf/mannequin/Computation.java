package org.jboss.perf.mannequin;

import java.math.BigInteger;

public class Computation {
   private static final BigInteger FOUR = BigInteger.valueOf(4);
   private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);
   private static final BigInteger MINUS_TWO = BigInteger.valueOf(-2);

   // See https://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
   public static boolean isMersennePrime(int p) {
      BigInteger s = FOUR;
      BigInteger M = BigInteger.valueOf(2).pow(p).add(MINUS_ONE);
      for (int i = 0; i < p - 2; ++i) {
         s = s.multiply(s).add(MINUS_TWO).mod(M);
      }
      return s.compareTo(BigInteger.ZERO) == 0;
   }
}
