package org.apache.pinot.common.utils;

import org.testng.annotations.Test;


public class TempFingerprintClass {

  @Test
  public void testFoo() {
    System.out.println(CustomQueryParser.getQueryFingerprint("SELECT * FROM tbl1 WHERE foo = 'bar'"));
    System.out.println(CustomQueryParser.getQueryFingerprint("SELECT * FROM tbl1 WHERE foo = 'bar' and foo = 'y'"));
    System.out.println(CustomQueryParser.getQueryFingerprint(
        "SELECT * FROM tbl1 WHERE foo IN ('bar', 'y', 'x', 'y', 'z')"));
    System.out.println(CustomQueryParser.getQueryFingerprint(
        "SELECT * FROM tbl1 WHERE foo IN ('x', 'y', 'z') AND uuid IN (SELECT uuid FROM tbl2 WHERE col = 'bar')"));
    System.out.println(CustomQueryParser.getQueryFingerprint(
        "SELECT 'foo', col1, foo FROM tbl1 WHERE foo IN ('x', 'y', 'z')"));

    // You can have dot separated table names.
    System.out.println(CustomQueryParser.getQueryFingerprint(
        "SELECT 'foo', col1, foo FROM a.b.tbl1 WHERE foo IN ('x', 'y', 'z')"));
  }
}