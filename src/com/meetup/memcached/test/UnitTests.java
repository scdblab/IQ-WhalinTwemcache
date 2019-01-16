/**
 * Copyright (c) 2008 Greg Whalin
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the BSD license
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.
 *
 * You should have received a copy of the BSD License along with this
 * library.
 *
 * @author Kevin Burton
 * @author greg whalin <greg@meetup.com> 
 */
package com.meetup.memcached.test;

import com.meetup.memcached.*;

import java.util.*;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.IOException;
import java.io.Serializable;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.log4j.BasicConfigurator;
import java.nio.ByteBuffer;

public class UnitTests {

    // logger
    private static Logger log =
            Logger.getLogger( UnitTests.class.getName() );

    public static MemcachedClient mc  = null;
    public static final int EXP_LEASE_TIME = 1000;

    public static void test1() {
        try {
            mc.set( "foo", Boolean.TRUE );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Boolean b = (Boolean)mc.get( "foo" );
        assert b.booleanValue();
        log.error( "+ store/retrieve Boolean type test passed" );
    }
    
    public static void testOQSwap(MemcachedClient mc) throws IOException, COException {
        String sid = mc.generateSID();
        assert mc.oqSwap(sid, "key1", 0, "val1".getBytes(), false);
        assert mc.dCommit(sid, 0);
        byte[] bytes = (byte[]) mc.get("key1");
        assert (bytes != null);
        assert new String(bytes).equals("val1");
    }

    public static void test2() {
        try {
            mc.set( "foo", new Integer( Integer.MAX_VALUE ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Integer i = (Integer)mc.get( "foo" );
        assert i.intValue() == Integer.MAX_VALUE;
        log.error( "+ store/retrieve Integer type test passed" );
    }

    public static void test3() {
        String input = "test of string encoding";
        try {
            mc.set( "foo", input );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        String s = (String)mc.get( "foo" );
        assert s.equals( input );
        log.error( "+ store/retrieve String type test passed" );
    }

    public static void test4() {
        try {
            mc.set( "foo", new Character( 'z' ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Character c = (Character)mc.get( "foo" );
        assert c.charValue() == 'z';
        log.error( "+ store/retrieve Character type test passed" );
    }

    public static void test5() {
        try {
            mc.set( "foo", new Byte( (byte)127 ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Byte b = (Byte)mc.get( "foo" );
        assert b.byteValue() == 127;
        log.error( "+ store/retrieve Byte type test passed" );
    }

    public static void test6() {
        try {
            mc.set( "foo", new StringBuffer( "hello" ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        StringBuffer o = (StringBuffer)mc.get( "foo" );
        assert o.toString().equals( "hello" );
        log.error( "+ store/retrieve StringBuffer type test passed" );
    }

    public static void test7() {
        try {
            mc.set( "foo", new Short( (short)100 ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Short o = (Short)mc.get( "foo" );
        assert o.shortValue() == 100;
        log.error( "+ store/retrieve Short type test passed" );
    }

    public static void test8() {
        try {
            mc.set( "foo", new Long( Long.MAX_VALUE ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Long o = (Long)mc.get( "foo" );
        assert o.longValue() == Long.MAX_VALUE;
        log.error( "+ store/retrieve Long type test passed" );
    }

    public static void test9() {
        try {
            mc.set( "foo", new Double( 1.1 ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Double o = (Double)mc.get( "foo" );
        assert o.doubleValue() == 1.1;
        log.error( "+ store/retrieve Double type test passed" );
    }

    public static void test10() {
        try {
            mc.set( "foo", new Float( 1.1f ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Float o = (Float)mc.get( "foo" );
        assert o.floatValue() == 1.1f;
        log.error( "+ store/retrieve Float type test passed" );
    }

    public static void test11() {
        try {
            mc.set( "foo", new Integer( 100 ), new Date( System.currentTimeMillis() + 1000));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        try { Thread.sleep( 2000 ); } catch ( Exception ex ) { }
        assert mc.get( "foo" ) == null;
        log.error( "+ store/retrieve w/ expiration test passed" );
    }

    public static void test12() {
        long i = 0;
        try {
            mc.storeCounter("foo", i);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        mc.incr("foo"); // foo now == 1
        mc.incr("foo", (long)5); // foo now == 6
        long j = mc.decr("foo", (long)2); // foo now == 4
        assert j == 4;
        assert j == mc.getCounter( "foo" );
        log.error( "+ incr/decr test passed" );
    }

    public static void test13() {
        Date d1 = new Date();
        try {
            mc.set("foo", d1);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Date d2 = (Date) mc.get("foo");
        assert d1.equals( d2 );
        log.error( "+ store/retrieve Date type test passed" );
    }

    public static void test14() {
        assert !mc.keyExists( "foobar123" );
        try {
            mc.set( "foobar123", new Integer( 100000) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        assert mc.keyExists( "foobar123" );
        log.error( "+ store/retrieve test passed" );

        assert !mc.keyExists( "counterTest123" );
        try {
            mc.storeCounter( "counterTest123", 0 );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        assert mc.keyExists( "counterTest123" );
        log.error( "+ counter store test passed" );
    }

    @SuppressWarnings("rawtypes")
    public static void test15() {

        Map stats = mc.statsItems();
        assert stats != null;

        stats = mc.statsSlabs();
        assert stats != null;

        log.error( "+ stats test passed" );
    }

    public static void test16() {
        try {
            assert !mc.set( "foo", null );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        log.error( "+ invalid data store [null] test passed" );
    }

    public static void test17() {
        try {
            mc.set( "foo bar", Boolean.TRUE );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        Boolean b = (Boolean)mc.get( "foo bar" );
        assert b.booleanValue();
        log.error( "+ store/retrieve Boolean type test passed" );
    }

    public static void test18() {
        try {
            mc.addOrIncr( "foo" );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        } // foo now == 0
        mc.incr( "foo" ); // foo now == 1
        mc.incr( "foo", (long)5 ); // foo now == 6

        try {
            mc.addOrIncr( "foo" );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        } // foo now 7

        long j = mc.decr( "foo", (long)3 ); // foo now == 4
        assert j == 4;
        assert j == mc.getCounter( "foo" );

        log.error( "+ incr/decr test passed" );
    }

    public static void test19() {
        int max = 100;
        String[] keys = new String[ max ];
        for ( int i=0; i<max; i++ ) {
            keys[i] = Integer.toString(i);
            try {
                mc.set( keys[i], "value"+i );
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                assert(false);
            }
        }

        Map<String,Object> results = mc.getMulti( keys );
        for ( int i=0; i<max; i++ ) {
            assert results.get( keys[i]).equals( "value"+i );
        }
        log.error( "+ getMulti test passed" );
    }

    public static void test20( int max, int skip, int start ) {
        log.warn( String.format( "test 20 starting with start=%5d skip=%5d max=%7d", start, skip, max ) );
        int numEntries = max/skip+1;
        String[] keys = new String[ numEntries ];
        byte[][] vals = new byte[ numEntries ][];

        int size = start;
        for ( int i=0; i<numEntries; i++ ) {
            keys[i] = Integer.toString( size );
            vals[i] = new byte[size + 1];
            for ( int j=0; j<size + 1; j++ )
                vals[i][j] = (byte)j;

            try {
                mc.set( keys[i], vals[i] );
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                assert(false);
            }
            size += skip;
        }

        Map<String,Object> results = mc.getMulti( keys );
        for ( int i=0; i<numEntries; i++ )
            assert Arrays.equals( (byte[])results.get( keys[i]), vals[i] );

        log.warn( String.format( "test 20 finished with start=%5d skip=%5d max=%7d", start, skip, max ) );
    }

    public static void test21() {
        try {
            mc.set( "foo", new StringBuilder( "hello" ) );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        StringBuilder o = (StringBuilder)mc.get( "foo" );
        assert o.toString().equals( "hello" );
        log.error( "+ store/retrieve StringBuilder type test passed" );
    }

    public static void test22() {
        byte[] b = new byte[10];
        for ( int i = 0; i < 10; i++ )
            b[i] = (byte)i;

        try {
            mc.set( "foo", b );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        assert Arrays.equals( (byte[])mc.get( "foo" ), b );
        log.error( "+ store/retrieve byte[] type test passed" );
    }

    public static void test23() {
        TestClass tc = new TestClass( "foo", "bar", new Integer( 32 ) );
        try {
            mc.set( "foo", tc );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            assert(false);
        }
        assert tc.equals( (TestClass)mc.get( "foo" ) );
        log.error( "+ store/retrieve serialized object test passed" );
    }

    public static void test24() {

        String[] allKeys = { "key1", "key2", "key3", "key4", "key5", "key6", "key7" };
        String[] setKeys = { "key1", "key3", "key5", "key7" };

        for ( String key : setKeys ) {
            try {
                mc.set( key, key );
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                assert(false);
            }
        }

        Map<String,Object> results = mc.getMulti( allKeys );

        assert setKeys.length == results.size();
        for ( String key : setKeys ) {
            String val = (String)results.get( key );
            assert key.equals( val );
        }

        log.error( "+ getMulti w/ keys that don't exist test passed" );
    }

    /***
     * Normal delete, get, set and get again.
     * @param mc
     * @throws Exception 
     */
    public static void test99( MemcachedClient mc) throws Exception
    {
        String key = "key99";
        String value = "value99";
        Object result = null;

        mc.delete(key);
        result = mc.get(key);

        mc.set(key, value);

        result = mc.get(key);
        System.out.println("Result: " + result);
        assert value.equals(result);
    }

    /***
     * Test get and overwrite transient value
     * @param mc
     * @throws Exception 
     */
    public static void test100( MemcachedClient mc ) throws Exception
    {
        String key = "key100";
        String value = "value100";		
        Object result = null;


        result = mc.get(key);		
        mc.set(key, value);

        mc.delete(key);

        result = mc.get(key);
        //assert value.equals(result);
        value += "new";
        mc.set(key, value);

        result = mc.get(key);
        assert value.equals(result);
    }

    public static void testGetMulti( MemcachedClient mc ) 
    {
        String keys[] = {"keySQLTrig", "keySQLTrigSecond"};
        Map<String, Object> results = mc.getMulti(keys);

        for(String ret_key : results.keySet()) {
            System.out.println(ret_key + " : " + results.get(ret_key).toString());
        }
    }

    public static void testDeleteRace( MemcachedClient mc, MemcachedClient mc2 ) throws Exception {
        String key = "key1";
        String value_v1 = "value1_v1";
        String value_v2 = "value1_v2";
        Object result = null;

        mc.delete(key);
        result = mc.iqget(key);
        assert result == null;

        mc2.delete(key);

        mc.iqset(key, value_v1);
        result = mc.iqget(key);		
        assert result == null;		

        mc.iqset(key, value_v2);
        result = mc.iqget(key);
        assert value_v2.equals(result);
    }

    public static void testExponentialBackoff( MemcachedClient mc, MemcachedClient mc2 ) throws Exception {

    }

    public static void testLease( MemcachedClient mc ) throws Exception {
        String key = "testleasekey1";
        String value = "tempvalue1";
        Object ret_value = null;

        // Disable backoff. So that get after lease doesn't wait.
        mc.disableBackoff();

        mc.delete(key);

        assert mc.iqget(key) == null;			
        assert mc.iqset(key, value);		
        ret_value = mc.iqget(key);
        assert value.equals(ret_value);

        // Re-enable backoff for other tests.
        mc.enableBackoff();

        System.out.println("OK");
    }

    public static void testLeaseConflict( MemcachedClient mc ) throws Exception {
        String key = "testleasekey2";
        String value = "tempvalue2";
        Object ret_value = null;

        // Disable backoff. So that get after lease doesn't wait.
        mc.disableBackoff();

        mc.delete(key);

        assert mc.iqget(key) == null;	
        mc.delete(key);
        assert mc.iqset(key, value) == false;

        assert mc.iqget(key) == null;
        assert mc.iqset(key, value);
        ret_value = mc.iqget(key);
        assert value.equals(ret_value);

        // Re-enable backoff for other tests.
        mc.enableBackoff();

        System.out.println("OK");
    }

    public static void testLeaseMultiple( MemcachedClient mc1, MemcachedClient mc2 ) throws Exception {
        String key = "testleasekey3";
        String value = "tempvalue3";
        Object ret_value = null;

        // Disable backoff. So that get after hold doesn't wait.
        mc1.disableBackoff();
        mc2.disableBackoff();

        mc1.delete(key);
        assert mc1.iqget(key) == null;
        assert mc2.iqget(key) == null;	// Should also fail to acquire lease because mc1 has it

        assert mc1.iqset(key, value);
        ret_value = mc1.iqget(key);
        assert value.equals(ret_value);	
        ret_value = mc2.iqget(key);	testIQGetSet(mc);
        assert value.equals(ret_value);

        // Re-enable backoff for other tests.
        mc1.enableBackoff();
        mc2.disableBackoff();
        System.out.println("OK");
    }

    public static void runAlTests( MemcachedClient mc, MemcachedClient mc2 ) throws Exception {
        test14();

        for ( int t = 0; t < 2; t++ ) {
            mc.setCompressEnable( ( t&1 ) == 1 );

            test1();
            test2();
            test3();
            test4();
            test5();
            test6();
            test7();
            test8();
            test9();
            test10();
            //			test11();
            test12();
            test13();
            test15();
            test16();
            test17();
            test21();
            test22();
            test23();
            test24();

            for ( int i = 0; i < 3; i++ )
                test19();

            test20( 8191, 1, 0 );
            test20( 8192, 1, 0 );
            test20( 8193, 1, 0 );

            test20( 16384, 100, 0 );
            test20( 17000, 128, 0 );

            test20( 128*1024, 1023, 0 );
            test20( 128*1024, 1023, 1 );
            test20( 128*1024, 1024, 0 );
            test20( 128*1024, 1024, 1 );

            test20( 128*1024, 1023, 0 );
            test20( 128*1024, 1023, 1 );
            test20( 128*1024, 1024, 0 );
            test20( 128*1024, 1024, 1 );

            test20( 900*1024, 32*1024, 0 );
            test20( 900*1024, 32*1024, 1 );
        }

    }

    public static void testReadLease(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        testLease(mc);
        testLeaseConflict(mc);
        testLeaseMultiple(mc, mc2);
    }

    /**
     * Test iqget and iqset
     * @param mc
     * @throws Exception
     */
    public static void testIQGetSet(MemcachedClient mc) throws Exception {
        String key = "key1", value = "value1";

        // try to empty first
        mc.delete(key);		
        Object obj =  mc.iqget(key);
        assert obj == null;

        // put new value into the cache
        mc.iqset(key, value);		
        assert mc.iqget(key).equals(value);

        System.out.println("OK READ");
    }

    public static void testQaRead(MemcachedClient mc) throws Exception {
        String key = "qareadkey1";
        String value = "value1";
        String value2 = "value2";

        // Clean the cache.
        mc.delete(key);
        assert mc.iqget(key) == null;

        // put new value into the cache
        mc.iqset(key, value);		
        assert mc.iqget(key).equals(value);

        assert value.equals(mc.quarantineAndRead(key));		
        mc.swapAndRelease(key, value2);

        assert value2.equals(mc.iqget(key));

        System.out.println("testQARead_RMW OK");
    }

    public static void testQARead_RMWemptyCache(MemcachedClient mc) throws Exception {
        String key = "qareadkey1";
        String value = "value1";

        // Clean the cache.
        mc.delete(key);

        // SaR with null value
        assert mc.quarantineAndRead(key) == null;		
        mc.swapAndRelease(key, null);

        // There should be nothing.
        assert mc.iqget(key) == null;
        assert mc.iqset(key, value) == true;
        assert value.equals(mc.iqget(key));

        System.out.println("testQARead_RMWemptyCache OK");
    }

    public static void testQARead_RMWemptyCacheIlease(MemcachedClient mc) throws Exception {
        String key = "qareadkey1";
        String value = "value1";

        // Clean the cache.
        mc.delete(key);
        assert mc.iqget(key) == null;

        // Case 1: SaR with null value
        assert mc.quarantineAndRead(key) == null;		
        mc.swapAndRelease(key, null);

        // There should be nothing.
        assert mc.iqget(key) == null;

        // Case 2: SaR with a value
        assert mc.quarantineAndRead(key) == null;		
        mc.swapAndRelease(key, value);

        assert mc.iqget(key).equals(value);		

        System.out.println("testQARead_RMWemptyCacheIlease OK");
    }

    public static void testQARead_RMWfailedQuarantine(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "qareadkey1";
        String value = "value1";
        String value2 = "value2";

        // Clean the cache.
        mc.delete(key);
        assert mc.quarantineAndRead(key) == null;

//<<<<<<< Updated upstream
        try {
            mc2.quarantineAndRead(key);
            // Exception should be thrown before this.
            assert false;
        } catch (IQException e) {

        }

        // This swap should fail.
        mc2.swapAndRelease(key, value);

        mc.swapAndRelease(key, value2);
        assert mc.iqget(key).equals(value2);

        System.out.println("testQARead_RMWfailedQuarantine OK");
    }

    public static void testRMW(MemcachedClient mc) throws Exception {
        String key = "key1", value = "value1", newVal = "newVal", retVal = null;

        // try to empty first
        mc.delete(key);
        assert mc.iqget(key) == null;

        // put new value into the cache
        mc.iqset(key, value);
        assert mc.iqget(key).equals(value);

        // try to write hold, should return the token successfully
        assert mc.quarantineAndRead(key).equals(value);

        // release token and set new value
        mc.swapAndRelease(key, newVal);

        // client now should get the new value
        retVal = (String)mc.iqget(key);
        assert newVal.equals(retVal);

        System.out.println("OK RMW");
    }

    public static void testReadOnWriteLease(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key = "key1", value = "value1";

        mc2.disableBackoff();

        // try to empty first
        mc1.delete(key);
        assert mc1.iqget(key) == null;

        // put new value into the cache
        mc1.iqset(key, value);
        assert mc1.iqget(key).equals(value);

        // try to write hold, should return the token successfully
        assert mc1.quarantineAndRead(key).equals(value);

        // try to read the value, should read the value
        assert mc2.iqget(key).equals(value);

        System.out.println("OK READ_ON_WRITE_LEASE");
    }

    public static void testWriteOnWriteLease(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key = "key1", value = "value1";

        mc2.disableBackoff();

        // try to empty first
        mc1.delete(key);
        assert mc1.iqget(key) == null;

        // put new value into the cache
        mc1.iqset(key, value);
        assert mc1.iqget(key).equals(value);

        // try to write hold, should return the token successfully
        assert mc1.quarantineAndRead(key).equals(value);	

        // mc2 try to request for token
        try {
            mc2.quarantineAndRead(key);
            assert false;
        } catch (IQException e) { }

        System.out.println("OK_WRITE_ON_WRITE_LEASE");
    }

    public static void testWriteAfterWriteLease(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key = "key1", value = "value1", newVal = "newVal";

        mc2.disableBackoff();

        // try to empty first
        mc1.delete(key);
        assert mc1.iqget(key) == null;

        // put new value into the cache
        mc1.iqset(key, value);
        assert mc1.iqget(key).equals(value);

        // try to write hold, should return the token successfully
        assert mc1.quarantineAndRead(key).equals(value);

        // release token and set new value
        mc1.swapAndRelease(key, newVal);

        // mc2 try to request for token
        assert mc2.quarantineAndRead(key).equals(newVal);

        // release token and set new value
        mc2.swapAndRelease(key, "something_new");

        System.out.println("OK_WRITE_AFTER_WRITE_LEASE");
    }	

    public static void testReleaseTokenWithoutXLease(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "key1", value = "value1", newVal = "newVal";

        mc.disableBackoff();
        mc2.disableBackoff();

        // try to empty first
        mc.delete(key);
        assert mc.iqget(key) == null;

        // put new value into the cache
        mc.iqset(key, value);
        assert mc.iqget(key).equals(value);

        // try to release token 
        // release token and set new value
        mc.swapAndRelease(key, newVal);

        // read the value again, it should be the old value
        assert mc.iqget(key).equals(value);

        // try to get a token
        assert mc.quarantineAndRead(key).equals(value);

        // try to release token, should fail
        mc2.swapAndRelease(key, newVal);
        assert mc2.iqget(key).equals(value);

        System.out.println("OK_RELEASE_TOKEN_WITHOUT_XLEASE");
    }

    public static void testRMWLeaseTimedOut(MemcachedClient mc) throws Exception {
        String key = "key1", value = "value1", newVal = "newVal";

        mc.disableBackoff();

        // try to empty first
        mc.delete(key);
        assert mc.iqget(key) == null;

        // put new value into the cache
        mc.iqset(key, value);
        assert mc.iqget(key).equals(value);

        // try to get token
        assert mc.quarantineAndRead(key).equals(value);

        Thread.sleep(EXP_LEASE_TIME + 1000);

        mc.swapAndRelease(key, newVal);

        assert mc.iqget(key) == null;

        System.out.println("OK_TEST_RMW_TIMED_OUT");
    }


    // Test that when multiple keys are xLeased, an xLease failure of a later key will
    // cause all earlier keys to be released.
    // mc1 wants to xLease key1 and key2. 
    // 1. mc1 successfully xLeases key1. 
    // 2. mc2 sneaks in and xLeases key2.
    // 3. when mc1 tries to xLease key2, it should fail. 
    // 4. after this, mc2 should be able to xLease key1.
    //	public static void testFailedXLease(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
    //		String key1 = "testxleasekey6a";
    //		String key2 = "testxleasekey6b";
    //		String value = "tempvalue6";
    //		String new_value1 = "tempvalue6_new1";
    //		String new_value2 = "tempvalue6_new2";
    //
    //		// Disable backoff. So that get after hold doesn't wait in this test.
    //		mc1.disableBackoff();
    //		mc2.disableBackoff();
    //
    //		// Empty the cache
    //		mc1.delete(key1);
    //		mc1.delete(key2);
    //
    //		// Assign a value for key1 in the cache
    //		assert mc1.iqget(key1) == null;
    //		mc1.iqset(key1, value);
    //		assert mc1.iqget(key1).equals(value);
    //
    //		// Assign a value for key2 in the cache
    //		assert mc1.iqget(key2) == null;
    //		mc1.iqset(key2, value);
    //		assert mc1.iqget(key2).equals(value);
    //
    //		assert mc1.quarantineAndRead(key1).equals(value);
    //		assert mc2.quarantineAndRead(key2).equals(value);
    //
    //		try {
    //			mc1.quarantineAndRead(key2);
    //			assert false;
    //		} catch (IQException e) { }
    //		
    //		assert mc2.quarantineAndRead(key1) == null;
    //
    //		// Attempt to swap with mc1
    //		mc1.swapAndRelease(key1, new_value1);		// This should fail because lease is lost.
    //		mc1.swapAndRelease(key2, new_value1);		// This should fail because lease is lost.
    //
    //		// Check that the value is still the old one
    //		assert mc1.iqget(key1).equals(value);
    //		assert mc1.iqget(key2).equals(value);
    //
    //		// Do the actual swaps with mc2
    //		mc2.swapAndRelease(key1, new_value2);
    //		mc2.swapAndRelease(key2, new_value2);
    //
    //		// Check that the value is now the new one
    //		assert mc1.iqget(key1).equals(new_value2);
    //		assert mc2.iqget(key2).equals(new_value2);
    //
    //
    //		// Re-enable backoff for other tests.
    //		mc1.enableBackoff();
    //		mc2.enableBackoff();
    //		System.out.println("OK FAILED_XLEASE_CLEANUP");
    //	}

    public static void testGetNoLease(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "testGetNoLeasekey1", value = "testvalue1";

        // Try getting when the cache is empty (no lease token or value).
        // This call assumes the cache is empty to begin with, which is not
        // necessarily true, so no assert checks here.
        mc.get(key);

        mc.delete(key);
        assert mc.get(key) == null;
        assert mc2.iqget(key) == null;
        assert mc.get(key) == null;
        assert mc2.iqset(key, value);
        assert value.equals(mc2.iqget(key));

        assert value.equals(mc.get(key));
        assert value.equals(mc2.get(key));
        System.out.println("OK GET_NO_LEASE");
    }

    public static void testReleaseX(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key = "testReleaseXkey1", value = "testvalue1";

        // Prevent backoff for this test.
        mc2.disableBackoff();

        // Clear key from the cache.
        mc1.delete(key);

        // mc1 acquires the lease
        assert mc1.iqget(key) == null;

        // mc2 should fail to acquire a lease and not be able to set
        assert mc2.iqget(key) == null;
        try { 
            mc2.iqset(key, value);
            assert false;
        } catch (IQException e) {

        }

        assert mc2.iqget(key) == null;

        // mc1 releases the lease. mc2 should now be able to get and set
        assert mc1.releaseX(key, null);

        assert mc2.iqget(key) == null;
        assert mc2.iqset(key, value);
        assert mc2.quarantineAndRead(key).equals(value);
        mc2.swapAndRelease(key, "newVal");

        assert mc2.get(key).equals("newVal");
        assert mc2.set(key, value);
        assert value.equals(mc2.get(key));

        // Reset backoff setting.
        mc2.enableBackoff();

        System.out.println("OK RELEASE_X");
    }

    /** Testing cases for SXQ cache **/
    /** @author hieun 
     * @throws Exception **/

    /*
     * Testing one client QaReg and DaR
     */
    public static void sxqQaReg(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = "key1", value1 = "value1";

        // Prevent backoff for this test.
        mc1.disableBackoff();

        // Clear key from the cache.
        mc1.delete(key1);

        // mc1 acquires the lease
        assert mc1.iqget(key1) == null;		

        // mc1 should store value successfully
        assert mc1.iqset(key1, value1);

        // notice KVS for invalidated key
        String tid = mc1.generateSID();
        assert mc1.quarantineAndRegister(tid, key1);

        assert mc2.get(key1).equals(value1);

        assert mc1.commit(tid, null);

        assert mc1.iqget(key1) == null;

        System.out.println("SXQ_QAREG_OK");
    }

    /*
     * Test one client try to QaReg multiple-keys
     */
    public static void sxqQaReg2(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = "key1", value1 = "value1";
        String key2 = "key2", value2 = "value2";
        String key3 = "key3", value3 = "value3";

        // Prevent backoff for this test.
        mc1.disableBackoff();

        // Clear key from the cache.
        mc1.delete(key1);
        mc1.delete(key2);
        mc1.delete(key3);

        // mc1 acquires the lease
        assert mc1.iqget(key1) == null;		
        assert mc1.iqget(key2) == null;
        assert mc1.iqget(key3) == null;

        // mc1 should store value successfully
        assert mc1.iqset(key1, value1);
        assert mc1.iqset(key2, value2);
        assert mc1.iqset(key3, value3);

        // notice KVS for invalidated key
        String tid = mc1.generateSID();

        assert mc1.quarantineAndRegister(tid, key1);
        assert mc1.quarantineAndRegister(tid, key2);
        assert mc1.quarantineAndRegister(tid, key3);

        assert mc1.commit(tid, null);

        assert mc1.iqget(key1) == null;
        assert mc2.iqget(key2) == null;
        assert mc2.iqget(key3) == null;

        System.out.println("SXQ_QAREG_2_OK");
    }	

    /**
     * Test multiple QaReg
     */
    public static void sxqQaReg3(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1="key1", key2="key2", key3="key3", key4="key4", key5="key5";
        String val1="val1", val2="val2", val3="val3", val4="val4", val5="val5";

        // Prevent backoff for this test.
        mc1.disableBackoff();
        mc2.disableBackoff();

        // Clear key from the cache.
        mc1.delete(key1);
        mc1.delete(key2);
        mc1.delete(key3);
        mc1.delete(key4);
        mc1.delete(key5);

        // mc1 acquires the lease
        assert mc1.iqget(key1) == null;		
        assert mc1.iqget(key2) == null;
        assert mc1.iqget(key3) == null;
        assert mc1.iqget(key4) == null;
        assert mc1.iqget(key5) == null;

        // mc1 should store value successfully
        assert mc1.iqset(key1, val1);
        assert mc1.iqset(key2, val2);
        assert mc1.iqset(key3, val3);
        assert mc1.iqset(key4, val4);
        assert mc1.iqset(key5, val5);

        String tid = mc1.generateSID();
        String tid2 = mc2.generateSID();

        assert mc1.quarantineAndRegister(tid, key1);
        assert mc1.quarantineAndRegister(tid, key2);
        assert mc1.quarantineAndRegister(tid, key3);

        assert mc2.quarantineAndRegister(tid2, key3);
        assert mc2.quarantineAndRegister(tid2, key4);
        assert mc2.quarantineAndRegister(tid2, key5);

        assert mc1.commit(tid, null);		
        assert mc1.iqget(key1) == null;
        assert mc1.iqget(key2) == null;
        assert mc1.get(key3) == null;

        assert mc1.get(key3) == null;

        assert mc2.commit(tid2, null);
        assert mc2.iqget(key3) == null;
        assert mc2.iqget(key4) == null;
        assert mc2.iqget(key5) == null;	

        System.out.println("SXQ_QAREG_3_OK");
    }

    /**
     * Test QaReg with no key exists
     */
    public static void sxq4(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = UUID.randomUUID().toString();
        String value1="value1";

        // test QaReg with no key exists
        String tid = mc1.generateSID();
        assert mc1.quarantineAndRegister(tid, key1) == true;

        mc2.disableBackoff();

        assert mc2.iqget(key1) == null;

        // this should fail because the previous get should not be granted a shared lease
        assert !mc2.iqset(key1, value1);
        assert mc1.commit(tid, null) == true;

        System.out.println("SXQ_4_OK");
    }

    /**
     * Try Set and SaR during Q leases
     */
    public static void sxq5(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = "key1", val1="val1", val2="val2";

        mc1.disableBackoff();

        mc1.delete(key1);

        // a shared lease now can be granted here
        mc1.iqget(key1);
        assert mc1.iqset(key1, val1);
        assert mc1.iqget(key1).equals(val1);

        // a x lease can be grated here
        assert mc1.quarantineAndRead(key1).equals(val1);

        // wait an amount of time for x lease to expire
        Thread.sleep(EXP_LEASE_TIME + 1000);

        // now QaReg should be granted a lease
        String tid = mc2.generateSID();
        assert mc2.quarantineAndRegister(tid, key1) == true;

        // should fail
        mc1.swapAndRelease(key1, "new_val");		
        assert mc1.get(key1) == null;

        // release Q lease, so the following get and set can success
        assert mc2.commit(tid, null);		
        assert mc1.iqget(key1) == null;		
        assert mc1.iqset(key1, val2);

        assert mc1.iqget(key1).equals(val2);

        System.out.println("SXQ_5_OK");
    }

    /**
     * Test QaReg in items that have S lease or X lease
     */
    public static void sxq6(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1="key1", val1="val1";

        mc1.disableBackoff();

        mc1.delete(key1);

        assert mc1.iqget(key1) == null;

        // QaReg here should be ok
        String tid = mc2.generateSID();
        assert mc2.quarantineAndRegister(tid, key1);

        // set then cannot successcdes
        assert mc1.iqset(key1, val1) == false;

        // delete and release Q lease
        assert mc2.commit(tid, null);

        assert mc1.iqget(key1) == null;
        assert mc1.iqset(key1, val1);

        assert mc1.iqget(key1).equals(val1);

        // grant an x lease for mc1
        assert mc1.quarantineAndRead(key1).equals(val1);

        // should fail to grant a Q lease here
        assert mc2.quarantineAndRegister(tid, key1) == true;

        mc1.swapAndRelease(key1, "new_val");

        assert mc1.get(key1).equals(val1);
        assert mc2.commit(tid, null);

        System.out.println("SXQ_6_OK");
    }

    /**
     * Test DaR
     */
    public static void sxq7(MemcachedClient mc1) throws Exception {
        String key1 = UUID.randomUUID().toString();

        mc1.disableBackoff();

        String tid = mc1.generateSID();
        assert mc1.commit(tid, null) == false;

        mc1.delete(key1);
        assert mc1.commit(tid, null) == false;

        mc1.iqget(key1);
        assert mc1.commit(tid, null) == false;

        mc1.iqset(key1, "val1");
        assert mc1.iqget(key1).equals("val1");

        assert mc1.quarantineAndRead(key1).equals("val1");
        assert mc1.commit(tid, null) == false;
        mc1.swapAndRelease(key1, "newVal");

        assert mc1.iqget(key1).equals("newVal");

        mc1.quarantineAndRegister(tid, key1);
        assert mc1.commit(UUID.randomUUID().toString(), null) == false;
        assert mc1.commit(tid, null) == true;

        assert mc1.iqget(key1) == null;
        assert mc1.iqset(key1, "val2");

        assert mc1.iqget(key1).equals("val2");

        System.out.println("SXQ_7_OK");
    }

    /**
     * Test QaC when item has no value, shared leases or Qinv
     * @param mc1
     * @param mc2
     * @throws Exception
     */
    public static void sxq8(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1=UUID.randomUUID().toString(), val1="val1";

        assert mc1.quarantineAndRead(key1) == null;

        mc1.delete(key1);
        assert mc1.quarantineAndRead(key1) == null;

        // item should not be granted an I lease here
        assert mc1.iqget(key1) == null;

        // QaRead should not backoff
        assert mc1.quarantineAndRead(key1) == null;

        assert mc1.iqset(key1, "temp") == false;

        mc1.swapAndRelease(key1, val1);
        mc1.swapAndRelease(key1, "temp2");
        mc1.swapAndRelease(key1, "temp3");
        mc1.swapAndRelease(key1, "temp4");

        assert mc1.iqget(key1).equals(val1);

        // now item has value, so QaReg should be success
        assert mc1.quarantineAndRead(key1).equals(val1);
        mc1.swapAndRelease(key1, "newVal");

        assert mc1.iqget(key1).equals("newVal");

        //mc2.disableBackoff();		
        String tid2 = mc2.generateSID();
        assert mc2.quarantineAndRegister(tid2, key1) == true;

        try {
            mc1.quarantineAndRead(key1);
            assert false;
        } catch (Exception e) {}

        mc1.swapAndRelease(key1, "temp");		// this should fail		

        assert mc1.commit(tid2, null) == true;		
        assert mc1.iqget(key1) == null;

        System.out.println("SXQ_8_OK");
    }

    /**
     * Use the same tid for 2 QaReg session
     */
    public static void sxq9(MemcachedClient mc1) throws Exception {
        String key1="key1", val1="val1";
        String key2="key2";
        String key3="key3", val3="val3";
        String key4="key4";
        String key5="key5";

        mc1.delete(key1);
        mc1.delete(key2);
        mc1.delete(key3);
        mc1.delete(key4);
        mc1.delete(key5);

        mc1.iqget(key1); mc1.iqset(key1, val1);
        //mc1.get(key2); mc1.set(key2, val2);
        mc1.iqget(key3); mc1.iqset(key3, val3);

        String tid = mc1.generateSID();
        mc1.quarantineAndRegister(tid, key1);
        mc1.quarantineAndRegister(tid, key2);
        mc1.quarantineAndRegister(tid, key3);

        assert mc1.commit(tid, null);

        assert mc1.commit(tid, null) == false;

        mc1.quarantineAndRegister(tid, key3);
        mc1.quarantineAndRegister(tid, key4);
        mc1.quarantineAndRegister(tid, key5);

        assert mc1.commit(tid, null) == true;

        assert mc1.commit(tid, null) == false;

        System.out.println("SXQ_9_OK");
    }

    public static void test_incr(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = UUID.randomUUID().toString();
        long val1 = 100;

        mc2.enableBackoff();

        // no lease, no value
        assert mc1.incr(key1, 5) == -1;
        assert mc1.incr(key1) == -1;

        // i-lease, no value
        assert mc2.iqget(key1) == null;
        assert mc1.incr(key1) == -1;
        assert mc1.incr(key1, 5) == -1;
        mc2.iqset(key1, val1);
        assert mc1.iqget(key1).equals(val1);

        // test set negative and zero value
        mc2.delete(key1);
        mc2.iqget(key1);
        mc2.iqset(key1, "7");
        mc1.iqget(key1).equals("7");
        assert mc1.incr(key1) == 8;
        assert mc1.incr(key1, 5) == 13;
        mc2.delete(key1);
        mc2.iqget(key1);
        mc2.iqset(key1, "-7");
        mc1.iqget(key1).equals("-7");
        assert mc1.incr(key1) == -1;
        assert mc1.incr(key1, 5) == -1;    

        // Qinv lease (has or has not value)
        mc2.delete(key1);
        String tid = mc2.generateSID();
        mc2.quarantineAndRegister(tid, key1);
        assert mc1.incr(key1) == -1;
        assert mc1.incr(key1, 5) == -1;
        mc2.commit(tid, null);     
        mc2.iqget(key1);
        mc2.iqset(key1, "2");
        assert mc1.iqget(key1).equals("2");
        mc2.quarantineAndRegister(tid, key1);
        assert mc1.incr(key1) == 3;		// the item currently has novalue, so incr() should return -1
        assert mc1.incr(key1, 5) == 8;
        mc2.commit(tid, null);
        assert mc1.get(key1) == null;

        // no lease, has value
        mc2.iqget(key1);
        mc2.iqset(key1, "3");
        assert mc1.iqget(key1).equals("3");
        assert mc1.incr(key1) == 4;
        assert mc1.incr(key1, 2) == 6;

        // Qref lease

        mc2.quarantineAndRead(key1).equals("6");

        assert mc1.incr(key1) == 7;
        assert mc1.incr(key1, 3) == 10;
        mc2.swapAndRelease(key1, "some_new_val");
        assert mc1.iqget(key1).equals("some_new_val");

        System.out.println("TEST_INCR_OK");
    }

    public static void test_decr(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1 = UUID.randomUUID().toString();
        long val1 = 100;

        mc2.enableBackoff();

        // no lease, no value
        assert mc1.decr(key1, 5) == -1;
        assert mc1.decr(key1) == -1;

        // i-lease, no value
        assert mc2.iqget(key1) == null;
        assert mc1.decr(key1) == -1;
        assert mc1.decr(key1, 5) == -1;
        mc2.iqset(key1, val1);
        assert mc1.iqget(key1).equals(val1);

        // test set negative and zero value
        mc2.delete(key1);
        mc2.iqget(key1);
        mc2.iqset(key1, "7");
        mc1.iqget(key1).equals("7");
        assert mc1.decr(key1) == 6;
        assert mc1.decr(key1, 5) == 1;
        mc2.delete(key1);
        mc2.iqget(key1);
        mc2.iqset(key1, "-7");
        mc1.iqget(key1).equals("-7");
        assert mc1.decr(key1) == -1;
        assert mc1.decr(key1, 5) == -1;    

        // Qinv lease (has or has not value)
        mc2.delete(key1);
        String tid = mc2.generateSID();
        mc2.quarantineAndRegister(tid, key1);
        assert mc1.decr(key1) == -1;
        assert mc1.decr(key1, 5) == -1;
        mc2.commit(tid, null);     
        mc2.iqget(key1);
        mc2.iqset(key1, "12");
        assert mc1.iqget(key1).equals("12");
        mc2.quarantineAndRegister(tid, key1);
        assert mc1.decr(key1) == 11;		// the item has no value, so decr() should return -1
        assert mc1.decr(key1, 5) == 6;
        mc2.commit(tid, null);
        assert mc1.get(key1) == null;

        // no lease, has value
        mc2.iqget(key1);
        mc2.iqset(key1, "13");
        assert mc1.iqget(key1).equals("13");
        assert mc1.decr(key1) == 12;
        assert mc1.decr(key1, 2) == 10;

        // Qref lease
        mc2.quarantineAndRead(key1).equals("10");
        assert mc1.decr(key1) == 9;
        assert mc1.decr(key1, 3) == 6;
        mc2.swapAndRelease(key1, "3");
        assert mc1.iqget(key1).equals("3");

        System.out.println("TEST_DECR_OK");
    }  

    public static void test_delete(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        String key1= UUID.randomUUID().toString();
        String val1="val1", val2="val2";

        mc2.enableBackoff();

        // no lease, no value
        assert mc1.delete(key1) == false;

        // i-lease, no value
        assert mc2.iqget(key1) == null;
        assert mc1.delete(key1) == false;
        assert mc2.iqget(key1) == null;             // should not back-off here
        assert mc1.delete(key1) == false;

        // Qinv lease, no value
        String key = UUID.randomUUID().toString();
        String tid = mc2.generateSID();
        assert mc2.quarantineAndRegister(tid, key) == true;
        assert mc1.delete(key) == false;
        assert mc2.commit(tid, null) == true;

        // no lease, with value
        assert mc2.iqget(key1) == null;
        mc2.iqset(key1, val1);
        assert mc1.iqget(key1).equals(val1);
        assert mc1.delete(key1) == true;

        // Qinv lease, with value
        key = UUID.randomUUID().toString();
        assert mc2.iqget(key) == null;
        mc2.iqset(key, val2);
        assert mc1.iqget(key).equals(val2);
        String tid2 = mc2.generateSID();
        assert mc2.quarantineAndRegister(tid2, key);
        assert mc2.delete(key);

        // Qref lease, with value
        assert mc2.iqget(key1) == null;
        mc2.iqset(key1, val1);
        assert mc1.iqget(key1).equals(val1);
        assert mc2.quarantineAndRead(key1).equals(val1);
        assert mc2.delete(key1);

        assert mc2.iqget(key1) == null;
        mc1.delete(key1);

        System.out.println("TEST_DELETE_OK");
    }

    public static void test_append(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", val1="val1";
        String randomKey = UUID.randomUUID().toString();
        assert mc.append(randomKey, "some_rand_val") == false;

        // no lease, no val
        mc.delete(key1);       
        assert mc.append(key1, "new_val") == false;

        // i-lease (no val)
        assert mc.iqget(key1) == null;
        assert mc.append(key1, "new_val") == false;

        // no lease, has value
        assert mc.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        assert mc.append(key1, "new_val") == true;
        assert mc.iqget(key1).equals(val1 + "new_val");

        // Qinv-lease, no value
        mc.delete(key1);
        String tid = mc2.generateSID();
        mc2.quarantineAndRegister(tid, key1);
        assert mc2.append(key1, "new_val") == false;
        mc2.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qinv-lease, has value
        mc2.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        mc2.quarantineAndRegister(tid, key1);
        assert mc.append(key1, "new_val");
        assert mc.get(key1).equals(val1 + "new_val");
        mc2.commit(tid, null);
        assert mc.iqget(key1) == null;

        // Qref-lease (has value)
        mc.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        assert mc2.quarantineAndRead(key1).equals(val1);
        assert mc.append(key1, "new_val") == true;
        assert mc.iqget(key1).equals(val1 + "new_val");
        mc2.swapAndRelease(key1, "new");      
        assert mc.iqget(key1).equals("new");

        System.out.println("TEST_APPEND_OK");
    }

    public static void test_prepend(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", val1="val1";
        String randomKey = UUID.randomUUID().toString();
        assert mc.prepend(randomKey, "some_rand_val") == false;

        // no lease, no val
        mc.delete(key1);       
        assert mc.prepend(key1, "new_val") == false;

        // i-lease (no val)
        assert mc.iqget(key1) == null;
        assert mc.prepend(key1, "new_val") == false;

        // no lease, has value
        mc.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        assert mc.prepend(key1, "new_val") == true;
        assert mc.iqget(key1).equals("new_val" + val1);

        // Qinv-lease, no value
        mc.delete(key1);
        String tid = mc2.generateSID();
        mc2.quarantineAndRegister(tid, key1);
        assert mc2.prepend(key1, "new_val") == false;
        mc2.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qinv-lease, has value
        mc2.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        mc2.quarantineAndRegister(tid, key1);
        assert mc.prepend(key1, "new_val");
        assert mc.get(key1).equals("new_val" + val1);
        mc2.commit(tid, null);
        assert mc.iqget(key1) == null;

        // Qref-lease (has value)
        mc.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        assert mc2.quarantineAndRead(key1).equals(val1);
        assert mc.prepend(key1, "new_val") == true;
        assert mc.iqget(key1).equals("new_val" + val1);
        mc2.swapAndRelease(key1, "new");      
        assert mc.iqget(key1).equals("new");

        System.out.println("TEST_PREPEND_OK");
    }

    public static void test_add(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", val1="val1", randKey=UUID.randomUUID().toString();

        mc.disableBackoff();

        // no lease, no val
        mc.add(randKey, "randVal");
        assert mc.iqget(randKey).equals("randVal");
        mc.delete(key1);
        mc.add(key1, val1);
        assert mc.iqget(key1).equals(val1);

        // i-lease (no val)
        mc.delete(key1);
        assert mc.iqget(key1) == null;
        mc.add(key1, val1);
        assert mc2.iqget(key1).equals(val1);
        assert mc.iqset(key1, "set_val");
        assert mc2.iqget(key1).equals("set_val");

        // no lease, has val
        assert mc.add(key1, "new_val") == false;

        // Qinv, no val
        mc.delete(key1);
        String tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key1);
        mc2.add(key1, val1);
        assert mc2.iqget(key1).equals(val1);
        mc.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qinv, has val
        mc2.iqset(key1, val1);
        assert mc2.iqget(key1).equals(val1);
        mc.quarantineAndRegister(tid, key1);
        assert mc2.add(key1, "new_val") == false;
        assert mc2.get(key1).equals(val1);
        mc.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qref (has val)
        assert mc2.iqset(key1, val1) == true;
        assert mc2.iqget(key1).equals(val1);
        assert mc.quarantineAndRead(key1).equals(val1);
        assert mc2.add(key1, "new_val") == false;
        assert mc2.iqget(key1).equals(val1);
        mc.swapAndRelease(key1, "new");
        assert mc2.iqget(key1).equals("new");      

        System.out.println("TEST_ADD_OK");
    }

    public static void test_replace(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", val1="val1", randKey=UUID.randomUUID().toString();

        mc2.disableBackoff();

        // no val, no lease
        assert !mc.replace(randKey, "randVal");
        assert mc.iqget(randKey) == null;

        assert mc.delete(key1);
        assert !mc.replace(key1, val1);
        assert mc.iqget(key1) == null;
        assert mc.iqset(key1, val1);
        assert mc2.iqget(key1).equals(val1);

        // i-lease (no val)
        mc.delete(key1);
        assert mc.iqget(key1) == null;
        mc.replace(key1, val1);
        assert mc2.iqget(key1) == null;
        assert mc.iqset(key1, "set_val");
        assert mc2.iqget(key1).equals("set_val");

        // no lease, has val
        assert mc.replace(key1, "new_val");
        assert mc.iqget(key1).equals("new_val");

        // Qinv, no val
        mc.delete(key1);
        String tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key1);
        mc2.replace(key1, val1);
        assert mc2.iqget(key1) == null;
        mc.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qinv, has val
        mc2.iqset(key1, val1);
        assert mc2.iqget(key1).equals(val1);
        mc.quarantineAndRegister(tid, key1);
        assert mc2.replace(key1, "new_val");
        assert mc2.get(key1).equals("new_val");
        mc.commit(tid, null);
        assert mc2.iqget(key1) == null;

        // Qref (has val)
        assert mc2.iqset(key1, val1) == true;
        assert mc2.iqget(key1).equals(val1);
        assert mc.quarantineAndRead(key1).equals(val1);
        assert mc2.replace(key1, "new_val");
        assert mc2.iqget(key1).equals("new_val");
        mc.swapAndRelease(key1, "new");
        assert mc2.iqget(key1).equals("new");      

        System.out.println("TEST_REPLACE_OK");
    }

    public static void test_get(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String randKey=UUID.randomUUID().toString(), key1="key1", val1="val1";

        mc2.disableBackoff();

        // no lease, no value
        assert mc.get(randKey) == null;
        mc.delete(key1);
        assert mc.get(key1) == null;

        // i-lease (no val)
        assert mc.iqget(key1) == null;      // get an i-lease
        assert mc2.get(key1) == null;       // should return null
        assert mc.iqset(key1, val1) == true;    // set should be success
        assert mc.iqget(key1).equals(val1);

        // no lease, has val
        assert mc.get(key1).equals(val1);

        // Qinv, no val
        assert mc.delete(key1);
        String tid = mc.generateSID();
        assert mc.quarantineAndRegister(tid, key1);
        assert mc2.get(key1) == null;
        assert mc.commit(tid, null);
        assert mc2.get(key1) == null;

        // Qinv, has val
        assert mc.iqget(key1) == null;
        assert mc.iqset(key1, val1);
        assert mc.quarantineAndRegister(tid, key1);
        assert mc2.get(key1).equals(val1);
        assert mc.commit(tid, null);        // this function should works
        assert mc2.get(key1) == null;

        // Qref (has val)
        assert mc.iqget(key1) == null;
        assert mc.iqset(key1, val1);
        assert mc.iqget(key1).equals(val1);
        assert mc.quarantineAndRead(key1).equals(val1);
        assert mc2.get(key1).equals(val1);
        mc.swapAndRelease(key1, "new_val");
        assert mc2.get(key1).equals("new_val");

        mc.delete(key1);
        mc.iqget(key1);
        assert mc.get(key1) == null;
        mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        System.out.println("TEST_GET_OK");
    }

    public static void test_set(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String randKey = UUID.randomUUID().toString();
        String key1="key1", val1="val1";

        mc2.disableBackoff();

        // no lease, no val
        assert mc.set(randKey, "randVal");
        assert mc2.get(randKey).equals("randVal");
        mc.delete(key1);
        assert mc.set(key1, val1);
        assert mc2.get(key1).equals(val1);

        // i-lease (no val)
        mc.delete(key1);
        assert mc.iqget(key1) == null;
        mc2.set(key1, val1);
        assert mc2.get(key1).equals(val1);
        assert mc.iqset(key1, "new_val");
        assert mc2.get(key1).equals("new_val");

        // no lease, has val
        mc.set(key1, "new_val_1");
        assert mc2.get(key1).equals("new_val_1");

        // Qinv lease, no val
        mc.delete(key1);
        assert mc2.get(key1) == null;
        String tid = mc.generateSID();
        assert mc.quarantineAndRegister(tid, key1);
        assert mc2.set(key1, val1);
        assert mc2.get(key1).equals(val1);
        assert mc.commit(tid, null);
        assert mc2.get(key1) == null;

        // Qinv lease, has val
        assert mc.set(key1, val1);
        assert mc.get(key1).equals(val1);
        assert mc.quarantineAndRegister(tid, key1);
        assert mc2.set(key1, "new_val");
        assert mc2.get(key1).equals("new_val");
        assert mc.commit(tid, null);
        assert mc.get(key1) == null;

        // Qref (has val)
        assert mc.iqget(key1) == null;
        assert mc.iqset(key1, val1);
        assert mc.quarantineAndRead(key1).equals(val1);
        assert mc2.set(key1, "new_val");
        assert mc2.get(key1).equals("new_val");
        mc.swapAndRelease(key1, "new_val_1");
        assert mc2.get(key1).equals("new_val_1");

        System.out.println("TEST_SET_OK");
    }	

    public static void test_unlease(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", key2="key2";

        mc.delete(key1);
        mc.delete(key2);

        mc.iqget(key1);
        mc.iqget(key2);
        mc.iqset(key2, "val2");

        mc2.quarantineAndRead(key2).equals("val2");

        // this should fail and release all the lease
        try {
            mc.quarantineAndRead(key2);
            assert false;
        } catch (IQException e) {

        }

        try {
            mc.iqset(key1, "val1");		// this should fail because the lease was released
            assert false;
        } catch (IQException e) {}

        assert mc.get(key1) == null;

        mc2.iqget(key1);
        mc2.iqset(key1, "val1");
        assert mc2.iqget(key1).equals("val1");

        mc2.swapAndRelease(key2, "new_val");
        mc2.iqget(key2).equals("new_val");

        System.out.println("TEST_UNLEASE_OK");
    }

    public static void test_QaRead(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", key2="key2";

        // clean cache server
        mc.delete(key1);
        mc.delete(key2);

        // no lease, no value
        assert mc.quarantineAndRead(key1) == null;
        mc.swapAndRelease(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        // no lease, has value
        mc.set(key1, "val1");
        assert mc.quarantineAndRead(key1).equals("val1");
        mc.swapAndRelease(key1, "new_val");
        assert mc.iqget(key1).equals("new_val");

        // has i-lease
        mc.iqget(key2);
        assert mc.quarantineAndRead(key2) == null;
        assert mc.iqset(key2, "temp_val") == false;
        mc.swapAndRelease(key2, "val2");
        assert mc.iqget(key2).equals("val2");

        // has q-lease, no val
        mc.delete(key1);
        assert mc.quarantineAndRead(key1) == null;
        try {
            mc2.quarantineAndRead(key1);
            assert false;
        } catch (IQException e) {

        }
        mc2.swapAndRelease(key1, "temp");
        assert mc2.get(key1) == null;
        mc.swapAndRelease(key1, "val1");
        assert mc.get(key1).equals("val1");

        // has q-lease, has val
        mc.set(key1, "val1");
        assert mc.quarantineAndRead(key1).equals("val1");
        try {
            mc2.quarantineAndRead(key1);
            assert false;
        } catch (IQException e) {

        }
        mc2.swapAndRelease(key1, "temp");
        assert mc2.get(key1).equals("val1");
        mc.swapAndRelease(key1, "newval1");
        assert mc.iqget(key1).equals("newval1");	

        // clean cache server at the end
        mc.delete(key1);
        mc.delete(key2);

        System.out.println("test_QaRead OK");
    }

    public static void test_SaR_HasVal(MemcachedClient mc, MemcachedClient mc2) throws Exception {		
        String key1="key1", key2="key2";

        mc.delete(key1);
        mc.delete(key2);

        // no value, no lease
        mc.quarantineAndRead(key1);
        Thread.sleep(EXP_LEASE_TIME + 1000);					// wait 3 seconds for the q lease to expire		
        mc.swapAndRelease(key1, "val1");
        assert mc.get(key1) == null;
        System.out.println("passed");

        // no value, has i lease
        mc2.quarantineAndRead(key1);
        Thread.sleep(EXP_LEASE_TIME + 1000);					// wait 3 seconds for the q lease to expire	
        assert mc.iqget(key1) == null;		
        mc2.swapAndRelease(key1, "val1");
        assert mc2.get(key1) == null;
        assert mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");
        System.out.println("passed");

        // has value, no lease
        mc.quarantineAndRead(key1);
        Thread.sleep(EXP_LEASE_TIME + 1000);
        mc.swapAndRelease(key1, "new_val");
        assert mc.get(key1) == null;
        System.out.println("passed");

        // q lease, no val
        mc.quarantineAndRead(key2);
        Thread.sleep(EXP_LEASE_TIME + 1000);
        mc2.quarantineAndRead(key2);
        mc.swapAndRelease(key2, "val2");
        assert mc.get(key2) == null;
        mc2.swapAndRelease(key2, "val2");		// no val -> server will not try to store this item
        assert mc.get(key2).equals("val2");
        System.out.println("passed");

        // q lease, has val
        mc.quarantineAndRead(key2);
        Thread.sleep(EXP_LEASE_TIME + 1000);
        mc2.iqget(key2);
        assert mc2.iqset(key2, "val2");
        mc2.quarantineAndRead(key2);
        mc.swapAndRelease(key2, "newval2");
        assert mc.get(key2).equals("val2");
        mc2.swapAndRelease(key2, "newval2");
        assert mc.get(key2).equals("newval2");
        System.out.println("passed");	

        System.out.println("test_SaR_HasVal OK");
    }

    public static void test_SaR_NoVal(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", key2="key2";

        // clean the cache
        mc.delete(key1);
        mc.delete(key2);

        // no lease, no value
        mc.quarantineAndRead(key1);
        Thread.sleep(3000);
        mc.swapAndRelease(key1, null);
        assert mc.get(key1) == null;
        System.out.println("passed");

        // i lease
        mc.quarantineAndRead(key1);
        Thread.sleep(3000);
        mc.iqget(key1);
        mc.swapAndRelease(key1, null);
        assert mc.get(key1) == null;
        mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");
        System.out.println("passed");

        // no lease, has value
        mc.quarantineAndRead(key1);
        Thread.sleep(3000);
        mc.swapAndRelease(key1, null);
        assert mc.get(key1) == null;
        System.out.println("passed");

        // q lease, no val
        mc.delete(key1);
        mc.quarantineAndRead(key1);
        Thread.sleep(3000);
        mc2.quarantineAndRead(key1);
        mc.swapAndRelease(key1, null);
        assert mc.get(key1) == null;
        mc2.swapAndRelease(key1, "val1");
        assert mc.iqget(key1).equals("val1");
        System.out.println("passed");

        // q lease, has val
        mc.quarantineAndRead(key1);
        Thread.sleep(3000);
        mc2.iqget(key1);
        assert mc2.iqset(key1, "val1");
        assert mc2.iqget(key1).equals("val1");
        mc2.quarantineAndRead(key1);
        mc.swapAndRelease(key1, "temp");
        assert mc.iqget(key1).equals("val1");
        mc2.swapAndRelease(key1, null);
        assert mc2.get(key1).equals("val1");
        System.out.println("passed");

        System.out.println("test_SaR_NoVal OK");
    }

    /**
     * Normally, operation should behave like what describes in the state table.
     * However, when operations come from the same thread (same whalin client instance),
     * some operations should behave more flexible.
     * 1. If a QaRead(k) comes and observes that there already has a q lease on
     * k granted to the same client, instead trying backing-off, it will allow
     * the program to proceed (return null but not throw IQ exception)
     * 
     * 2. If an IQGet(k) comes and observes that there already has a i lease or
     * q lease on key k granted to the same client, instead trying to back-off,
     * it will allow the program to proceed (return value if there is value on that
     * item from the cache)
     * 
     */
    public static void test_getlease_same_thread(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1";

        // enable back off on whalin client
        mc.enableBackoff();

        // iqget on i-lease item
        mc.delete(key1);
        mc.iqget(key1);
        mc.iqget(key1);	// should not try back-off here
        mc.iqget(key1);
        mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        // qaread on q-lease item
        assert mc.quarantineAndRead(key1).equals("val1");
        assert mc.quarantineAndRead(key1).equals("val1");		// should return null but not throw exception\
        mc.swapAndRelease(key1, "newval1");
        assert mc.iqget(key1).equals("newval1");

        // iqget on q-lease item
        mc.quarantineAndRead(key1);
        assert mc2.iqget(key1).equals("newval1");
        try {
            mc.iqset(key1, "temp");
        } catch (IQException e) {}
        assert mc2.iqget(key1).equals("newval1");
        mc.swapAndRelease(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        // qaread on i-lease item (handle normally)
        mc.delete(key1);
        mc.iqget(key1);
        assert mc.quarantineAndRead(key1) == null;		// q-lease overrides i-lease
        assert mc.iqset(key1, "temp") == false;			// MUST fail!
        assert mc.iqget(key1) == null;					// no back-off, no value return
        mc.swapAndRelease(key1, "val1");				// successfully swap new item
        assert mc.iqget(key1).equals("val1");

        System.out.println("test_getlease_same_thread OK");
    }

    public static void test_getlease_same_thread_expired(MemcachedClient mc) throws Exception {
        String key1="key1";

        mc.enableBackoff();

        mc.delete(key1);

        // iqget follows iqget expired
        assert mc.iqget(key1) == null;
        Thread.sleep(EXP_LEASE_TIME + 1000);
        assert mc.iqget(key1) == null;
        assert mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        // qaread follows iqget expired
        assert mc.delete(key1);
        assert mc.iqget(key1) == null;
        Thread.sleep(EXP_LEASE_TIME + 1000);
        assert mc.quarantineAndRead(key1) == null;
        mc.swapAndRelease(key1, "newval1");
        assert mc.iqget(key1).equals("newval1");

        // iqget follows qaread expired
        mc.delete(key1);
        assert mc.quarantineAndRead(key1) == null;
        Thread.sleep(EXP_LEASE_TIME + 1000);
        assert mc.iqget(key1) == null;			// should not back-off here
        assert mc.iqset(key1, "val1");
        assert mc.iqget(key1).equals("val1");

        // qaread follows qaread expired
        assert mc.quarantineAndRead(key1).equals("val1");
        Thread.sleep(EXP_LEASE_TIME + 1000); 
        assert mc.quarantineAndRead(key1) == null;
        mc.swapAndRelease(key1, "newval1");
        mc.iqget(key1).equals("newval1");
        mc.swapAndRelease(key1, "newval2");
        mc.iqget(key1).equals("newval1");

        System.out.println("test_getlease_same_thread_expired OK");
    }

    public static void test_getlease_different_thread(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key="key1";

        mc.enableBackoff();
        mc2.enableBackoff();

        // client2 tries qaread on client1's q-item
        mc.delete(key);		
        mc.set(key, "val1");
        mc.quarantineAndRead(key);
        try {
            mc2.quarantineAndRead(key);
            assert false;
        } catch (IQException e) {

        }
        mc.swapAndRelease(key, "val1");
        mc.iqget(key).equals("val1");
        mc.quarantineAndRead(key);
        try {
            mc2.quarantineAndRead(key);
            assert false;
        } catch (IQException e) {

        }
        mc.swapAndRelease(key, "val2");
        mc.iqget(key).equals("val2");	
        System.out.println("passed");

        // client2 tries qaread on client1's i-item
        mc.delete(key);
        mc.iqget(key);
        mc2.quarantineAndRead(key);
        assert mc.iqset(key, "val") == false;
        mc2.swapAndRelease(key, "val1");
        assert mc.get(key).equals("val1");
        System.out.println("passed");

        // client2 tries iqget on client1's q-item
        // should see long waiting time here because iqget back-off
        System.out.println("should observe long waiting time here because iqget back-off");
        mc.delete(key);
        mc.quarantineAndRead(key);
        mc2.iqget(key);
        mc2.iqset(key, "newval1");
        mc.get(key).equals("newval1");
        System.out.println("passed");

        // client2 tries iqget on client1's i-item
        // should observe long waiting time here because iqget back-off
        System.out.println("should observe long waiting time here because iqget back-off");
        mc.delete(key);		
        mc.iqget(key);
        mc2.iqget(key);
        mc2.iqset(key, "val1");
        mc.iqset(key, "temp");
        assert mc.get(key).equals("val1");
        mc2.iqget(key).equals("val1");
        System.out.println("passed");

        System.out.println("test_getlease_different_thread OK");
    }


    /** =============================== **/

    public static void test_delete_leases(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc = new MemcachedClient("test");
        mc2 = new MemcachedClient("test");
        String key1="key1";

        mc.iqget(key1);

        mc2.iqget("key2");

        mc.iqget("key3");
        mc.iqget("key4");

        System.out.println("Test OK");
    }

    public static void test_stale_data_1(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key1="key1", key2="key2";

        mc.disableBackoff();
        mc.delete(key1);
        mc.delete(key2);

        mc.quarantineAndRead(key1);
        mc2.quarantineAndRead(key2);

        try {
            mc.quarantineAndRead(key2);
            assert false; 
        } catch (IQException e) {}
        mc.swapAndRelease(key1, "old_val1");
        mc2.swapAndRelease(key2, "val2");

        assert mc.iqget(key1).equals("old_val1");
        assert mc.iqget(key2).equals("val2");
    }

    public static void test_iqget() throws Exception {
        MemcachedClient mc = new MemcachedClient("test");
        MemcachedClient mc2 = new MemcachedClient("test");

        String key = "key";
        String tid;

        // no lease, no val
        mc.delete(key);
        assert mc.iqget(key) == null;		// should get the lease
        assert mc.iqset(key, "val");
        assert mc.iqget(key).equals("val");

        // no lease, has val
        assert mc.iqget(key).equals("val");

        // has i lease, should backoff
        assert mc.delete(key);
        assert mc.iqget(key) == null;
        assert mc.iqget(key) == null;		// iqget same session
        assert mc2.iqget(key) == null;		// back-off here
        assert mc2.iqset(key, "val");

        // has inv lease, no value
        assert mc.delete(key);
        tid = mc.generateSID(); 
        mc.quarantineAndRegister(tid, key);
        assert mc.iqget(key) == null;		// iqget same session
        assert mc2.iqget(key) == null;		// back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "val");

        // has inv lease, has value
        mc.delete(key);
        mc.set(key, "val");
        assert mc.get(key).equals("val");
        tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key);
        assert mc.iqget(key) == null;			// read same session
        assert mc2.iqget(key).equals("val");	// read different session
        assert mc.commit(tid, null);

        // has ref lease, no val
        mc.delete(key);
        mc.quarantineAndRead(key);
        assert mc.iqget(key) == null;		// iqget same session
        assert mc2.iqget(key) == null;		// back-off here
        mc.swapAndRelease(key, "val");
        assert mc2.iqset(key, "val2");
        assert mc.iqget(key).equals("val2");

        // has ref lease, has val
        mc.delete(key);
        mc.set(key, "val");
        assert mc.get(key).equals("val");
        mc.quarantineAndRead(key);
        assert mc.iqget(key) == null;			// iqget same session
        assert mc2.iqget(key).equals("val");	// iqget different session
        mc.swapAndRelease(key, "val2");
        assert mc.iqget(key).equals("val2");

        // inc lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        assert mc.iqappend(key, "val", tid);
        assert mc.iqget(key) == null;	// no back-off
        assert mc2.iqget(key) == null;	// back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "newval");

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "val");
        assert mc.get(key).equals("val");
        tid = mc.generateSID();
        assert mc.iqappend(key, "app", tid);
        assert mc.iqget(key).equals("valapp");	// read same session
        assert mc2.iqget(key).equals("val");	// read different session
        assert mc.commit(tid, null);

        // inc lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        assert mc.iqincr(key, 5, tid) == true;
        assert mc.iqget(key) == null;	// read same session, no back-off
        assert mc2.iqget(key) == null;	// read different session, back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "newval");

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "7");
        assert mc.get(key).equals("7");
        tid = mc.generateSID();
        assert mc.iqincr(key, 5, tid) == true;
        assert mc.iqget(key).equals("12");	// read same session, observer mid-flight update
        assert mc2.iqget(key).equals("7");	// read different session
        assert mc.commit(tid, null);	

        System.out.println("TEST_IQGET_OK");
    }

    public static void test_iqset() throws Exception {
        MemcachedClient mc = new MemcachedClient("test");
        MemcachedClient mc2 = new MemcachedClient("test");

        String key = "key";
        String tid;

        // no lease, no val
        mc.delete(key);
        try { mc.iqset(key, "val"); assert false; } catch (Exception e) {}

        // no lease, has val
        mc.delete(key);
        mc.set(key, "val");
        mc.get(key).equals("val");
        try { mc.iqset(key, "val"); assert false; } catch (Exception e) {}

        // has i lease, no val
        mc.delete(key);
        assert mc.iqget(key) == null;
        try { mc2.iqset(key, "val"); assert false; } catch (Exception e) {}
        assert mc.iqset(key, "val2");
        assert mc.iqget(key).equals("val2");

        // inv lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key);
        assert mc.iqget(key) == null;
        assert mc2.iqget(key) == null;		// back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "val");
        assert mc.iqget(key).equals("val");

        // inv lease, has val
        mc.delete(key);
        mc.set(key, "val1");
        tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key);
        assert mc.iqget(key) == null;
        assert mc2.iqget(key).equals("val1");
        assert mc.commit(tid, null);
        assert mc.iqget(key) == null;

        // ref lease, no val
        mc.delete(key);
        assert mc.quarantineAndRead(key) == null;
        assert mc.iqget(key) == null;
        assert mc2.iqget(key) == null;	// back-off here
        mc.swapAndRelease(key, "val1");
        assert mc2.iqset(key, "val2");
        assert mc.iqget(key).equals("val2");

        // ref lease, has val
        mc.delete(key);
        mc.set(key, "val");
        assert mc.quarantineAndRead(key).equals("val");
        assert mc.iqget(key) == null;
        assert mc2.iqget(key).equals("val");
        mc.swapAndRelease(key, "val2");
        assert mc.iqget(key).equals("val2");

        // inc lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        mc.iqappend(key, "val", tid);
        assert mc.iqget(key) == null;
        assert mc2.iqget(key) == null;	// back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "val2");
        assert mc.iqget(key).equals("val2");

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "val");
        tid = mc.generateSID();
        mc.iqappend(key, "app", tid);
        assert mc.iqget(key).equals("valapp");
        assert mc2.iqget(key).equals("val");
        assert mc.commit(tid, null);
        assert mc2.iqget(key).equals("valapp");

        mc.delete(key);
        tid = mc.generateSID();
        mc.iqappend(key, "val", tid);
        assert mc.iqget(key) == null;
        assert mc2.iqget(key) == null;	// back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "val2");
        assert mc.iqget(key).equals("val2");

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "val");
        tid = mc.generateSID();
        mc.iqappend(key, "app", tid);
        assert mc.iqget(key).equals("valapp");
        assert mc2.iqget(key).equals("val");
        assert mc.commit(tid, null);
        assert mc2.iqget(key).equals("valapp");

        // inc lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        assert mc.iqincr(key, 5, tid) == true;
        assert mc.iqget(key) == null;	// read same session, no back-off
        assert mc2.iqget(key) == null;	// read different session, back-off here
        assert !mc.commit(tid, null);
        assert mc2.iqset(key, "7");
        assert mc.iqget(key).equals("7");

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "7");
        assert mc.get(key).equals("7");
        tid = mc.generateSID();
        assert mc.iqincr(key, 5, tid) == true;
        assert mc.iqget(key).equals("12");	// read same session, observer mid-flight update
        assert mc2.iqget(key).equals("7");	// read different session
        assert mc.commit(tid, null);	
        assert mc2.iqget(key).equals("12");

        System.out.println("TEST_IQSET_OK");
    }	

    public static void test_quarantine_and_register() throws Exception {
        String key = "key";
        String tid;

        MemcachedClient mc = new MemcachedClient("test");
        MemcachedClient mc2 = new MemcachedClient("test");

        // no lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key);
        assert mc.get(key) == null;
        mc.commit(tid, null);
        assert mc2.iqget(key) == null;

        // no lease, has val
        mc.delete(key);
        mc.set(key, "val");
        tid = mc.generateSID();
        mc.quarantineAndRegister(tid, key);
        mc.iqget(key).equals("val");
        mc2.iqget(key).equals("val");
        mc.deleteAndRelease(tid);
        assert mc2.iqget(key) == null;

        // has I lease
        mc.delete(key);
        assert mc.iqget(key) == null;		// I lease is granted here
        tid = mc2.generateSID();
        mc2.quarantineAndRegister(tid, key);	// Q lease voids I lease
        assert mc2.iqget(key) == null;
        assert mc.iqset(key, "val") == false;
    }

    public static void test_delete_and_release(){}

    public static void test_iqappend() throws Exception {
        String key = "key", val1 = "val1", val2 = "val2", val3 = "val3";

        MemcachedClient mc = new MemcachedClient("test");
        MemcachedClient mc2 = new MemcachedClient("test");

        String tid;
        String tid2;

        // item has no lease, no value
        mc.delete(key);
        tid = mc.generateSID();
        assert mc.iqappend(key, val1, tid);
        assert mc.iqget(key) == null;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);

        // item has I lease, no value
        mc.delete(key);
        mc.iqget(key);		// now key has i lease
        assert mc.iqappend(key, val1, tid);		// q lease should void i lease
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);
        assert mc.get(key) == null;

        // item has value, no lease
        mc.delete(key);
        mc.set(key, val1);
        tid = mc.generateSID();
        assert mc.iqappend(key, val2, tid);
        assert mc.iqappend(key, val3, tid);
        assert mc.commit(tid, null);
        assert mc.get(key).equals(val1 + val2 + val3);

        // item has no value, q inv lease
        mc.delete(key);
        mc.quarantineAndRegister(tid, key);
        assert mc.iqappend(key, val1, tid);
        assert mc.iqappend(key, val2, tid);
        assert mc.commit(tid, null);
        assert mc.get(key) == null;

        // item has value, q inv lease
        mc.delete(key);
        mc.set(key, val1);
        mc.quarantineAndRegister(tid, key);
        assert !mc2.iqappend(key, val2, UUID.randomUUID().toString());
        assert mc.iqappend(key, val3, tid);
        assert mc.get(key).equals(val1);
        assert mc.commit(tid, null);
        assert mc.get(key) == null;

        // ref lease, no val
        mc.delete(key);
        assert mc.quarantineAndRead(key) == null;
        tid = mc.generateSID();
        assert !mc.iqappend(key, "val", tid);
        tid2 = mc2.generateSID();
        assert !mc2.iqappend(key, "val2", tid2);	// back-off here
        assert !mc.commit(tid, null);
        mc.swapAndRelease(key, "test");
        assert mc.get(key).equals("test");
        assert !mc2.commit(tid2, null);
        assert mc.iqget(key).equals("test");

        // ref lease, has val
        mc.delete(key);
        mc.set(key, "val");
        assert mc.quarantineAndRead(key).equals("val");
        tid = mc.generateSID();
        assert !mc.iqappend(key, "val1", tid);
        tid2 = mc2.generateSID();
        assert !mc2.iqappend(key, "val2", tid2);	// back-off here
        assert !mc.commit(tid, null);
        mc.swapAndRelease(key, "test");
        assert mc.get(key).equals("test");
        assert !mc2.commit(tid2, null);
        assert mc.iqget(key).equals("test");		

        // inc lease, no val
        mc.delete(key);
        tid = mc.generateSID();
        assert mc.iqappend(key, "val1", tid);
        tid2 = mc2.generateSID();
        assert !mc2.iqappend(key, "val2", tid2);	// back-off here
        assert mc.commit(tid, null);
        assert !mc2.commit(tid, null);
        assert mc.iqget(key) == null;

        // inc lease, has val
        mc.delete(key);
        mc.set(key, "val");
        tid = mc.generateSID();
        assert mc.iqappend(key, "val1", tid);
        tid2 = mc2.generateSID();
        assert !mc2.iqappend(key, "val2", tid2);	// back-off here
        assert mc2.iqget(key).equals("val");
        assert mc.commit(tid, null);
        assert !mc2.commit(tid, null);
        assert mc.iqget(key).equals("valval1");

        System.out.println("IQAPPEND_OK");
    }	

    public static void test_iqprepend(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "key", val1 = "val1", val2 = "val2", val3 = "val3";

        mc.delete(key);
        mc.disableBackoff();

        // item has no lease, no value
        String tid = mc.generateSID();
        assert mc.iqprepend(key, val1, tid);
        assert mc.iqprepend(key, val2, tid);
        assert mc.iqget(key) == null;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);

        // item has I lease, no value
        mc.delete(key);
        mc.iqget(key);		// now key has i lease
        assert mc.iqprepend(key, val1, tid);		// q lease should void i lease
        assert mc.iqprepend(key, val2, tid);
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);

        // item has value, no lease
        mc.delete(key);
        mc.iqget(key);
        assert mc.iqset(key, val1);
        assert mc.iqprepend(key, val2, tid);
        assert mc.iqprepend(key, val3, tid);
        assert mc.commit(tid, null);
        assert mc.get(key).equals(val3 + val2 + val1);

        // item has no value, q inv lease
        mc.delete(key);
        mc.quarantineAndRegister(tid, key);
        assert mc.iqprepend(key, val1, tid);
        assert mc.iqprepend(key, val2, tid);
        assert !mc.commit(tid, null);

        // item has value, q inv lease
        mc.delete(key);
        mc.set(key, val1);
        mc.quarantineAndRegister(tid, key);
        assert !mc2.iqprepend(key, val2, tid);
        assert mc.iqprepend(key, val3, tid);
        assert !mc.commit(tid, null);
        assert mc.get(key).equals(val1);
        assert mc.commit(tid, null);
        assert mc.get(key) == null;

        System.out.println("IQPREPEND_OK");
    }		

    public static void test_iqincr(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "key", val1 = "5";
        long val2 = 6, val3 = 7;

        mc.delete(key);

        // item has no lease, no value
        String tid = mc.generateSID();
        assert mc.iqincr(key, val2, tid) == true;
        assert mc.iqincr(key, val3, tid) == true;
        assert mc.iqget(key) == null;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);
        assert mc.get("nv:key") == null;

        // item has I lease, no value
        mc.delete(key);
        mc.iqget(key);		// now key has i lease
        assert mc.iqincr(key, val2, tid) == true;		// q lease should void i lease
        assert mc.iqincr(key, val3, tid) == true;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);
        assert mc.get("nv:key") == null;

        // item has value, no lease
        mc.delete(key);
        mc.iqget(key);
        assert mc.iqset(key, val1);
        assert mc.iqget(key).equals(val1);
        assert mc.iqincr(key, val2, tid) == true;
        assert mc.iqget(key).equals("11");
        assert mc.iqincr(key, val3, tid) == true;
        assert mc.commit(tid, null);
        assert mc.get(key).equals("18");
        assert mc.get("nv:key") == null;

        // item has no value, q inv lease
        mc.delete(key);
        mc.quarantineAndRegister(tid, key);
        assert mc.iqincr(key, val2, tid) == true;
        assert mc.iqincr(key, val3, tid) == true;
        assert mc.commit(tid, null);
        assert mc.get("nv:key") == null;

        // item has value, q inv lease
        mc.delete(key);
        mc.set(key, val1);
        mc.quarantineAndRegister(tid, key);
        assert mc.iqincr(key, val2, UUID.randomUUID().toString()) == false;
        assert mc.iqincr(key, val2, tid) == true;
        assert mc.iqincr(key, val3, tid) == true;
        assert mc.get(key).equals(val1);		
        assert mc.commit(tid, null);

        // item has value, q incr lease
        mc.delete(key);
        mc.set(key, val1);
        assert mc.iqincr(key, val2, tid) == true;
        assert mc2.iqget(key).equals(val1);
        assert mc.iqget(key).equals("11");
        String tid2 = UUID.randomUUID().toString();
        assert mc2.iqincr(key, val3, tid2) == false;
        assert mc2.iqget(key).equals(val1);
        assert mc.iqget(key).equals("11");
        assert !mc2.commit(tid2, null);
        assert mc.commit(tid, null);
        assert mc2.iqget(key).equals("11");

        System.out.println("IQINCR_OK");
    }			

    public static void test_iqdecr(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        String key = "key", val1 = "12";
        long val2 = 2, val3 = 3;

        mc.delete(key);
        mc.disableBackoff();

        // item has no lease, no value
        String tid = mc.generateSID();
        assert mc.iqdecr(key, val2, tid) == true;
        assert mc.iqdecr(key, val3, tid) == true;
        assert mc.iqget(key) == null;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);

        // item has I lease, no value
        mc.delete(key);
        mc.iqget(key);		// now key has i lease
        assert mc.iqdecr(key, val2, tid) == true;		// q lease should void i lease
        assert mc.iqdecr(key, val3, tid) == true;
        assert mc.iqset(key, val1) == false;
        assert mc.commit(tid, null);

        // item has value, no lease
        mc.delete(key);
        mc.iqget(key);
        assert mc.iqset(key, val1);
        assert mc.iqdecr(key, val2, tid) == true;
        assert mc.iqdecr(key, val3, tid) == true;
        assert mc.commit(tid, null);
        assert mc.get(key).equals("7");

        // item has no value, q inv lease
        mc.delete(key);
        mc.quarantineAndRegister(tid, key);
        assert mc.iqdecr(key, val2, tid) == false;
        assert mc.iqdecr(key, val3, tid) == false;
        assert !mc.commit(tid, null);
        mc.commit(tid, null);

        // item has value, q inv lease
        mc.delete(key);
        mc.set(key, val1);
        mc.quarantineAndRegister(tid, key);
        assert mc2.iqdecr(key, val2, tid) == false;
        assert mc.iqdecr(key, val3, tid) == true;
        assert !mc.commit(tid, null);
        assert mc.get(key).equals(val1);
        assert mc.commit(tid, null);
        assert mc.get(key) == null;

        System.out.println("IQDECR_OK");
    }				

    //	public static void testCOLeases1(MemcachedClient mc, 
    //			MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid1 = mc.generateSID();
    //		assert mc.regC(sid1, "key1");
    //		assert mc.iqget("key1").equals("val1");
    //		
    //		assert mc.regC(sid1, "key2");
    //		assert mc.iqget("key2") == null;
    //		assert mc.iqset("key2", "val2");
    //		
    //		assert mc.regC(sid1, "key3");
    //		assert mc.iqget("key3").equals("val3");
    //		
    //		// another read session interleaves
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regC(sid2, "key1");	// success because key1 has value
    //		assert mc2.iqget("key1").equals("val1");
    //		
    //		assert mc2.regC(sid2, "key2"); 
    //		assert mc2.iqget("key2").equals("val2");
    //		
    //		assert mc2.commitCO(sid2);	// should success
    //		
    //		assert mc.commitCO(sid1);
    //	} 

    //	public static void testCOLeases2(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid1 = mc.generateSID();
    //		assert mc.regO(sid1, "key1");
    //		assert mc.regO(sid1, "key2");
    //		assert mc.regO(sid1, "key3");
    //		
    //		String tid1 = mc.generateID();
    //		assert mc.quarantineAndRegister(tid1, "key1");
    //		assert mc.quarantineAndRegister(tid1, "key2");
    //		assert mc.quarantineAndRegister(tid1, "key3");		
    //		assert mc.commit(tid1);
    //		
    //		assert mc.commitCO(sid1);
    //	}

    //	public static void testCOLeases3(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		// sess 1 reads data
    //		String sid = mc.generateSID();
    //		assert mc.regC(sid, "key1");
    //		assert mc.regC(sid, "key1");	// reg c same sess, should success
    //		assert mc.iqget("key1").equals("val1");
    //		
    //		assert mc.regC(sid, "key2");
    //		assert mc.iqget("key2") == null;
    //		
    //		// sess 2 invalidates key 2
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key2");
    //		
    //		assert mc.iqset("key2", "val2");	// still success
    //		
    //		try {
    //			mc.regC(sid, "key3");
    //			assert false;
    //		} catch (COException e) {}
    //		
    //		
    //		String tid2 = mc2.generateID();
    //		mc2.quarantineAndRegister(tid2, "key2");
    //		mc2.commit(tid2, null);
    //		
    //		assert mc2.commitCO(sid2);	// should success
    //	}

    //	public static void testCOLeases4(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid = mc.generateSID();
    //		assert mc.regC(sid, "key1");
    //		assert mc.iqget("key1").equals("val1");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key2");
    //		
    //		try {
    //			mc.regC(sid, "key2");
    //			assert false;
    //		} catch (COException e) {			
    //		}
    //		
    //		String tid = mc2.generateID();
    //		assert mc2.quarantineAndRegister(tid, "key2");
    //		assert mc2.deleteAndRelease(tid);
    //		
    //		assert mc2.commitCO(sid2);
    //	}

    //	public static void testCOLeases5(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid = mc.generateSID();
    //		assert mc.regO(sid, "key1");
    //		assert mc.regO(sid, "key2");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key2");
    //		assert mc2.regO(sid2, "key3");
    //		
    //		String tid = mc.generateID();
    //		assert mc.quarantineAndRegister(tid, "key1");
    //		assert mc.quarantineAndRegister(tid, "key2");
    //		
    //		String tid2 = mc2.generateID();
    //		assert mc2.quarantineAndRegister(tid2, "key2");
    //		assert mc2.quarantineAndRegister(tid2, "key3");
    //		
    //		assert mc.deleteAndRelease(tid);
    //		assert mc2.deleteAndRelease(tid2);
    //		
    //		assert mc.commitCO(sid);
    //		assert mc2.commitCO(sid2);
    //	}

    //	public static void testCOLeases6(MemcachedClient mc, MemcachedClient mc2, MemcachedClient mc3) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		String sid = mc.generateSID();
    //		assert mc.regC(sid, "key1");
    //		assert mc.iqget("key1") == null;
    //		assert mc.iqset("key1", "val1");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key1");
    //		assert mc2.quarantineAndRegister("tidx", "key1");
    //		assert mc2.deleteAndRelease("tidx");
    //		assert mc2.commitCO(sid2);
    //		
    //		String sid3 = mc3.generateSID();
    //		assert mc3.regC(sid3, "key1");
    //		assert mc3.iqget("key1") == null;
    //		assert mc3.iqset("key1", "val2");
    //		
    //		try {
    //			mc.commitCO(sid);
    //			assert false;
    //		} catch (COException e) { }
    //		
    //		assert mc3.commitCO(sid3);
    //	}

    //	public static void testCILeases1(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid1 = mc.generateSID();
    //		assert mc.ciget(sid1, "key1").equals("val1");		
    //		
    //		assert mc.ciget(sid1, "key2") == null;
    //		assert mc.iqset("key2", "val2");
    //		
    //		assert mc.ciget(sid1, "key3").equals("val3");
    //		
    //		// another read session interleaves
    //		String sid2 = mc2.generateSID();	
    //		assert mc2.ciget(sid2, "key1").equals("val1");
    //		
    //		assert mc2.ciget(sid2, "key2").equals("val2");
    //		
    //		assert mc2.commitCO(sid2);	// should success
    //		
    //		assert mc.commitCO(sid1);		
    //	}

    //	public static void testCILeases2(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		// sess 1 reads data
    //		String sid = mc.generateSID();		
    //		assert mc.ciget(sid, "key1").equals("val1");
    //		assert mc.ciget(sid, "key1").equals("val1"); // reg c same sess, should success
    //		
    //		assert mc.ciget(sid, "key2") == null;
    //		
    //		// sess 2 invalidates key 2
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key2");
    //		
    //		assert mc.iqset("key2", "val2");	// still success
    //		
    //		try {
    //			mc.ciget(sid, "key3");
    //			assert false;
    //		} catch (Exception e) {}
    //		
    //		assert mc.get("co:key1") == null;
    //		assert mc.get("co:key2") == null;
    //		
    //		String tid2 = mc2.generateID();
    //		mc2.quarantineAndRegister(tid2, "key2");
    //		mc2.commit(tid2, null);
    //		
    //		assert mc2.commitCO(sid2);	// should success
    //	}

    //	public static void testCILeases3(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		mc.set("key3", "val3");
    //		
    //		String sid = mc.generateSID();
    //		mc.disableBackoff();
    //		assert mc.ciget(sid, "key1").equals("val1");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key2");
    //		
    //		try {
    //			mc.ciget(sid, "key2");
    //			assert false;
    //		} catch (Exception e) {			
    //		}
    //		
    //		assert mc.get("co:key1") == null;
    //		
    //		String tid = mc2.generateID();
    //		assert mc2.quarantineAndRegister(tid, "key2");
    //		assert mc2.deleteAndRelease(tid);
    //		
    //		assert mc2.commitCO(sid2);
    //	}

    //	public static void testCILeases4(MemcachedClient mc, MemcachedClient mc2, MemcachedClient mc3) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		String sid = mc.generateSID();
    ////		assert mc.regC(sid, "key1");
    //		assert mc.ciget(sid, "key1") == null;
    //		assert mc.iqset("key1", "val1");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.regO(sid2, "key1");
    //		assert mc2.quarantineAndRegister("tidx", "key1");
    //		assert mc2.deleteAndRelease("tidx");
    //		assert mc2.commitCO(sid2);
    //		
    //		String sid3 = mc3.generateSID();
    ////		assert mc3.regC(sid3, "key1");
    //		assert mc3.ciget(sid3, "key1") == null;
    //		assert mc3.iqset("key1", "val2");
    //		
    //		try {
    //			mc.commitCO(sid);
    //			assert false;
    //		} catch (COException e) { }
    //		
    //		assert mc3.commitCO(sid3);
    //	}

    public static void testCO1(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        mc.set("key1", "val1");
        mc.set("key3", "val3");

        String sid1 = mc.generateSID();
        assert mc.oqReg(sid1, "key1");
        assert mc.oqReg(sid1, "key2");
        assert mc.oqReg(sid1, "key3");

        assert mc.dCommit(sid1);
    }

    public static void testCO2(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        mc.set("key1", "val1");
        mc.set("key3", "val3");

        // sess 1 reads data
        String sid = mc.generateSID();		
        assert mc.ciget(sid, "key1").equals("val1");
        assert mc.ciget(sid, "key1").equals("val1"); // reg c same sess, should success

        assert mc.ciget(sid, "key2") == null;

        // sess 2 invalidates key 2
        String sid2 = mc2.generateSID();
        assert mc2.oqReg(sid2, "key2");

        assert mc.iqset("key2", "val2") == false;	// still success

        try {
            mc.ciget(sid, "key3");
            assert false;
        } catch (Exception e) {}

        assert mc.get("co:key1") == null;
        assert mc.get("co:key2") == null;

        assert mc2.dCommit(sid2);	// should success
    }

    public static void testCO3(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        mc.set("key1", "val1");
        mc.set("key3", "val3");

        String sid = mc.generateSID();
        mc.disableBackoff();
        assert mc.ciget(sid, "key1").equals("val1");

        String sid2 = mc2.generateSID();
        assert mc2.oqReg(sid2, "key2");

        try {
            mc.ciget(sid, "key2");
            assert false;
        } catch (Exception e) {			
        }

        assert mc.get("co:key1") == null;
        assert mc.get("co:key2") == null;		

        assert mc2.dCommit(sid2);
    }

    //	public static void testCO4(MemcachedClient mc, MemcachedClient mc2, MemcachedClient mc3) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		String sid = mc.generateSID();
    //		assert mc.ciget(sid, "key1") == null;
    //		assert mc.iqset("key1", "val1");
    //		
    //		String sid2 = mc2.generateSID();
    //		assert mc2.oqReg(sid2, "tidx", "key1");
    //		assert mc2.dCommit(sid2, "tidx");
    //		
    //		String sid3 = mc3.generateSID();
    //		assert mc3.ciget(sid3, "key1") == null;
    //		assert mc3.iqset("key1", "val2");
    //		
    //		try {
    //			mc.commitCO(sid);
    //			assert false;
    //		} catch (COException e) { }
    //		
    //		assert mc3.commitCO(sid3);
    //	}

    public static void testCO5(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        mc.set("key1", "val1");
        mc.set("key3", "val3");

        String sid = mc.generateSID();
        assert mc.oqReg(sid, "key1");
        assert mc.oqReg(sid, "key2");

        String sid2 = mc2.generateSID();
        assert mc2.oqReg(sid2, "key2");
        assert mc2.oqReg(sid2, "key3");

        assert mc.dCommit(sid);
        assert mc.dCommit(sid2);
    }

    //	public static void testCOSameSess1(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		
    //		String sid = mc.generateSID();
    //		String tid = mc.generateID();
    //		assert mc.ciget(sid, "key1").equals("val1");
    //		assert mc.ciget(sid, "key2") == null;
    //		
    //		String sid2 = mc2.generateSID();
    //		mc2.disableBackoff();
    //		assert mc2.ciget(sid2, "key1").equals("val1");
    //		
    //		try {
    //			assert mc2.ciget(sid2, "key2") == null;
    //		} catch (COException e) {
    //			
    //		}
    //		
    //		assert mc.oqReg(sid, tid, "key3");
    //		assert mc.iqset("key2", "val2");
    //		assert mc2.commitCO(sid2);
    //		assert mc.dCommit(sid, tid);
    //	}

    //	public static void testCOSameSess2(MemcachedClient mc, MemcachedClient mc2) throws Exception {
    //		mc.delete("key1");
    //		mc.delete("key2");
    //		mc.delete("key3");
    //		
    //		mc.set("key1", "val1");
    //		
    //		String sid = mc.generateSID();
    //		String tid = mc.generateID();
    //		assert mc.ciget(sid, "key1").equals("val1");
    //		assert mc.ciget(sid, "key2") == null;
    //		
    //		String sid2 = mc2.generateSID();
    //		mc2.disableBackoff();
    //		assert mc2.ciget(sid2, "key1").equals("val1");
    //		
    //		assert mc.oqReg(sid, tid, "key1");
    //		assert mc.iqset("key2", "val2");
    //		assert mc.ciget(sid, "key1").equals("val1");
    //		assert mc.ciget(sid, "key2").equals("val2");
    //		try {
    //			mc2.commitCO(sid2);
    //			assert false;
    //		} catch (COException e) {}
    //		
    //		try {
    //			mc2.ciget(sid2, "key1");
    //			assert false;
    //		} catch (COException e) {
    //			
    //		}
    //		
    //		assert mc.dCommit(sid, tid);
    //	}

    public static void testCOSameSess3(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc.delete("key2");

        String sid = mc.generateSID();

        assert mc.oqReg(sid, "key1");
        assert mc.ciget(sid, "key1") == null;
        assert mc.dCommit(sid);
    }

    public static void testCOUnlease(MemcachedClient mc) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.set("key1", "val1");

        String sid = mc.generateSID();

        assert mc.oqReg(sid, "key1");
        assert mc.ciget(sid, "key2") == null;

        // unlease co
        assert mc.unleaseCO(sid);

        try {
            assert mc.iqset("key2", "val2") == false;
        } catch (Exception e) {

        }

        mc.delete("key1");
        mc.delete("key2");
        mc.set("key1", "val1");

        sid = mc.generateSID();
        //		String tid = mc.generateID();

        assert mc.ciget(sid, "key1").equals("val1");
        assert mc.ciget(sid, "key2") == null;

        // unlease co
        assert mc.unleaseCO(sid);

        try {
            assert mc.iqset("key2", "val2") == false;
        } catch (Exception e) {

        }
    }

    public static void testCOUnlease2(MemcachedClient mc) throws Exception {
        mc.delete("key1");
        mc.delete("key2");		

        mc.set("key1", "val1");

        String sid = mc.generateSID();
        assert mc.ciget(sid, "key1").equals("val1");
        assert mc.ciget(sid, "key2") == null;

        assert mc.unleaseCO(sid);
    }

    public static void testUnlease(MemcachedClient mc) throws Exception {
        mc.set("key1", "value1");
        mc.quarantineAndRegister("abc", "key1");
        mc.quarantineAndRegister("abc", "key2");
        mc.quarantineAndRegister("abc", "key3");
        mc.release("abc");
        System.out.println(mc.get("key1"));
    }

    public static void testPendingTrans(MemcachedClient mc) throws Exception {
        mc.disableBackoff();
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        mc.set("key2", "val2");
        assert mc.quarantineAndRead("1", "key1", 1, false) == null;
        assert mc.iqappend("key3", "5", "1") == null;
        mc.swapAndRelease("key1", null);
        mc.commit("1", null);
        assert mc.iqget("key1") == null;
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        keyMap.put("key3", 1);
        mc.finishTrans("1", keyMap);
        assert mc.iqget("key1") == null;
        assert mc.iqset("key1", "val1");
        assert mc.iqget("key1").equals("val1");
        assert mc.iqget("key3") == null;
        assert mc.iqset("key3", "val3");
        assert mc.iqget("key3").equals("val3");

        System.out.println("testPendingTrans OK");
    }

    public static void testPendingTrans2(MemcachedClient mc) throws Exception {
        mc.disableBackoff();
        mc.delete("key1");
        mc.delete("key2");

        mc.set("key2", "val2");
        assert mc.iqappend("key2", "3", "1");
        assert mc.commit("1", null);
        assert mc.iqget("key2").equals("val23");

        System.out.println("testPendingTrans2 OK");
    }

    public static void testPendingTrans3(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.disableBackoff();
        //		mc2.disableBackoff();
        String tid = "tid1";

        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");

        assert mc.iqappend("key1", "3", tid) == null;
        assert mc.iqincr("key2", 3, tid) == null;
        assert mc.quarantineAndRead(tid, "key3", null, false) == null;
        assert mc.commit(tid, null);
        mc.swapAndRelease("key3", null);

        tid = "tid2";
        assert mc.quarantineAndRead(tid, "key1", null, false) == null;
        assert mc.iqprepend("key2", 2, tid) == null;
        assert mc.iqdecr("key3", 5, tid) == null;
        assert mc.commit(tid, null);
        mc.swapAndRelease("key1", null);

        tid = "tid3";
        assert mc.quarantineAndRead(tid, "key1", null, false) == null;
        assert mc.iqprepend("key2", 2, tid) == null;
        assert mc.iqdecr("key3", 5, tid) == null;
        assert mc.commit(tid, null);
        mc.swapAndRelease("key1", null);

        assert mc.iqget("key1") == null;
        assert mc.iqget("key2") == null;
        assert mc.iqget("key3") == null;

        try { mc.iqset("key1", "val1"); assert false; } catch (Exception e) { }
        try { mc.iqset("key2", "val2"); assert false; } catch (Exception e) { }
        try { mc.iqset("key3", "val3"); assert false; } catch (Exception e) { }

        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1); keyMap.put("key2", 1); keyMap.put("key3", 1);

        mc.finishTrans("tid1", keyMap);	
        assert mc.iqget("key1") == null; assert mc.iqget("key2") == null; assert mc.iqget("key3") == null;
        try { mc.iqset("key1", "val1"); assert false; } catch (Exception e) { }
        try { mc.iqset("key2", "val2"); assert false; } catch (Exception e) { }
        try { mc.iqset("key3", "val3"); assert false; } catch (Exception e) { }

        mc.finishTrans("tid3", keyMap);
        assert mc.iqget("key1") == null; assert mc.iqget("key2") == null; assert mc.iqget("key3") == null;
        try { mc.iqset("key1", "val1"); assert false; } catch (Exception e) { }
        try { mc.iqset("key2", "val2"); assert false; } catch (Exception e) { }
        try { mc.iqset("key3", "val3"); assert false; } catch (Exception e) { }

        mc.finishTrans("tid2", keyMap);

        assert mc2.iqget("key1") == null; 
        assert mc2.iqget("key2") == null; 
        assert mc2.iqget("key3") == null;
        assert mc2.iqset("key1", "val2"); assert mc2.iqset("key2", "val3"); assert mc2.iqset("key3", "val1");
        assert mc2.iqget("key1").equals("val2");
        assert mc2.iqget("key2").equals("val3");
        assert mc2.iqget("key3").equals("val1");

        System.out.println("testPendingTrans3 OK");
    }
    
    public static void testAppendAddFail(MemcachedClient mc) throws Exception {
        while (true) {
            try {
                String sid = mc.generateSID();
                boolean success = mc.oqAppend(sid, "key1", 0, "test".getBytes(), false);
                if (!success) {
                    success = mc.oqAdd(sid, "key1", "test".getBytes(), 0, false);
                    if (!success) {
                        mc.oqRead(sid, "key1", 0, false);
                        mc.oqSwap(sid, "key1", 0, "test".getBytes(), false);
                    }
                }
                mc.dCommit(sid);
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
    }
    
    public static void testRangeBW(MemcachedClient mc) throws COException, IOException {
        String sid = mc.generateSID();
        assert !mc.oqAppend(sid, "rbw_name_min_max,1,33,33,xxxx", 0, "test".getBytes(), false);
        assert mc.oqAdd(sid, "rbw_name_min_max,1,33,33,xxxx", "test".getBytes(), 0, false);
        mc.dCommit(sid);
    }
    
    private static byte[] serializeList(List<Integer> val) {
        if (val == null || val.size() == 0) return null;
        ByteBuffer bf = ByteBuffer.allocate(val.size()*4);
        for (Integer v: val) {
            bf.putInt(v);
        }
        return bf.array();
    }
    
    public static List<Integer> deserialize(byte[] bytes) {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        List<Integer> list = new ArrayList<>();
        int offset = 0;
        while (offset < bytes.length) {
            int o_id = buff.getInt();
            list.add(o_id);
            offset+=4;
        }
        return list;
    }
    
    public static void testAppend(MemcachedClient mc) throws COException, IOException {
        
        List<Integer> x = new ArrayList<>();
        x.add(5); x.add(7);
        byte[] bytes = serializeList(x);
        mc.set("key1", bytes, false);
        
        String sid = mc.generateSID();
        assert mc.oqAppend(sid, "key1", 
                ByteBuffer.allocate(4).putInt(18).array(), false);
        mc.dCommit(sid, 0);
        
        sid = mc.generateSID();
        byte[] res = (byte[]) mc.oqRead(sid, "key1", 0, false);
        x = deserialize(res);
        x.remove((Integer)7);
        res = serializeList(x);
        mc.oqSwap(sid, "key1", 0, res, false);
        mc.dCommit(sid);
        
        res = (byte[])mc.get("key1");
        x = deserialize(res);
        for (int i: x ) System.out.println(i);
    }
    

    /**
     * Read only session.
     * @param mc1
     */
    public static void testSession1(MemcachedClient mc1) throws Exception {	
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("sid");

        String key1="key1", key2="key2";
        String sid = "sid";
        try {
            mc1.ciget(sid, key1);
            mc1.iqset(key1, "val1");
            mc1.ciget(sid, key2);
            mc1.iqset(key2, "val2");
            mc1.dCommit(sid);
            assert mc1.get(key1).equals("val1");
            assert mc1.get(key2).equals("val2");
        } catch (Exception e) {
            try {
                mc1.unleaseCO(sid);
            } catch (COException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        System.out.println("Test session 1 OK.");
    }

    /**
     * 
     * @param mc1
     */
    public static void testSession2(MemcachedClient mc1) throws Exception {
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("key2");
        mc1.delete("sid"); mc1.delete("sid2");

        String key1="key1", key2="key2";
        String sid = "sid", sid2 = "sid2";
        try {
            mc1.delete(key1);
            mc1.delete(key2);
        } catch (Exception e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }
        try {
            mc1.ciget(sid, key1);
            mc1.iqset(key1, "val1");
            mc1.ciget(sid, key2);
            mc1.unleaseCO(sid);
        } catch (Exception e) {
            try {
                mc1.unleaseCO(sid);
            } catch (COException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        assert mc1.get(key1).equals("val1");

        try {
            assert mc1.ciget(sid2, key2) == null;
            mc1.iqset(key2, "val2");
            assert mc1.ciget(sid2, key1).equals("val1");
            mc1.dCommit(sid2);
        } catch (Exception e) {
            try {
                mc1.unleaseCO(sid2);
            } catch (COException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        System.out.println("Test session 2 OK.");
    }

    public static void testSession3(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("sid"); mc1.delete("sid2"); mc1.delete("sid3");
        mc1.delete("key1"); mc1.delete("key2");

        mc1.ciget("sid", "key1");
        mc2.oqReg("sid2", "key1");
        try {
            mc1.ciget("sid", "key2");
        } catch (COException e) {}

        mc2.dCommit("sid2");
        mc1.ciget("sid", "key2");
        mc1.iqset("key2", "new_val2");
        mc1.dCommit("sid");
        assert mc1.ciget("sid3", "key2").equals("new_val2");
        assert mc1.dCommit("sid3");

        System.out.println("Test session 3 OK.");
    }

    public static void testSession4(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("sid"); mc1.delete("sid2"); mc1.delete("sid3");
        mc1.delete("key1"); mc1.delete("key2");

        mc1.ciget("sid", "key1");
        mc2.oqReg("sid2", "key1");
        mc1.iqset("key1", "val1");
        try {
            mc1.dCommit("sid");
            assert false;
        } catch (COException e) { }

        mc2.dCommit("sid2");
        assert mc1.ciget("sid", "key1") == null;
        mc1.ciget("sid", "key2");
        mc1.iqset("key2", "new_val2");
        mc1.iqset("key1", "val1");
        mc1.dCommit("sid");
        assert mc1.ciget("sid3", "key2").equals("new_val2");
        assert mc1.dCommit("sid3");

        System.out.println("Test session 4 OK.");
    }

    /**
     * Get ci leases multiple times same session
     * @param mc1
     * @param mc2
     * @throws Exception
     */
    public static void testSession5(
            MemcachedClient mc1) throws Exception {
        mc1.cleanup();
        mc1.delete("sid1");
        mc1.delete("key1"); mc1.delete("key2");

        for (int i = 0; i < 10; i++) {
            mc1.ciget("sid1", "key1");
        }
        mc1.iqset("key1", "val1");
        mc1.dCommit("sid1");

        System.out.println("Test session 5 OK.");
    }

    /**
     * Get oq leases multiple times same session
     * @param mc1
     * @param mc2
     * @throws Exception
     */
    public static void testSession6(
            MemcachedClient mc1) throws Exception {
        mc1.cleanup();
        mc1.delete("sid1"); mc1.delete("tid1");
        mc1.delete("key1");

        for (int i = 0; i < 10; i++) {
            mc1.oqReg("sid1", "key1");
        }
        mc1.dCommit("sid1");

        System.out.println("Test session 6 OK.");
    }

    /**
     * Get oq leases multiple times same session
     * @param mc1
     * @param mc2
     * @throws Exception
     */
    public static void testSession7(
            MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("sid1"); mc1.delete("sid2");
        mc1.delete("key1"); mc1.delete("key2");

        mc1.ciget("sid1", "key1");
        mc1.ciget("sid1", "key2");
        mc1.unleaseCO("sid1");
        mc2.ciget("sid2", "key1");
        mc2.ciget("sid2", "key2");
        assert mc2.iqset("key1", "val1");
        assert mc2.iqset("key2", "val2");
        mc2.dCommit("sid2");
        assert mc1.get("key1").equals("val1");
        assert mc1.get("key2").equals("val2");

        System.out.println("Test session 7 OK.");
    }

    public static void testSession8(MemcachedClient mc1, MemcachedClient mc2, 
            MemcachedClient mc3, MemcachedClient mc4) throws Exception {
        mc1.cleanup(); mc2.cleanup(); mc3.cleanup(); mc4.cleanup();
        mc1.delete("sid1"); mc1.delete("sid2"); mc1.delete("sid3"); mc1.delete("sid4"); mc1.delete("sid5");
        mc1.delete("key1"); mc1.delete("key2");

        mc1.oqReg("sid1", "key1");
        mc2.oqReg("sid2", "key1");
        mc3.oqReg("sid3", "key1");
        mc1.oqReg("sid1", "key2");
        mc2.oqReg("sid2", "key2");
        mc3.oqReg("sid3", "key2");
        mc4.oqReg("sid4", "key1");
        mc4.oqReg("sid4", "key2");
        assert mc3.dCommit("sid3");
        assert mc1.dCommit("sid1");
        assert mc4.dCommit("sid4");
        assert mc2.dCommit("sid2");
        assert (mc1.ciget("sid5", "key1") == null);
        assert (mc1.ciget("sid5", "key2") == null);
        assert mc1.iqset("key1", "val1");
        assert mc1.iqset("key2", "val2");
        assert mc1.dCommit("sid5");
        assert mc1.get("key1").equals("val1");
        assert mc1.get("key2").equals("val2");

        System.out.println("Test session 8 OK.");
    }

    public static void testSession9(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("sid1"); mc1.delete("sid2"); mc1.delete("sid3");
        mc1.delete("key1"); mc1.delete("key2");

        // session 1 is pending
        mc1.ciget("sid1", "key1");
        mc1.oqReg("sid1", "key2");

        // session 2 comes along should abort session 1
        mc2.oqReg("sid3", "key1");

        assert !mc1.iqset("key1", "val1");
        try {
            mc1.dCommit("sid1");
            assert false;
        } catch (COException e) {

        }

        assert mc1.ciget("sid2", "key2") == null;
        assert mc1.iqset("key2", "val2");
        mc1.dCommit("sid2");
        assert mc1.get("key2").equals("val2");
        assert mc2.dCommit("sid3");

        System.out.println("Test session 9 OK.");
    }

    public static void testSession10(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup();	mc1.enableBackoff();
        mc2.cleanup();	mc2.enableBackoff();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("key3"); mc1.delete("key4");
        mc1.delete("sid1"); mc1.delete("sid2"); mc1.delete("sid3"); mc1.delete("sid4");
        mc1.delete("tid1"); mc1.delete("tid2"); mc1.delete("tid3"); mc1.delete("tid4");

        assert mc1.set("key1", "val1");
        assert mc1.ciget("sid1", "key1").equals("val1");
        assert mc1.ciget("sid1", "key2") == null;
        assert mc1.oqReg("sid1", "key3");
        assert mc2.oqReg("sid2", "key2");
        try {
            mc1.oqReg("sid1", "key4");
            assert false;
        } catch (Exception e) {

        }
        assert mc2.dCommit("sid2");

        System.out.println("Test session 10 OK.");
    }

    public static void testSession11(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        mc1.cleanup();	mc1.enableBackoff();
        mc2.cleanup();	mc2.enableBackoff();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("key3"); mc1.delete("key4");
        mc1.delete("sid1"); mc1.delete("sid2"); mc1.delete("sid3"); mc1.delete("sid4");
        mc1.delete("tid1"); mc1.delete("tid2"); mc1.delete("tid3"); mc1.delete("tid4");

        assert mc1.set("key1", "val1");
        assert mc1.ciget("sid1", "key1").equals("val1");
        assert mc1.ciget("sid1", "key2") == null;
        assert mc1.oqReg("sid1", "key3");
        assert mc2.oqReg("sid2", "key2");
        try {
            mc1.ciget("sid1", "key4");
            assert false;
        } catch (Exception e) {

        }
        assert mc2.dCommit("sid2");

        System.out.println("Test session 11 OK.");
    }

    public static void testSession12(MemcachedClient mc1, 
            MemcachedClient mc2) throws Exception {
        System.out.print("Test session 12 validate()... ");
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("key1"); mc2.delete("key2");
        mc1.delete("sid1"); mc2.delete("sid2");
        mc1.set("key1", "val1");
        mc1.ciget("sid1", "key1").equals("val1");
        mc1.oqReg("sid1", "key2");
        mc2.oqReg("sid2", "key1");
        try {
            mc1.validate("sid1");
            assert false;
        } catch (COException e) { }
        mc2.validate("sid2");
        mc2.dCommit("sid2");
        System.out.println("OK");
    }

    public static void testSession13(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 13 validate()... ");
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("key3");
        mc1.delete("sid1"); 
        mc1.set("key1", "val1");
        mc1.ciget("sid1", "key1").equals("val1");
        assert mc1.ciget("sid1", "key3") == null;
        assert mc1.iqset("key3", "val3");
        mc1.oqReg("sid1", "key2");
        mc1.validate("sid1");
        mc1.dCommit("sid3");
        assert mc1.iqget("key3").equals("val3");
        System.out.println("OK");
    }

    public static void testSession14(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 14 CO refill... ");
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("key3");
        mc1.delete("sid1"); 
        mc1.set("key1", "val1");
        assert mc1.oqRead("sid1", "key1").equals("val1");
        assert mc1.oqWrite("sid1", "key1", "val1new");
        assert mc1.oqRead("sid1", "key2") == null;
        assert mc1.oqWrite("sid1", "key2", null);
        assert mc1.ciget("sid1", "key3") == null;
        assert mc1.iqset("key3", "val3");
        assert mc1.validate("sid1");
        assert mc1.dCommit("sid1");
        System.out.println("OK");
    }

    public static void testSession15(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 15 CO rmw normal... ");
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("key2"); mc1.delete("key3");
        mc1.delete("sid1"); 
        mc1.set("key1", "val1");		
        assert mc1.oqRead("sid1", "key1").equals("val1");
        mc1.oqWrite("sid1", "key1", "val2");
        mc1.dCommit("sid1");		
        assert mc1.get("key1").equals("val2");
        System.out.println("OK");
    }

    public static void testSession16(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 16 CO rmv read write null value ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("key2"); 
        mc1.delete("key3"); mc1.delete("sid1");
        assert mc1.oqRead("sid1", "key1") == null;
        mc1.oqWrite("sid1", "key1", null);
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        System.out.println("OK");
    }

    public static void testSession17(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 17 CO rmv oqwrite normal ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("key2"); 
        mc1.delete("key3"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession18(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 18 CO rmv oqwrite null value ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("key2"); 
        mc1.delete("key3"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", null);
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession19(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 19 CO rmv oqwrite multiple keys ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("key2"); 
        mc1.delete("key3"); mc1.delete("sid1");
        mc1.delete("key3"); mc1.delete("key4");
        mc1.set("key3", "val3"); mc1.set("key4", "val4");
        mc1.oqWrite("sid1", "key1", null);
        mc1.oqWrite("sid1", "key2", "val2");
        mc1.oqWrite("sid1", "key3", null);
        mc1.oqWrite("sid1", "key4", "newVal4");
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1") == null;
        assert mc1.get("key2").equals("val2");
        assert mc1.get("key3") == null;
        assert mc1.get("key4").equals("newVal4");
        System.out.println("OK");
    }

    public static void testSession20(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 20 CO rmv oqwrite read same session ...");
        mc1.cleanup();
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");
        mc1.ciget("sid1", "key1").equals("val1");	// observe the update from same session
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession21(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        System.out.print("Test session 21 CO rmv oqwrite read diff session ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");

        try {
            mc2.ciget("sid2", "key1");
            assert false;
        } catch (Exception e) {}

        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession22(MemcachedClient mc1, MemcachedClient mc2) throws Exception {
        System.out.print("Test session 22 CO rmv oqwrite write-write session ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1"); mc1.delete("sid2");
        mc1.oqWrite("sid1", "key1", "val1");

        try {
            mc2.oqWrite("sid2", "key1", "val2");
            assert false;
        } catch (Exception e) {}

        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession23(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 23 CO w oqwrite abort session ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");
        mc1.validate("sid1");
        mc1.unleaseCO("sid1");
        assert mc1.get("key1") == null;

        mc1.set("key1", "val1");
        mc1.oqWrite("sid1", "key1", "val2");
        mc1.validate("sid1");
        mc1.unleaseCO("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession24(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 24 CO iqincr ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.set("key1", "3");
        assert mc1.oqincr("sid1", "key1", 7) == 10;
        assert mc1.get("key1").equals("3");
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("10");
        System.out.println("OK");
    }

    public static void testSession25(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 25 CO iqincr ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.set("key1", "3");
        assert mc1.oqincr("sid1", "key1", 7) == 10;
        assert mc1.oqdecr("sid1", "key1", 2) == 8;
        assert mc1.get("key1").equals("3");
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("8");
        System.out.println("OK");
    }

    public static void testSession26(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 26 CO iqincr ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        assert mc1.oqincr("sid1", "key1", 7) == null;
        assert mc1.oqdecr("sid1", "key1", 2) == null;
        assert mc1.get("key1") == null;
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1") == null;
        System.out.println("OK");
    }	

    public static void testSession27(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 27 CO oqwrite iqincr ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        assert mc1.oqincr("sid1", "key1", 7) == null;
        assert mc1.oqdecr("sid1", "key1", 2) == null;
        assert mc1.get("key1") == null;
        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession28(MemcachedClient mc1, MemcachedClient mc2) 
            throws Exception {
        System.out.print("Test session 28 CO oqwrite iqincr ...");
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.set("key1", "val1");
        assert mc1.ciget("sid1", "key1").equals("val1");

        // session 2 voids C lease granted by session 1
        assert mc2.oqWrite("sid2", "key1", "val2");

        // session 1 should fail validate
        try {
            mc1.validate("sid1");
            assert false;
        } catch (Exception e) {}
        mc2.dCommit("sid2");
        assert mc1.get("key1").equals("val2");
        System.out.println("OK");
    }

    public static void testSession29(MemcachedClient mc1, MemcachedClient mc2) 
            throws Exception {
        System.out.print("Test session 29 CO oqwrite multi-sessions...");
        mc1.cleanup(); mc2.cleanup();
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.delete("key2"); mc1.delete("key3");

        assert mc1.oqWrite("sid1", "key1", "5");
        assert mc1.oqincr("sid1", "key1") == 6;
        assert mc1.oqdecr("sid1", "key1", 6) == 0;
        assert mc1.oqdecr("sid1", "key1") == 0;
        assert mc1.get("key1") == null;

        // session 2 is aborted
        try {
            mc2.oqWrite("sid2", "key1", "val2");
            assert false;
        } catch (COException e) { }

        assert mc1.oqWrite("sid1", "key1", "newVal");

        mc1.validate("sid1");
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("newVal");
        System.out.println("OK");
    }

    public static void testSession30(
            MemcachedClient mc1, MemcachedClient mc2, MemcachedClient mc3) 
                    throws Exception {
        System.out.print("Test session 30 CO oqwrite multi-sessions...");
        mc1.cleanup(); mc2.cleanup(); mc3.cleanup();
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.delete("key2"); mc1.delete("key3");
        mc1.set("key2", "val2");

        assert mc1.ciget("sid1", "key1") == null;
        assert mc1.iqset("key1", "val1");
        assert mc1.get("key1").equals("val1");
        assert mc2.ciget("sid2", "key1").equals("val1");
        assert mc2.ciget("sid2", "key2").equals("val2");
        assert mc2.ciget("sid2", "key3") == null;

        assert mc3.oqWrite("sid3", "key1", "5");

        try {
            mc2.ciget("sid2", "key1");
            assert false;
        } catch (COException e) {}

        try {
            mc1.ciget("sid1", "key1");
            assert false;
        } catch (COException e) {}

        mc3.validate("sid3");
        mc3.dCommit("sid3");
        //		System.out.println(mc1.get("key1"));
        assert mc1.get("key1").equals("5");
        assert mc1.get("key2").equals("val2");
        assert mc1.ciget("sid1", "key3") == null;
        assert mc1.iqset("key3", "val3");
        assert mc1.ciget("sid1", "key3").equals("val3");
        assert mc1.validate("sid1");
        assert mc1.dCommit("sid1");
        System.out.println("OK");
    }

    public static void testSession31(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 31 CO oqwrite abort...");
        mc.cleanup();
        mc.delete("key1"); mc.delete("key2");
        mc.delete("key3"); mc.delete("sid");
        mc.oqWrite("sid", "key1", "val1");
        mc.unleaseCO("sid");
        assert mc.get("key1") == null;

        assert mc.ciget("sid", "key1") == null;
        assert mc.iqset("key1", "val1");
        assert mc.oqRead("sid", "key2") == null;
        assert mc.ciget("sid", "key3") == null;
        assert mc.unleaseCO("sid");
        assert mc.get("key1").equals("val1");
        assert mc.get("key2") == null;
        assert mc.get("key3") == null;
        System.out.println("OK");
    }

    public static void testSession32(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 32 CO abort after validate...");
        mc.cleanup();
        mc.delete("key1"); mc.delete("key2");
        mc.delete("key3"); mc.delete("sid");
        mc.oqWrite("sid", "key1", "val1");
        assert mc.validate("sid");
        mc.unleaseCO("sid");
        assert mc.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession33(MemcachedClient mc, MemcachedClient mc2) 
            throws Exception {
        System.out.print("Test session 33 CO oqread only...");
        mc.cleanup(); mc2.cleanup();
        mc.delete("key1"); mc.delete("key2");
        mc.delete("key3"); mc.delete("sid"); mc.delete("sid2");
        mc.delete("sid3"); mc.delete("sid4");
        mc.set("key1", "val1");

        assert mc.oqRead("sid", "key1").equals("val1");
        assert mc.oqRead("sid", "key2") == null;
        assert mc.validate("sid");
        assert mc.dCommit("sid");
        assert mc2.oqRead("sid2", "key2") == null;
        assert mc2.unleaseCO("sid2");
        assert mc.oqWrite("sid", "key2", "newVal");
        assert mc.dCommit("sid");
        assert mc.get("key2").equals("newVal");

        System.out.println("OK");
    }

    public static void testSession34(MemcachedClient mc, MemcachedClient mc2) 
            throws Exception {
        System.out.print("Test session 34 CO complex test...");
        mc.cleanup(); mc2.cleanup();
        mc.delete("key1"); mc.delete("key2");
        mc.delete("key3"); mc.delete("sid"); mc.delete("sid2");
        mc.delete("sid3"); mc.delete("sid4");

        // sess 1 aborts
        assert mc.ciget("sid", "key1") == null;
        assert mc.unleaseCO("sid");

        // sess 2 should complete normally
        assert mc2.ciget("sid2", "key1") == null;
        assert mc2.iqset("key1", "val1");
        assert mc2.oqWrite("sid2", "key2", "val2");
        assert mc2.validate("sid2");
        assert mc2.dCommit("sid2");
        assert mc2.get("key1").equals("val1");
        assert mc2.get("key2").equals("val2");

        //		assert mc.oqWrite("sid, key, value)


        System.out.println("OK");
    }

    public static void testSession35(MemcachedClient mc, MemcachedClient mc2) 
            throws Exception {
        System.out.print("Test session 35 CO multi-ciget...");
        mc.cleanup(); mc2.cleanup();
        mc.delete("key1"); mc.delete("key2");
        mc.delete("key3"); mc.delete("sid1"); mc.delete("sid2");
        mc.delete("sid3"); mc.delete("sid4");

        assert mc.ciget("sid1", "key1") == null;
        assert mc.ciget("sid1", "key2") == null;
        assert mc.iqset("key1", "val1");
        assert mc.iqset("key2", "val2");
        mc.dCommit("sid1");
        assert mc.get("key1").equals("val1");
        assert mc.get("key2").equals("val2");

        System.out.println("OK");
    }

    public static void testSession36(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 36 CO oqappend...");
        mc.cleanup(); mc.delete("key1"); mc.delete("sid1");		
        assert mc.set("key1", "val1");
        assert mc.oqAppend("sid1", "key1", "val2", true);
        assert mc.get("key1").equals("val1");
//        assert mc.oqprepend("sid1", "key1", "val3");
        assert mc.get("key1").equals("val1");
        assert mc.validate("sid1");
        assert mc.dCommit("sid1");
        System.out.println(mc.get("key1"));
        assert mc.get("key1").equals("val1val2");
        System.out.println("OK");
    }

    public static void testSession37(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 37 CO oqappend null value...");
        mc.cleanup(); mc.delete("key1"); mc.delete("sid1");
        assert (!mc.oqAppend("sid1", "key1", "val2", true));
        assert mc.get("key1") == null;
//        assert (!mc.oqprepend("sid1", "key1", "val3"));
        assert mc.get("key1") == null;
//        assert mc.validate("sid1", null);
//        assert mc.dCommit("sid1");
        assert mc.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession38(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 38 CO write session...");
        mc.cleanup(); mc.delete("key1"); mc.delete("key2"); mc.delete("sid1");
//        assert mc.oqWrite("sid1", "key1", "val1");
        assert mc.oqAppend("sid1", "key1", "val2", true) == false;
        assert mc.get("key1") == null;
//        assert mc.oqprepend("sid1", "key1", "val3");
        assert mc.get("key1") == null;
//        assert mc.validate("sid1", null);
//        assert mc.dCommit("sid1");
//        assert mc.get("key1").equals("val1val2");
        assert mc.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession39(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 39 CO read-write same key same session...");
        mc.cleanup(); mc.delete("key1"); mc.delete("key2"); mc.delete("sid1");
        assert mc.ciget("sid1", "key1") == null;
        assert mc.oqWrite("sid1", "key1", "val1");
        assert mc.iqset("key1", "val1") == false;
        assert mc.get("key1") == null;
        assert mc.validate("sid1");
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val1");

        mc.delete("key1");
        assert mc.ciget("sid1", "key1") == null;
        assert mc.iqset("key1", "val1");
        assert mc.get("key1").equals("val1");
        assert mc.oqWrite("sid1", "key1", "val2");
        assert mc.get("key1").equals("val1");
        assert mc.validate("sid1");
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val2");

        System.out.println("OK");
    }

    public static void testSession40(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 40 append incr non-existing key.");
        mc.cleanup(); mc.delete("key1"); mc.delete("key2"); mc.delete("sid1");
        assert (!mc.oqAppend("sid1", "key1", "val1", true));
//        assert mc.validate("sid1", 0);
//        assert mc.dCommit("sid1", 0);
        assert mc.get("key1") == null;
        assert mc.oqAdd("sid2", "key1", "val2", 0, true);
        assert mc.validate("sid2", 0);
        assert mc.dCommit("sid2", 0);
        assert mc.get("key1", 0, true).equals("val2");
        System.out.println("OK");
    }

    public static void cleanup(MemcachedClient mc1) {
        mc1.cleanup();
    }
    
    public static void testSession41(MemcachedClient mc) throws Exception {
        System.out.println("Test Memory leak.");
        mc.cleanup(); mc.delete("sid1"); mc.delete("key1"); mc.delete("key2");
        
        int i = 0;
        while (true) {
//            mc.delete("key1"); 
//            mc.delete("key2");
            
            i++;
            Object obj = mc.ciget("sid"+i, "key1");
            if (obj == null) 
                mc.iqset("key1", "val1");
            obj = mc.ciget("sid"+i, "key2");
            if (obj == null)
                mc.iqset("key2", "val2");
            
            mc.validate("sid"+i);
            mc.dCommit("sid"+i);
//            if (i == 1) break;
        }
    }
    
    public static void testSession42(MemcachedClient mc) throws Exception {
        mc.set("key1", "val1");
        mc.oqRead("sid1", "key1");
        mc.oqSwap("sid1", "key1", "val2");
        mc.oqRead("sid1", "key1");
        mc.oqSwap("sid1", "key1", "val3");
        mc.validate("sid1");
        mc.dCommit("sid1");
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");
    }
    
    public static void testSession43(MemcachedClient mc) throws Exception {
        System.out.print("Test oqswap null then oqappend same session...");
        mc.set("key1", "val1");        
        assert mc.oqRead("sid1", "key1").equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, null, true);
//        assert (!mc.oqAppend("sid1", "key1", "val2", true));
//        assert (mc.get("key1").equals("val1"));
        assert (mc.oqRead("sid1", "key1", 0, true) == null);
        assert mc.validate("sid1");
        assert mc.dCommit("sid1");
        
        assert mc.get("key1") == null;
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");
        System.out.println("OK.");
    }
    
    public static void testSession44(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        System.out.print("Test multi-ciget...");
        //mc.set("key1", "val1");        
        assert mc.ciget("sid1", "key1") == null;
        
        mc2.disableBackoff();
        try {
            mc2.ciget("sid2", "key1");            
            assert false;
        } catch (COException e) {}
        
        assert mc.iqset("key1", "val1");
        assert mc2.ciget("sid2", "key1").equals("val1");
        assert mc.ciget("sid1", "key1").equals("val1");
        assert mc.validate("sid1", 0) == true;
        assert mc.dCommit("sid1");
        assert mc2.validate("sid2", 0) == true;
        assert mc2.dCommit("sid2");
                
        assert mc.get("key1").equals("val1");
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");
        mc.delete("sid2");
        System.out.println("OK.");
    }
    
    public static void testSession45(MemcachedClient mc) throws Exception {
        System.out.print("Test ciget iqset then abort...");
        //mc.set("key1", "val1");        
        assert mc.ciget("sid1", "key1") == null;                
        assert mc.iqset("key1", "val1");
        assert mc.dAbort("sid1", 0) == true;
        assert mc.get("key1").equals("val1");
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testSession46(MemcachedClient mc) throws Exception {
        System.out.print("Test ciget iqset then abort...");
        assert mc.set("key1", "val1");
        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, null, true);        
        assert mc.ciget("sid1", "key1") == null;                
        //assert mc.iqset("key1", "val1");
        assert mc.dAbort("sid1", 0) == true;
        //assert mc.get("key1").equals("val1");
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testSession47(MemcachedClient mc) throws Exception {
        System.out.print("Test oqswap null then oqappend...");
        assert mc.set("key1", "val1");
        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, null, true);
        assert !mc.oqAppend("sid1", "key1", "val1", true);
        assert mc.oqAdd("sid1", "key1", "val2", 0, true);
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true).equals("val2");
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testSession48(MemcachedClient mc) throws Exception {
        System.out.print("Test oqswap null then oqappend...");
        assert mc.set("key1", "val1");
        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, null, true);
        assert mc.oqRead("sid1", "key1", 0, true) == null;
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true) == null;
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testSession49(MemcachedClient mc) throws Exception {
        System.out.print("Test oqswap null then oqappend...");
        assert mc.set("key1", "val1");
        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, null, true);
        assert mc.oqWrite("sid1", "key2", 0, "val3", true);
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true) == null;
        assert mc.get("key2", 0, true).equals("val3");
        
        // clean up
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testSession50(MemcachedClient mc) throws Exception {
        System.out.print("Test oqWrite...");
        assert mc.set("key1", "val1");
        assert mc.oqWrite("sid1", "key1", 0, null, true);
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true) == null;
        
        // clean up
        mc.delete("key1");
        mc.delete("sid1");        
        System.out.println("OK.");
    }
    
    public static void testCOMultiget1(MemcachedClient mc) throws Exception {
        System.out.print("Test co_getMulti read...");
        assert mc.set("key1", "val1");
        assert mc.set("key2", "val2");
        String[] keys = new String[3];
        keys[0] = "key1"; keys[1] = "key2"; keys[2] = "key3";
        Map<String, Object> map = mc.co_getMulti("sid1", 0, 0, false, keys);
        assert (map.get("key1").equals("val1"));
        assert (map.get("key2").equals("val2"));
        assert map.get("key3") == null;
        assert mc.iqset("key3", "val3");
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key3").equals("val3");
        System.out.println("OK");
        
        mc.delete("sid1");
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3");        
    }
    
    public static void testCOMultiget2(MemcachedClient mc) throws Exception {
        System.out.print("Test co_getMulti write...");
        assert mc.set("key1", "val1", 0, false);
        assert mc.set("key2", "val2", 0, false);
        String[] keys = new String[3];
        
        keys[0] = "key1"; keys[1] = "key2"; keys[2] = "key3";
        Map<String, Object> map = mc.co_getMulti("sid1", 0, 1, false, keys);
        
        assert (map.get("key1").equals("val1"));
        assert (map.get("key2").equals("val2"));
        assert map.get("key3") == null;
        assert mc.oqSwap("sid1", "key3", 0, "val3", false);
        assert map.get("key3") == null;
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key3").equals("val3");
        System.out.println("OK");
        
        mc.delete("sid1");
        mc.delete("key1");
        mc.delete("key2");
        mc.delete("key3"); 
    }    
    
    public static void testCOMultiget3(MemcachedClient mc) throws Exception {
        System.out.print("Test co_getMulti write...");
        
        String[] keys = new String[1];        
        keys[0] = "key1";
        Map<String, Object> map = mc.co_getMulti("sid1", 0, 1, false, keys);
        for (int i = 0; i < 100; ++i) 
            map = mc.co_getMulti("sid1", 0, 1, false, keys);
        
        
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
            
        mc.delete("sid1");
        mc.delete("key1");         
    }    
    
    public static void testLeasesRepeat(MemcachedClient mc) throws Exception {
        System.out.print("Test lease repeated...");
        int K = 1;
     
        for (int i = 0; i < K; ++i) {
            mc.delete("key"+i);
        }
        
        for (int l = 0; l < 2; ++l) {
            for (int k = 0; k < 2; ++k) {
                for (int i = 0; i < K; ++i) {
                    assert mc.ciget("sid1", "key"+i, false, 0) == null;
                }
            }
            assert mc.validate("sid1", 0);
            mc.dAbort("sid1", 0);
        }
        
        
        for (int i = 0; i < K; ++i) {
            mc.delete("key"+i);
        }
        mc.delete("sid1");
        System.out.println("OK");
    }
    
    public static void testMultiCOAppends1(MemcachedClient mc) throws Exception {
        String[] keys = new String[] {"key1", "key2", "key3"};
        mc.set("key1", "val");
        mc.set("key2", "val");
        
        String[] values = new String[] {"app1", "app2", "app3"};
        
        Map<String, Boolean> res = mc.oqAppends("sid1", keys, values, 0, true);
        assert (res != null && res.size() == 3);
        assert (res.get("key1") == true);
        assert (res.get("key2") == true);
        assert (res.get("key3") == false);
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true).equals("valapp1") : mc.get("key1", 0, true);
        assert mc.get("key2", 0, true).equals("valapp2") : mc.get("key2", 0, true);
        assert mc.get("key3", 0, true) == null : mc.get("key3", 0, true);
        
        for (String key: keys)
            mc.delete(key);
        mc.delete("sid1");
    }
    
    public static void testMultiCOAppends3(MemcachedClient mc) throws Exception {
        System.out.print("Test Multi-Append large keys");
        int K = 1000;
        
        String[] keys = new String[K];
        for (int i = 0; i < K; ++i) {
            keys[i] = "this,is,very,long_key"+i;
            if (i % 2 == 0) {
                mc.set(keys[i], "val");
            }
        }
        
        String[] values = new String[K];
        for (int i = 0; i < K; ++i) {
            values[i] = "append_long,_value"+i;
        }
        
        for (int loop = 0; loop < 1000; ++loop) {
            Map<String, Boolean> res = mc.oqAppends("sid"+loop, keys, values, 0, true);
            assert mc.validate("sid"+loop, 0);
            assert mc.dCommit("sid"+loop, 0);
        }
        
        for (String key: keys)
            mc.delete(key);
        mc.delete("sid1");
        System.out.println("... OK");

    }
    
    public static void testMultiCOAppends2(MemcachedClient mc, MemcachedClient mc2) throws COException, IOException {
        String[] keys = new String[] {"key1", "key2", "key3"};
        mc.set("key1", "val");
        mc.set("key2", "val");
        
        String[] values = new String[] {"app1", "app2", "app3"};
        
        assert mc2.oqRead("sid2", "key2", 0, true).equals("val");
        
        try {
            Map<String, Boolean> res = mc.oqAppends("sid1", keys, values, 0, true);
            assert false;
        } catch (COException e) {
            try {
                mc.dAbort("sid1", 0);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        
        assert mc2.oqSwap("sid2", "key2", 0, "val2", true);
        assert mc2.validate("sid2", 0);
        assert mc2.dCommit("sid2", 0);
        
        assert mc.get("key1", 0, true).equals("val") : mc.get("key1", 0, true);
        assert mc.get("key2", 0, true).equals("val2") : mc.get("key2", 0, true);
        assert mc.get("key3", 0, true) == null : mc.get("key3", 0, true);
    }
    
    
    public static void testMultiOQSwapsNull(MemcachedClient mc) throws COException, IOException {
        mc.set("key1", "val1");
        mc.set("key3", "val3");
        
        String[] keys = new String[] {"key1", "key2", "key3" };
        Object[] values = new Object[] { "val2", null, null };
        
        mc.co_getMulti("sid1", 0, 1, true, keys);
        mc.oqSwaps("sid1", keys, values, 0, true);
        mc.validate("sid1", 0);
        mc.dCommit("sid1");
        
        assert mc.get("key1", 0, true).equals("val2");
        assert mc.get("key2", 0, true) == null;
        assert mc.get("key3", 0, true) == null;
    }
    
    public static void testOqRead(MemcachedClient mc) throws COException, IOException {
        mc.set("key1", "val1");

        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.get("key1", 0, true).equals("val1");
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true).equals("val1");
    }
    
    public static void testOqWrite(MemcachedClient mc) throws Exception {
        System.out.print("Test OqWrite...");
        mc.set("key1", "val1");

        assert mc.oqWrite("sid1", "key1", 0, "val2", true);
        assert mc.get("key1", 0, true).equals("val1");
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1", 0, true).equals("val2");
        
        assert mc.oqWrite("sid2", "key1", 0, "val3", true);
        assert mc.get("key1", 0, true).equals("val2");
        mc.dAbort("sid2", 0);
        assert mc.get("key1", 0, true).equals("val2");
        
        System.out.println("OK");
    }
    
    public static void testUpgradeLease(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        System.out.print("Test Upgrade lease...");
        mc.set("key1", "val1");        

        assert mc.ciget("sid1", "key1", true, 0).equals("val1");
        assert mc2.ciget("sid2", "key1", true, 0).equals("val1");
        assert mc.oqRead("sid1", "key1", 0, true).equals("val1");
        assert mc.oqSwap("sid1", "key1", 0, "val2", true);
        try {
            mc2.oqRead("sid2", "key1", 0, true);
            assert false;
        } catch (Exception e) {}
        mc.validate("sid1", 0);
        mc.dCommit("sid1", 0);
        
        assert mc.get("key1", 0, true).equals("val2");
        
        System.out.println("OK");
    }
    
    public static void testOQAppendContinuously(MemcachedClient mc) throws Exception {
        mc.delete("key1");
        
        byte[] bytes = new byte[1024*1024-200];
        assert mc.oqAdd("sid1", "key1", bytes, 0, false);
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        
        bytes = new byte[1];
        
        for (int i = 0; i < 100; ++i) {
            boolean success = mc.oqAppend("sid1", "key1", 0, bytes, false);
            if (!success) {
                assert mc.oqAdd("sid1", "key1", bytes, 0, false) == false;
                System.out.println("here");
            }
            mc.validate("sid1", 0);
            mc.dCommit("sid1", 0);
        }
    }
    
    public static void testOQAdd(MemcachedClient mc) throws Exception {
        String sid = mc.generateSID();
        assert mc.ewAdd(sid, "key1", "val1".getBytes(), 0, false, true);
        assert mc.ewAdd(sid, "key2", "val2".getBytes(), 0, false, false);
        assert mc.dCommit(sid);
        
        Object obj = mc.get("key1");
        assert new String((byte[])obj).equals("val1");
        obj = mc.get("key2");
        assert new String((byte[])obj).equals("val2");
    }
    
    /**
     * This runs through some simple tests of the MemcacheClient.
     *
     * Command line args:
     * args[0] = number of threads to spawn
     * args[1] = number of runs per thread
     * args[2] = size of object to store
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if ( !UnitTests.class.desiredAssertionStatus() ) {
            System.err.println( "WARNING: assertions are disabled!" );
            try { Thread.sleep( 3000 ); } catch ( InterruptedException e ) {}
        }

        // specify the list of cache server
        // currently test on a single cache server only
        String[] serverlist = {	"localhost:11211" };

        Integer[] weights = {1}; 

        if ( args.length > 0 )
            serverlist = args;

        // initialize the pool for memcache servers
        SockIOPool pool = SockIOPool.getInstance( "test" );
        pool.setServers( serverlist );
        pool.setWeights( weights );
        pool.setMaxConn( 250 );
        pool.setNagle( false );
        pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
        pool.initialize();

        Logger.getRootLogger().setLevel(Level.INFO);

        mc = new MemcachedClient( "test" );
        MemcachedClient mc2 = new MemcachedClient("test");
        MemcachedClient mc3 = new MemcachedClient("test");
        MemcachedClient mc4 = new MemcachedClient("test");

        ConsoleAppender console = new ConsoleAppender(); // create appender
        // configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.OFF);
        console.activateOptions();
        // add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(console);

        try {			
            // old tests for original twemcache functionalities 
//            testRangeBW(mc);
//            testAppend(mc4);
            testOQSwap(mc);
//            			runAlTests( mc, mc2 );
            //			test99(mc);
            //			test100(mc);			

            //			testDeleteRace(mc, mc2);
            //			testReadLease(mc, mc2);
            //
            //			testIQGetSet(mc);
            //			testRMW(mc);
            //			testReadOnWriteLease(mc, mc2);
            //			testWriteOnWriteLease(mc, mc2);
            //			testWriteAfterWriteLease(mc, mc2);
            //			testReleaseTokenWithoutXLease(mc, mc2);
            //			testFailedXLease(mc, mc2);
            //			testGetNoLease(mc, mc2);
            //			testReleaseX(mc, mc2);
            //			sxqQaReg(mc, mc2);
            //			sxqQaReg2(mc, mc2);
            //			sxqQaReg3(mc, mc2);
            //			sxq4(mc, mc2);
            //			sxq6(mc, mc2);
            //			sxq7(mc);
            //			sxq8(mc, mc2);
            //			sxq9(mc);
            //			
            //			test_stale_data_1(mc, mc2);
            //
            //			test_incr(mc, mc2);
            //			test_decr(mc, mc2);
            //			test_delete(mc, mc2);
            //			test_append(mc, mc2);
            //			test_prepend(mc, mc2);
            //			test_add(mc, mc2);
            //			test_replace(mc, mc2);
            //			test_get(mc, mc2);
            //			test_set(mc, mc2);		
            ////			test_unlease(mc, mc2);
            //
            //			testQARead_RMWemptyCacheIlease(mc);
            //			testQARead_RMWemptyCache(mc);
            //			testQARead_RMWfailedQuarantine(mc, mc2);
            //			
            //			test_QaRead(mc, mc2);
            //			test_getlease_same_thread(mc, mc2);
            //			
            //			test_getlease_different_thread(mc, mc2);			
            //
            //			System.out.println("Following tests requires expired_lease_time=" + EXP_LEASE_TIME + " ms to execute");
            //			test_SaR_HasVal(mc, mc2);
            //			test_SaR_NoVal(mc, mc2);
            //			test_getlease_same_thread_expired(mc);
            //			sxq5(mc, mc2);
            //			testRMWLeaseTimedOut(mc);
            //			
            //			test_delete_leases(mc,  mc2);
            //			
            //			test_qareg_session(mc);		
            //			
            //			test_iqget();
            //			test_iqset();
            //			test_iqappend();
            //			test_iqincr(mc, mc2);
            //			
            ////			testCOLeases1(mc, mc2);
            ////			testCOLeases2(mc, mc2);
            ////			testCOLeases3(mc, mc2);
            ////			testCOLeases4(mc, mc2);
            ////			testCOLeases5(mc, mc2);
            ////			testCOLeases6(mc, mc2, mc3);
            //			
            ////			testCILeases1(mc, mc2);
            ////			testCILeases2(mc, mc2);
            ////			testCILeases3(mc, mc2);
            ////			testCILeases4(mc, mc2, mc3);
            //			
            //			testCO1(mc,mc2);
            //			testCO2(mc,mc2);
            //			testCO3(mc,mc2);
            ////			testCO4(mc,mc2, mc3);
            ////			testCOSameSess1(mc, mc2);
            ////			testCOSameSess2(mc, mc2);
            //			testCOSameSess3(mc, mc2);
            //			testCOUnlease(mc);
            //			testCOUnlease2(mc);
            //			
            ////			testPendingTrans(mc);
            ////			testPendingTrans2(mc);
            ////			testPendingTrans3(mc, mc2);
            ////			testBlockWrite(mc, mc2, mc3);
            ////			testBlockWrite2(mc, mc2, mc3);
            ////			testBlockWrite3(mc, mc2, mc3);
            ////			testBlockWrite4(mc, mc2, mc3, mc4);
            ////			test_qaread(mc, mc2);
            ////			test_getprik(mc,mc2);
            
//            testAppendAddFail(mc);
//            testOQAdd(mc);
//            testSession1(mc);
//            testSession2(mc);
//            testSession3(mc, mc2);
//            testSession4(mc, mc2);
//            testSession5(mc);
//            testSession6(mc);
//            testSession7(mc, mc2);
//            testSession8(mc, mc2, mc3, mc4);
//            testSession9(mc, mc2);
//            testSession10(mc, mc2);
//            testSession11(mc, mc2);
//            testSession12(mc, mc2);
//            testSession13(mc);			
//            testSession14(mc);
//            testSession15(mc);
//            testSession16(mc);
//            testSession17(mc);
//            testSession18(mc);
//            testSession19(mc);
//            testSession20(mc);
//            testSession21(mc, mc2);
//            testSession22(mc, mc2);
//            testSession23(mc);
//            testSession24(mc);
//            testSession25(mc);
//            testSession26(mc);
//            testSession27(mc);
//            testSession28(mc, mc2);
//            testSession29(mc, mc2);
//            testSession30(mc, mc2, mc3);
//            testSession31(mc);
//            testSession32(mc);
//            testSession33(mc, mc2);
//            testSession34(mc, mc2);
//            testSession35(mc, mc2);
//            testSession36(mc);
//            testSession37(mc);
//            testSession38(mc);
//            testSession39(mc);
//            testSession40(mc);
//            
//            testSession42(mc);
//            testSession43(mc);
//            testSession44(mc, mc2);
//            testSession45(mc);
//            testSession46(mc);
//            testSession47(mc);
//            testSession48(mc);
//            testSession49(mc);
//            testSession50(mc);
//            
//            testCOMultiget1(mc);
//            testCOMultiget2(mc);
//            testLeasesRepeat(mc);
//            testMultiCOAppends1(mc);
//            testMultiCOAppends2(mc, mc2);
//            testCOMultiget3(mc);
////            testMultiCOAppends3(mc);
//            testMultiOQSwapsNull(mc);
//            testOqRead(mc);
//            testOqWrite(mc);
//            testUpgradeLease(mc, mc2);
//            testOQAppendContinuously(mc);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void testBlockWrite(MemcachedClient mc, 
            MemcachedClient mc2, MemcachedClient mc3) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.disableBackoff();
        mc2.disableBackoff();
        mc3.disableBackoff();

        mc.iqappend("key1", 2, "tid1");
        mc.commit("tid1", null);
        mc3.iqget("key1");
        mc2.iqappend("key1", 6, "tid2");
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        mc.finishTrans("tid1", keyMap);
        mc3.iqget("key1");
        mc3.iqset("key1", "val3");
        assert mc2.commit("tid", null) == false;
        assert mc.iqget("key1").equals("val3");
    }

    private static void testBlockWrite2(MemcachedClient mc, 
            MemcachedClient mc2, MemcachedClient mc3) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.disableBackoff();
        mc2.disableBackoff();
        mc3.disableBackoff();

        mc.iqincr("key1", 2, "tid1");
        mc3.iqget("key1");
        mc.commit("tid1", null);
        mc2.iqincr("key1", 6, "tid2");
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        mc.finishTrans("tid1", keyMap);
        mc3.iqget("key1");
        mc3.iqset("key1", "val3");
        assert mc2.commit("tid", null) == false;
        assert mc.iqget("key1").equals("val3");
    }

    private static void testBlockWrite3(MemcachedClient mc, 
            MemcachedClient mc2, MemcachedClient mc3) throws Exception {
        mc.delete("key1");
        mc.delete("key2");
        mc.disableBackoff();
        mc2.disableBackoff();
        mc3.disableBackoff();

        mc.quarantineAndRead("tid1", "key1", 1, false);
        mc.swapAndRelease("key1", null);
        mc3.iqget("key1");
        mc2.quarantineAndRead("tid2", "key1", 1, false);
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        mc.finishTrans("tid1", keyMap);
        mc3.iqget("key1");
        mc3.iqset("key1", "val3");
        mc2.swapAndRelease("key1", null);
        assert mc.iqget("key1").equals("val3");
    }

    private static void testBlockWrite4(MemcachedClient mc, MemcachedClient mc2, 
            MemcachedClient mc3, MemcachedClient mc4) throws Exception {
        mc.delete("key1");
        mc.disableBackoff();
        mc2.disableBackoff();
        mc3.disableBackoff();
        mc4.disableBackoff();

        mc.iqincr("key1", 2, "tid1");
        mc.commit("tid1", null);
        mc2.iqincr("key1", 6, "tid2");
        mc3.iqget("key1");
        mc2.commit("tid2", null);
        mc3.iqget("key1");
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        mc.finishTrans("tid1", keyMap);
        mc3.iqget("key1");
        assert mc4.iqincr("key1", 6, "tid3") == false;
        mc2.finishTrans("tid2", keyMap);
        mc3.iqget("key1");
        mc3.iqset("key1", "val3");
        assert mc3.iqget("key1").equals("val3");
    }

    public static void test_qaread(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.delete("key1");
        mc2.iqappend("key1", 2, "tid1");
        mc.enableBackoff();
        assert mc.quarantineAndRead("tid1", "key1", 1, false) == null;
        mc.swapAndRelease("key1", "val2");
        assert mc.iqget("key1").equals("val2");
    }

    public static void test_qareg_session(MemcachedClient mc) throws Exception {
        mc = new MemcachedClient("test");

        mc.enableBackoff();

        mc.delete("key1");
        mc.iqget("key1");
        mc.iqset("key1", "val1");
        assert mc.iqget("key1").equals("val1");

        String tid = mc.generateSID();
        mc.quarantineAndRegister(tid, "key1");
        assert mc.iqget("key1") == null;	// prevent read same session
        assert mc.commit(tid, null);
    }

    public static void test_getprik(MemcachedClient mc, MemcachedClient mc2) throws Exception {
        mc.disableBackoff();
        mc2.disableBackoff();

        mc.delete("key1");
        mc.iqappend("key1", "2", "tid1");
        assert mc2.iqget("key1") == null;
        List<String> keys = mc.getPriKeys();
        assert keys.size() == 1;
        assert keys.get(0).equals("key1");
        mc.commit("tid1", null);
        HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
        keyMap.put("key1", 1);
        mc.finishTrans("tid1", keyMap);
        keys = mc.getPriKeys();
        assert keys.size() == 0;
        mc.iqget("key1");
        assert mc.iqset("key1", "val1");
        assert mc.iqget("key1").equals("val1");
    }

    /** 
     * Class for testing serializing of objects. 
     * 
     * @author $Author: $
     * @version $Revision: $ $Date: $
     */
    public static final class TestClass implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        private String field1;
        private String field2;
        private Integer field3;

        public TestClass( String field1, String field2, Integer field3 ) {
            this.field1 = field1;
            this.field2 = field2;
            this.field3 = field3;
        }

        public String getField1() { return this.field1; }
        public String getField2() { return this.field2; }
        public Integer getField3() { return this.field3; }

        public boolean equals( Object o ) {
            if ( this == o ) return true;
            if ( !( o instanceof TestClass ) ) return false;

            TestClass obj = (TestClass)o;

            return ( ( this.field1 == obj.getField1() || ( this.field1 != null && this.field1.equals( obj.getField1() ) ) )
                    && ( this.field2 == obj.getField2() || ( this.field2 != null && this.field2.equals( obj.getField2() ) ) )
                    && ( this.field3 == obj.getField3() || ( this.field3 != null && this.field3.equals( obj.getField3() ) ) ) );
        }
    }
}
