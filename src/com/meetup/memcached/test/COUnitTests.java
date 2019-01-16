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

public class COUnitTests {

    // logger
    private static Logger log =
            Logger.getLogger( COUnitTests.class.getName() );

    public static MemcachedClient mc  = null;
    public static final int EXP_LEASE_TIME = 1000;

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
            mc1.validate("sid1", 0);
            assert false;
        } catch (COException e) { }
        mc2.validate("sid2", 0);
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
        mc1.validate("sid1", 0);
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
        assert mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
        mc1.dCommit("sid1");
        System.out.println("OK");
    }

    public static void testSession17(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 17 CO rmv oqwrite normal ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("key2"); 
        mc1.delete("key3"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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

        mc1.validate("sid1", 0);
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

        mc1.validate("sid1", 0);
        mc1.dCommit("sid1");
        assert mc1.get("key1").equals("val1");
        System.out.println("OK");
    }

    public static void testSession23(MemcachedClient mc1) throws Exception {
        System.out.print("Test session 23 CO w oqwrite abort session ...");
        mc1.cleanup(); 
        mc1.delete("key1"); mc1.delete("sid1");
        mc1.oqWrite("sid1", "key1", "val1");
        mc1.validate("sid1", 0);
        mc1.unleaseCO("sid1");
        assert mc1.get("key1") == null;

        mc1.set("key1", "val1");
        mc1.oqWrite("sid1", "key1", "val2");
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
        mc1.validate("sid1", 0);
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
            mc1.validate("sid1", 0);
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

        mc1.validate("sid1", 0);
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

        mc3.validate("sid3", 0);
        mc3.dCommit("sid3");
        //		System.out.println(mc1.get("key1"));
        assert mc1.get("key1").equals("5");
        assert mc1.get("key2").equals("val2");
        assert mc1.ciget("sid1", "key3") == null;
        assert mc1.iqset("key3", "val3");
        assert mc1.ciget("sid1", "key3").equals("val3");
        assert mc1.validate("sid1", 0);
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
        assert mc.validate("sid", 0);
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
        assert mc.validate("sid", 0);
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
        assert mc2.validate("sid2", 0);
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
        assert mc.oqprepend("sid1", "key1", "val3");
        assert mc.get("key1").equals("val1");
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val3val1val2");
        System.out.println("OK");
    }

    public static void testSession37(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 37 CO oqappend null value...");
        mc.cleanup(); mc.delete("key1"); mc.delete("sid1");
        assert (!mc.oqAppend("sid1", "key1", "val2", true));
        assert mc.get("key1") == null;
        assert (!mc.oqprepend("sid1", "key1", "val3"));
        assert mc.get("key1") == null;
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1");
        assert mc.get("key1") == null;
        System.out.println("OK");
    }

    public static void testSession38(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 38 CO write session...");
        mc.cleanup(); mc.delete("key1"); mc.delete("key2"); mc.delete("sid1");
        assert mc.oqWrite("sid1", "key1", "val1");
        assert mc.oqAppend("sid1", "key1", "val2", true);
        assert mc.get("key1") == null;
        assert mc.oqprepend("sid1", "key1", "val3");
        assert mc.get("key1") == null;
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val3val1val2");
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
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val1");

        mc.delete("key1");
        assert mc.ciget("sid1", "key1") == null;
        assert mc.iqset("key1", "val1");
        assert mc.get("key1").equals("val1");
        assert mc.oqWrite("sid1", "key1", "val2");
        assert mc.get("key1").equals("val1");
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1");
        assert mc.get("key1").equals("val2");

        System.out.println("OK");
    }

    public static void testSession40(MemcachedClient mc) 
            throws Exception {
        System.out.print("Test session 40 append incr non-existing key.");
        mc.cleanup(); mc.delete("key1"); mc.delete("key2"); mc.delete("sid1");
        assert (!mc.oqAppend("sid1", "key1", "val1", true));
        assert mc.validate("sid1", 0);
        assert mc.dCommit("sid1", 0);
        assert mc.get("key1") == null;
        assert !mc.oqAppend("sid2", "key1", "val2", true);
        assert mc.validate("sid2", 0);
        assert mc.dCommit("sid2", 0);
        assert mc.get("key2") == null;
        System.out.println("OK");
    }

    public static void cleanup(MemcachedClient mc1) {
        mc1.cleanup();
    }
    
    public static void testSession41(MemcachedClient mc) throws Exception {
        System.out.println("Test Memory leak.");
        mc.cleanup(); mc.delete("sid1"); mc.delete("key1"); mc.delete("key2");
        
        int i = 0;
        while (i < 100) {
//            mc.delete("key1"); 
//            mc.delete("key2");
            
            i++;
            Object obj = mc.ciget("sid"+i, "key1");
            if (obj == null) 
                mc.iqset("key1", "val1");
            obj = mc.ciget("sid"+i, "key2");
            if (obj == null)
                mc.iqset("key2", "val2");
            
            mc.validate("sid"+i, 0);
            mc.dCommit("sid"+i);
//            if (i == 1) break;
        }
    }
    
    public static void testSession42(MemcachedClient mc) throws Exception {
        System.out.print("Test session 42 Read after Swap...");
        assert mc.set("key1", "val1");
        assert mc.oqRead("sid1", "key1").equals("val1");
        assert mc.oqSwap("sid1", "key1", "val2");
        String val = (String) mc.oqRead("sid1", "key1");
//        System.out.println(val);
        assert val.equals("val2");
        assert mc.oqSwap("sid1", "key1", "val3");
        mc.validate("sid1", 0);
        mc.dCommit("sid1");
        assert mc.get("key1").equals("val3");
        System.out.println("OK");
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
        if ( !COUnitTests.class.desiredAssertionStatus() ) {
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
            testSession1(mc);
            testSession2(mc);
            testSession3(mc, mc2);
            testSession4(mc, mc2);
            testSession5(mc);
            testSession6(mc);
            testSession7(mc, mc2);
            testSession8(mc, mc2, mc3, mc4);
            testSession9(mc, mc2);
            testSession10(mc, mc2);
            testSession11(mc, mc2);
            testSession12(mc, mc2);
            testSession13(mc);			
            testSession14(mc);
            testSession15(mc);
            testSession16(mc);
            testSession17(mc);
            testSession18(mc);
            testSession19(mc);
            testSession20(mc);
            testSession21(mc, mc2);
            testSession22(mc, mc2);
            testSession23(mc);
            testSession24(mc);
            testSession25(mc);
            testSession26(mc);
            testSession27(mc);
            testSession28(mc, mc2);
            testSession29(mc, mc2);
            testSession30(mc, mc2, mc3);
            testSession31(mc);
            testSession32(mc);
            testSession33(mc, mc2);
            testSession34(mc, mc2);
            testSession35(mc, mc2);
            testSession36(mc);
            testSession37(mc);
            testSession38(mc);
            testSession39(mc);
            testSession40(mc);
//            testSession41(mc);
            testSession42(mc);
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
}
