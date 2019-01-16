package com.meetup.memcached;

import org.apache.log4j.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BDBCacheClient {

    static List<String> twemcached = new ArrayList<>();
    static String twemcachedDir = "/home/haoyu/Documents/wb/IQ-Twemcached-wb";

    public static void before(String twemcache) throws Exception {
        String uuid = UUID.randomUUID().toString();
        if (twemcache.contains("%s")) {
            twemcache = String.format(twemcache, uuid);
        }
        System.out.println("Running " + twemcache);

        Process p = Runtime.getRuntime().exec("mkdir /tmp/bdb/" + uuid);
        p.waitFor();

        final Process killp = Runtime.getRuntime().exec("killall twemcache");
        killp.waitFor();

        final Process twemp = Runtime.getRuntime().exec(twemcache);
        Thread.sleep(5000);
        new Thread(new Runnable() {
            public void run() {
                BufferedReader input = new BufferedReader(new InputStreamReader(twemp.getInputStream()));
                String line = null;

                try {
                    while ((line = input.readLine()) != null)
                        System.out.println(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ConsoleAppender console = new ANSIConsoleAppender(); // create appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        org.apache.log4j.Logger.getRootLogger().removeAllAppenders();
        org.apache.log4j.Logger.getRootLogger().addAppender(console);
        org.apache.log4j.Logger.getLogger(MemcachedClient.class).setLevel(Level.DEBUG);

        twemcached.add(String.format("%s/src/twemcache -m 10000 -t 4 -c 10 -g 1000 -G 999999", twemcachedDir));
        twemcached.add(twemcachedDir + "/src/twemcache -m 10000 -t 4 -c 10 -g 1000 -G 999999 -q 1 -i /tmp/bdb/%s -w 10 -F 1");
        twemcached.add(twemcachedDir + "/src/twemcache -m 10000 -t 4 -c 10 -g 1000 -G 999999 -q 1 -i /tmp/bdb/%s -w 10 -F 1 -Q 1");

        testAll();
    }

    private static void testAll() throws Exception {

        for (int i = 0; i < twemcached.size(); i++) {
            before(twemcached.get(i));
            normalCase();
            before(twemcached.get(i));
            testSession();
            before(twemcached.get(i));
            testSyncBDB(i != 2);
        }

        before(twemcached.get(2));
        testDeque();
    }


    public static void testSession() throws Exception {
        MemcachedClient mc = getMemcachedClient();

        mc.set("key1", "val1".getBytes(), false);  // for read-modify-write
        mc.set("key2", "val2".getBytes(), false);  // for write
        mc.set("key3", "val3", true);  // for append
        mc.set("BW_key1", "bw1", true);    // for append
        mc.set("TW_w,1,xxxx", "tw1", true);
        mc.set("TW_w,2,xxxx", "tw2", true);

        System.out.println("#####" + mc.get("BW_key1", 0, true));

        String sid = mc.generateSID();
        mc.oqRead(sid, "key1", 0, false);
        mc.oqSwap(sid, "key1", 0, "val1prime".getBytes(), false);
        mc.oqWrite(sid, "key2", 0, "val2prime".getBytes(), false);
        mc.oqAppend(sid, "key3", "prime", true);

        assert mc.oqAppend(sid, "BW_key1", "prime", true);
        assert !mc.oqAppend(sid, "BW_key2", "bw2", true);
        assert mc.oqAdd(sid, "BW_key2", "bw2prime", 0, true);

        assert mc.oqWrite(sid, "S-abc", 0, "test".getBytes(), false);
        mc.oqAppend(sid, "TW_w,1,xxxx", "prime", true);
        mc.oqAppend(sid, "TW_w,2,xxxx", "prime", true);

        mc.dCommit(sid);

        Object obj = mc.get("key1", 0, false);
        String str = new String((byte[]) obj);
        assert str.equals("val1prime");
        obj = mc.get("key2", 0, false);
        str = new String((byte[]) obj);
        assert str.equals("val2prime");
        assert mc.get("key3", 0, true).equals("val3prime");

        System.out.println("#####" + mc.get("BW_key1", 0, true));

        assert mc.get("BW_key1", 0, true).equals("bw1prime");
        assert mc.get("BW_key2", 0, true).equals("bw2prime");
        System.out.println(mc.get("S-abc", 0, false));

        String line = new String((byte[]) mc.get("S-abc", 0, false));
        System.out.println(line);
        assert "test".equals(line);
        System.out.println(mc.get("TW_w,1,xxxx", 0, true));
        assert mc.get("TW_w,1,xxxx", 0, true).equals("tw1prime");
        assert mc.get("TW_w,2,xxxx", 0, true).equals("tw2prime");

        System.out.println("Test Session OK.");
    }

    public static void testSyncBDB(boolean isNotSyncDB) throws Exception {
        MemcachedClient mc = getMemcachedClient();

        assert mc.set("TW_w,1,xxxx", ";", true);
        String sid = mc.generateSID();

        assert mc.oqWrite(sid, "S-abc", 0, "test".getBytes(), false);
        assert !mc.oqAppend(sid, "BW_a", 0, ";abc;", true);
        assert mc.oqAdd(sid, "BW_a", ";abc;", 0, true);
        assert mc.oqAppend(sid, "TW_w,1,xxxx", 0, "abc;", true);
        assert mc.dCommit(sid);

        System.out.println("#####BW_a: " + mc.get("BW_a"));

        System.out.println("########" + mc.get("TW_w,1,xxxx"));
        sid = mc.generateSID();
        assert mc.oqWrite(sid, "S-def", 0, "test".getBytes(), false);
        assert mc.oqAppend(sid, "BW_a", 0, ";def;", true);
        assert mc.oqAppend(sid, "TW_w,1,xxxx", 0, ";def;", true);
        mc.dCommit(sid);

        System.out.println("#####BW_a: " + mc.get("BW_a"));

        if (isNotSyncDB) {
            sid = mc.generateSID();
            System.out.println("########" + mc.get("TW_w,1,xxxx"));
            assert mc.oqRead(sid, "TW_w,1,xxxx", 0, true).equals(";abc;;def;");
            assert mc.oqSwap(sid, "TW_w,1,xxxx", 0, ";def;", true);
            mc.dCommit(sid);

            System.out.println(mc.get("TW_w,1,xxxx"));
            assert mc.get("TW_w,1,xxxx").equals(";def;");
        } else {
            assert mc.get("TW_w,1,xxxx").equals(";abc;;def;");
        }
        System.out.println("#####BW_a: " + mc.get("BW_a"));
        assert mc.get("BW_a").equals(";abc;;def;");
        System.out.println("#####S-abc: " + mc.get("S-abc"));
        assert new String((byte[]) mc.get("S-abc")).equals("test");
        assert new String((byte[]) mc.get("S-def")).equals("test");
        System.out.println("Test SynC BDB OK.");
    }

    public static void testDeque() throws Exception {
        MemcachedClient mc = getMemcachedClient();

        String sid = mc.generateSID();
        assert !mc.oqAppend(sid, "BW_abc", ";1-xx", true);
        assert mc.oqAdd(sid, "BW_abc", ";1-xx", 0, true);
        mc.dCommit(sid);

        System.out.println(mc.get("BW_abc"));

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "BW_abc", ";2-xx", true);
        mc.dCommit(sid);

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "BW_abc", ";1-xy", true);
        mc.dCommit(sid);

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "BW_abc", ";1-xz", true);
        mc.dCommit(sid);

        mc.deque("BW_abc", 1, 2);
        assert mc.get("BW_abc", 0, true).equals(";2-xx;1-xz");

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "TW_w,1,xxxx", ";1-xx", true);
        mc.dCommit(sid);

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "TW_w,1,xxxx", ";1-xy", true);
        mc.dCommit(sid);

        sid = mc.generateSID();
        assert mc.oqAppend(sid, "TW_w,1,xxxx", ";1-xz", true);
        mc.dCommit(sid);

        mc.deque("TW_w,1,xxxx", 1, 2);
        assert mc.get("TW_w,1,xxxx", 0, true).equals(";1-xz");
        System.out.println("Test Deque success.");
    }

    public static void check(boolean val, String msg) {
        if (!val) {
            System.out.println(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void normalCase() throws Exception {
        // initialize the pool for memcache servers

        MemcachedClient mc = getMemcachedClient();
        System.out.println(mc.get("S-1"));

        List<String> val = new ArrayList<>();
        mc.set("normal", "normal");
        System.out.print(mc.get("normal"));

        for (int i = 0; i < 14; i++) {
            val.add(String.format("%d-%s", i, UUID.randomUUID().toString()));
            mc.set("S-" + i, val.get(i).getBytes(), false);
            String memVal = new String((byte[]) mc.get("S-" + i));
//            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        mc.set("TW_w,1,0000", ";0;1;2;3");
        mc.set("TW_w,1,0001", ";4;5;6;7");
        mc.set("TW_w,1,xxxx", ";10;11;12;13");
        mc.set("idxs_TW_w,1,xxxx", "0,1");
//        System.out.println("#####################3" + mc.get("TW_w,1,xxxx"));

        Thread.sleep(3000l);

        for (int i = 0; i < 14; i++) {
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("persisted key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        for (int i = 14; i < 18; i++) {
            val.add(String.format("%d-%s", i, UUID.randomUUID().toString()));
            mc.set("S-" + i, val.get(i).getBytes(), false);
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        mc.set("TW_w,1,xxxx", ";10;11;12;13;14;15;16;17");
        System.out.println(mc.get("TW_w,1,xxxx"));

        for (int i = 0; i < 17; i++) {
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }
        //
        for (int i = 18; i <= 20; i++) {
            val.add(String.format("%d-%s", i, UUID.randomUUID().toString()));
            mc.set("S-" + i, val.get(i).getBytes(), false);
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        mc.set("TW_w,1,0002", ";10;11;12;13;14;15;16;17");
        mc.set("TW_w,1,xxxx", ";18;19;20");
        mc.set("idxs_TW_w,1,xxxx", "1,2");

        System.out.println(mc.get("TW_w,1,xxxx"));

        for (int i = 0; i < 21; i++) {
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        for (int i = 0; i < 21; i++) {
            String memVal = new String((byte[]) mc.get("S-" + i));
            check(val.get(i).equals(memVal), String.format("key %s, %s, %s", "S-" + i, val.get(i), memVal));
        }

        Thread.sleep(5000l);

        for (int i = 0; i < 21; i++) {
            mc.delete("S-" + i);
            check(mc.get("S-" + i) == null, "value is not null");
        }
        mc.set("normal", "normal");
        System.out.print(mc.get("normal"));
        System.out.println("Passed!");
    }

    private static MemcachedClient getMemcachedClient() {
        // get client instance
        String[] serverlist = {"127.0.0.1:11211"};

        String uid = String.valueOf(UUID.randomUUID().toString());
        SockIOPool pool = SockIOPool.getInstance(uid);
        pool.setServers(serverlist);

        pool.setInitConn(1);
        pool.setMinConn(1);
        pool.setMaxConn(10);
        pool.setMaintSleep(0);
        pool.setNagle(false);
        pool.initialize();

        MemcachedClient mc = new MemcachedClient(uid);
        mc.setCompressEnable(false);
        return mc;
    }
}