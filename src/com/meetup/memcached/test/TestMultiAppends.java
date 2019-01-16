package com.meetup.memcached.test;

import java.io.IOException;

import com.meetup.memcached.COException;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

public class TestMultiAppends {
    public static String KEY = "key";
    
    public static void kAppends(MemcachedClient mc, String sid, byte[] val, String[] keys) throws IOException, COException {        
        for (int i = 0; i < keys.length; ++i) { 
            mc.oqAppend(sid, keys[i], val, false);
        }
        
        mc.validate(sid, 0);
        mc.dCommit(sid);
    }
    
    public static void oneMultiAppend(MemcachedClient mc, String sid, String[] keys, Object[] values) throws IOException, COException {        
        mc.oqAppends(sid, keys, values, 0, false);
        
        mc.validate(sid, 0);
        mc.dCommit(sid);
    }
    
    public static void kSwaps(MemcachedClient mc, String sid, byte[] val, String[] keys) throws IOException, COException {
        for (int i = 0; i < keys.length; ++i) { 
            mc.oqRead(sid, keys[i], 0, false);
        }
        
        for (int i = 0; i < keys.length; ++i) { 
            mc.oqSwap(sid, keys[i], 0, val);
        }
        
        mc.validate(sid, 0);
        mc.dCommit(sid);
    }
    
    public static void oneSwap(MemcachedClient mc, String sid, String[] keys, Object[] values) throws IOException, COException {        
        mc.co_getMulti(sid, 0, 1, false, keys);
        mc.oqSwaps(sid, keys, values, 0, false);
        
        mc.validate(sid, 0);
        mc.dCommit(sid);
    }    
    
    public static void main(String[] args) throws IOException, COException {
        // specify the list of cache server
        // currently test on a single cache server only
        String[] serverlist = { "10.0.0.220:11211" };

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
        
        MemcachedClient mc = new MemcachedClient("test");
        
        int loop = 1000;
        int[] ks = new int[] { 5, 10, 20, 50, 100 };
        int[] blens = new int[] { 50, 100, 200 };
        
        System.out.println("=============== Test sequential get and swap");
        
        for (int k: ks) {
            for (int blen: blens) {         
                String[][] keys = new String[loop][];
                for (int i = 0; i < loop; ++i) {
                    keys[i] = new String[k];
                    for (int j = 0; j < k; ++j) {
                        keys[i][j] = "key_"+i+"_"+j;
                    }
                }
                
                byte[] bytes = new byte[blen];
                
                long time = System.nanoTime();
                
                for (int i = 0; i < loop; i++) {
                    kSwaps(mc, "sid1", bytes, keys[i]);
                }
                
                time = System.nanoTime() - time;
                
                System.out.println(String.format("kAppends k=%d bytelen=%d avr time=%.2f msec", k, blen, (double)time / 1000 / 1000 / 1000));
            }
        }
        
        System.out.println("================ Test one multiGet + one multiSwap");
        
        for (int k: ks) {
            for (int blen: blens) {                
                String[][] keys = new String[loop][];
                for (int i = 0; i < loop; ++i) {
                    keys[i] = new String[k];
                    for (int j = 0; j < k; ++j) {
                        keys[i][j] = "key_"+i+"_"+j;
                    }
                }
                Object[][] values = new Object[loop][];
                for (int i = 0; i < loop; ++i) {
                    values[i] = new Object[k];
                    for (int j = 0; j < k; ++j) {
                        values[i][j] = new byte[blen];
                    }
                }
                
                long time = System.nanoTime();
                
                for (int i = 0; i < loop; i++) {
                    oneSwap(mc, "sid1", keys[i], values[i]);
                }
                
                time = System.nanoTime() - time;
                
                System.out.println(String.format("oneMultiAppends k=%d bytelen=%d avr time=%.2f", k, blen, (double)time / 1000 / 1000));
            }
        }
        
        System.out.println("=============== Test sequential oqAppend");
        
        for (int k: ks) {
            for (int blen: blens) {
                String[][] keys = new String[loop][];
                for (int i = 0; i < loop; ++i) {
                    keys[i] = new String[k];
                    for (int j = 0; j < k; ++j) {
                        keys[i][j] = "key_"+i+"_"+j;
                    }
                }
                
                byte[] bytes = new byte[blen];
                
                long time = System.nanoTime();
                
                for (int i = 0; i < loop; i++) {
                    kAppends(mc, "sid1", bytes, keys[i]);
                }
                
                time = System.nanoTime() - time;
                
                System.out.println(String.format("kAppends k=%d bytelen=%d avr time=%.2f", k, blen, (double)time / 1000 / 1000));
            }
        }
        
        System.out.println("================ Test one multiAppend");
        
        for (int k: ks) {
            for (int blen: blens) {
                String[][] keys = new String[loop][];
                for (int i = 0; i < loop; ++i) {
                    keys[i] = new String[k];
                    for (int j = 0; j < k; ++j) {
                        keys[i][j] = "key_"+i+"_"+j;
                    }
                }
                Object[][] values = new Object[loop][];
                for (int i = 0; i < loop; ++i) {
                    values[i] = new Object[k];
                    for (int j = 0; j < k; ++j) {
                        values[i][j] = new byte[blen];
                    }
                }
                
                long time = System.nanoTime();
                
                for (int i = 0; i < loop; i++) {
                    oneMultiAppend(mc, "sid1", keys[i], values[i]);
                }
                
                time = System.nanoTime() - time;
                
                System.out.println(String.format("oneMultiAppends k=%d bytelen=%d avr time=%.2f", k, blen, (double)time / 1000 / 1000));
            }
        }
    }
}
