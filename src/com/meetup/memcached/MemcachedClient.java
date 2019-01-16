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
 * @author Greg Whalin <greg@meetup.com> 
 */
package com.meetup.memcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.sun.xml.internal.txw2.output.StreamSerializer;

/**
 * This is a Memcached client for the Java platform available from
 * <a href="http:/www.danga.com/memcached/">http://www.danga.com/memcached/</a>.
 * <br/>
 * Supports setting, adding, replacing, deleting compressed/uncompressed and
 * <br/>
 * serialized (can be stored as string if object is native class) objects to
 * memcached.<br/>
 * <br/>
 * Now pulls SockIO objects from SockIOPool, which is a connection pool. The
 * server failover<br/>
 * has also been moved into the SockIOPool class.<br/>
 * This pool needs to be initialized prior to the client working. See javadocs
 * from SockIOPool.<br/>
 * <br/>
 * Some examples of use follow.<br/>
 * <h3>To create cache client object and set params:</h3>
 * 
 * <pre>
 * 
 * MemcachedClient mc = new MemcachedClient();
 *
 * // compression is enabled by default
 * mc.setCompressEnable(true);
 *
 * // set compression threshhold to 4 KB (default: 15 KB)
 * mc.setCompressThreshold(4096);
 *
 * // turn on storing primitive types as a string representation
 * // Should not do this in most cases.
 * mc.setPrimitiveAsString(true);
 * </pre>
 * 
 * <h3>To store an object:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "cacheKey1";
 * Object value = SomeClass.getObject();
 * mc.set(key, value);
 * </pre>
 * 
 * <h3>To store an object using a custom server hashCode:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "cacheKey1";
 * Object value = SomeClass.getObject();
 * Integer hash = new Integer(45);
 * mc.set(key, value, hash);
 * </pre>
 * 
 * The set method shown above will always set the object in the cache.<br/>
 * The add and replace methods do the same, but with a slight difference.<br/>
 * <ul>
 * <li>add -- will store the object only if the server does not have an entry
 * for this key</li>
 * <li>replace -- will store the object only if the server already has an entry
 * for this key</li>
 * </ul>
 * <h3>To delete a cache entry:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "cacheKey1";
 * mc.delete(key);
 * </pre>
 * 
 * <h3>To delete a cache entry using a custom hash code:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "cacheKey1";
 * Integer hash = new Integer(45);
 * mc.delete(key, hashCode);
 * </pre>
 * 
 * <h3>To store a counter and then increment or decrement that counter:</h3>
 * 
 * <pre>
 *	MemcachedClient mc = new MemcachedClient();
 *	String key   = "counterKey";	
 *	mc.storeCounter(key, new Integer(100));
 *	System.out.println("counter after adding      1: " mc.incr(key));	
 *	System.out.println("counter after adding      5: " mc.incr(key, 5));	
 *	System.out.println("counter after subtracting 4: " mc.decr(key, 4));	
 *	System.out.println("counter after subtracting 1: " mc.decr(key));
 * </pre>
 * 
 * <h3>To store a counter and then increment or decrement that counter with
 * custom hash:</h3>
 * 
 * <pre>
 *	MemcachedClient mc = new MemcachedClient();
 *	String key   = "counterKey";	
 *	Integer hash = new Integer(45);	
 *	mc.storeCounter(key, new Integer(100), hash);
 *	System.out.println("counter after adding      1: " mc.incr(key, 1, hash));	
 *	System.out.println("counter after adding      5: " mc.incr(key, 5, hash));	
 *	System.out.println("counter after subtracting 4: " mc.decr(key, 4, hash));	
 *	System.out.println("counter after subtracting 1: " mc.decr(key, 1, hash));
 * </pre>
 * 
 * <h3>To retrieve an object from the cache:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "key";
 * Object value = mc.get(key);
 * </pre>
 * 
 * <h3>To retrieve an object from the cache with custom hash:</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String key = "key";
 * Integer hash = new Integer(45);
 * Object value = mc.get(key, hash);
 * </pre>
 * 
 * <h3>To retrieve an multiple objects from the cache</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String[] keys = { "key", "key1", "key2" };
 * Map&lt;Object&gt; values = mc.getMulti(keys);
 * </pre>
 * 
 * <h3>To retrieve an multiple objects from the cache with custom hashing</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * String[] keys = { "key", "key1", "key2" };
 * Integer[] hashes = { new Integer(45), new Integer(32), new Integer(44) };
 * Map&lt;Object&gt; values = mc.getMulti(keys, hashes);
 * </pre>
 * 
 * <h3>To flush all items in server(s)</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * mc.flushAll();
 * </pre>
 * 
 * <h3>To get stats from server(s)</h3>
 * 
 * <pre>
 * MemcachedClient mc = new MemcachedClient();
 * Map stats = mc.stats();
 * </pre>
 *
 * @author greg whalin <greg@meetup.com>
 * @author Richard 'toast' Russo <russor@msoe.edu>
 * @author Kevin Burton <burton@peerfear.org>
 * @author Robert Watts <robert@wattsit.co.uk>
 * @author Vin Chawla <vin@tivo.com>
 * @version 1.5
 */
public class MemcachedClient {

    // logger
    private static Logger log = Logger.getLogger(MemcachedClient.class.getName());

    // return codes
    private static final String VALUE = "VALUE"; // start of value line from
    // server
    private static final String STATS = "STAT"; // start of stats line from
    // server
    private static final String ITEM = "ITEM"; // start of item line from server
    private static final String DELETED = "DELETED"; // successful deletion
    private static final String NOTFOUND = "NOT_FOUND"; // record not found for
    // delete or incr/decr
    private static final String STORED = "STORED"; // successful store of data
    private static final String NOTSTORED = "NOT_STORED"; // data not stored
    private static final String EXISTS = "EXISTS"; // exists
    private static final String OK = "OK"; // success
    private static final String END = "END"; // end of data from server
    private static final String INVALID = "INVALID";
    private static final String ABORT = "ABORT";
    private static final String RETRY = "RETRY";
    private static final String NOT_PERSISTED = "NOT_PERSISTED";

    private static final String LEASEVALUE = "LVALUE"; // start of a lease token
    // line from server
    private static final String LEASE = "LEASE"; // start of a lease token line
    // from server for a hold
    private static final String NOVALUE = "NOVALUE"; // no value return

    private static final String ERROR = "ERROR"; // invalid command name from
    // client
    private static final String CLIENT_ERROR = "CLIENT_ERROR"; // client error
    // in input line
    // - invalid
    // protocol
    private static final String SERVER_ERROR = "SERVER_ERROR"; // server error

    private static final byte[] B_END = "END\r\n".getBytes();

    // default compression threshold
    private static final int COMPRESS_THRESH = 30720;

    // default lease token value
    private static final long TOKEN_HOTMISS = 3;

    private static final int DEFAULT_INITIAL_BACKOFF_VALUE = 50;
    private int INITIAL_BACKOFF_VALUE = DEFAULT_INITIAL_BACKOFF_VALUE;

    // Stats for leases
    private static boolean stats = false;
    private static AtomicInteger numBackoff;
    public static AtomicInteger numILeaseGranted; // number of ilease granted
    public static AtomicInteger numILeaseRelease; // number of ilease release
    public static AtomicInteger numIQGet; // number of iqget call
    public static AtomicInteger numIQSet; // number of iqset call
    public static AtomicInteger numIUnlease; // number of i-lease unlease

    // Max object size
    private static long MAX_OBJECT_SIZE = 1024 * 1024 * 5;

    // values for cache flags
    public static final int MARKER_BYTE = 1;
    public static final int MARKER_BOOLEAN = 8192;
    public static final int MARKER_INTEGER = 4;
    public static final int MARKER_LONG = 16384;
    public static final int MARKER_CHARACTER = 16;
    public static final int MARKER_STRING = 32;
    public static final int MARKER_STRINGBUFFER = 64;
    public static final int MARKER_FLOAT = 128;
    public static final int MARKER_SHORT = 256;
    public static final int MARKER_DOUBLE = 512;
    public static final int MARKER_DATE = 1024;
    public static final int MARKER_STRINGBUILDER = 2048;
    public static final int MARKER_BYTEARR = 4096;
    public static final int F_COMPRESSED = 2;
    public static final int F_SERIALIZED = 8;

    // flags
    private boolean sanitizeKeys;
    private boolean primitiveAsString;
    private boolean compressEnable;
    private long compressThreshold;
    private String defaultEncoding;

    // pool instance
    private SockIOPool pool;

    // which pool to use
    private String poolName;

    // optional passed in classloader
    private ClassLoader classLoader;

    // optional error handler
    private ErrorHandler errorHandler;

    // internal states for handling leases
    private ConcurrentHashMap<String, Long> i_lease_list;
    private ConcurrentHashMap<String, Long> q_lease_list;

    public static int maxBackoff = 0;
    
    private Stats st = null;

    /**
     * Creates a new instance of MemCachedClient.
     */
    public MemcachedClient() {
        init();
    }

    /**
     * Creates a new instance of MemCachedClient accepting a passed in pool
     * name.
     * 
     * @param poolName
     *            name of SockIOPool
     */
    public MemcachedClient(String poolName) {
        this.poolName = poolName;
        init();
    }

    /**
     * Creates a new instance of MemCacheClient but acceptes a passed in
     * ClassLoader.
     * 
     * @param classLoader
     *            ClassLoader object.
     */
    public MemcachedClient(ClassLoader classLoader) {
        this.classLoader = classLoader;
        init();
    }

    /**
     * Creates a new instance of MemCacheClient but acceptes a passed in
     * ClassLoader and a passed in ErrorHandler.
     * 
     * @param classLoader
     *            ClassLoader object.
     * @param errorHandler
     *            ErrorHandler object.
     */
    public MemcachedClient(ClassLoader classLoader, ErrorHandler errorHandler) {
        this.classLoader = classLoader;
        this.errorHandler = errorHandler;
        init();
    }

    /**
     * Creates a new instance of MemCacheClient but acceptes a passed in
     * ClassLoader, ErrorHandler, and SockIOPool name.
     * 
     * @param classLoader
     *            ClassLoader object.
     * @param errorHandler
     *            ErrorHandler object.
     * @param poolName
     *            SockIOPool name
     */
    public MemcachedClient(ClassLoader classLoader, ErrorHandler errorHandler, String poolName) {
        this.classLoader = classLoader;
        this.errorHandler = errorHandler;
        this.poolName = poolName;
        init();
    }

    /**
     * Initializes client object to defaults.
     *
     * This enables compression and sets compression threshhold to 15 KB.
     */
    private void init() {
        this.sanitizeKeys = false;
        this.primitiveAsString = false;
        this.compressEnable = false;
        this.compressThreshold = COMPRESS_THRESH;
        this.defaultEncoding = "UTF-8";
        this.poolName = (this.poolName == null) ? "default" : this.poolName;

        this.i_lease_list = new ConcurrentHashMap<String, Long>();
        this.q_lease_list = new ConcurrentHashMap<String, Long>();
        this.st = Stats.getStats();

        // get a pool instance to work with for the life of this instance
        this.pool = SockIOPool.getInstance(poolName);

        if (numBackoff == null) {
            numBackoff = new AtomicInteger(0);
        }

        if (numILeaseGranted == null) {
            numILeaseGranted = new AtomicInteger(0);
        }

        if (numILeaseRelease == null) {
            numILeaseRelease = new AtomicInteger(0);
        }

        if (numIQGet == null) {
            numIQGet = new AtomicInteger(0);
        }

        if (numIQSet == null) {
            numIQSet = new AtomicInteger(0);
        }

        if (numIUnlease == null) {
            numIUnlease = new AtomicInteger(0);
        }
    }

    /**
     * Disable back-off feature. The cache client then does not try to do iqget
     * multiple times until it can get either a value or an I lease.
     */
    public void disableBackoff() {
        this.INITIAL_BACKOFF_VALUE = 0;
    }

    /**
     * Enables the back-off feature. Whenever iqget experiences a get miss, it
     * retries until getting either value or an I lease.
     */
    public void enableBackoff() {
        this.INITIAL_BACKOFF_VALUE = DEFAULT_INITIAL_BACKOFF_VALUE;
    }

    public long getILeaseToken(String key) {
        Long l = this.i_lease_list.get(key);
        if (l != null)
            return l.longValue();

        return 0L;
    }

    /**
     * Sets an optional ClassLoader to be used for serialization.
     * 
     * @param classLoader
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Sets an optional ErrorHandler.
     * 
     * @param errorHandler
     */
    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    /**
     * Enables/disables sanitizing keys by URLEncoding.
     * 
     * @param sanitizeKeys
     *            if true, then URLEncode all keys
     */
    public void setSanitizeKeys(boolean sanitizeKeys) {
        this.sanitizeKeys = sanitizeKeys;
    }

    /**
     * Enables storing primitive types as their String values.
     * 
     * @param primitiveAsString
     *            if true, then store all primitives as their string value.
     */
    public void setPrimitiveAsString(boolean primitiveAsString) {
        this.primitiveAsString = primitiveAsString;
    }

    /**
     * Sets default String encoding when storing primitives as Strings. Default
     * is UTF-8.
     * 
     * @param defaultEncoding
     */
    public void setDefaultEncoding(String defaultEncoding) {
        this.defaultEncoding = defaultEncoding;
    }

    /**
     * Enable storing compressed data, provided it meets the threshold
     * requirements.
     *
     * If enabled, data will be stored in compressed form if it is<br/>
     * longer than the threshold length set with setCompressThreshold(int)<br/>
     * <br/>
     * The default is that compression is enabled.<br/>
     * <br/>
     * Even if compression is disabled, compressed data will be automatically
     * <br/>
     * decompressed.
     *
     * @param compressEnable
     *            <CODE>true</CODE> to enable compression, <CODE>false</CODE> to
     *            disable compression
     */
    public void setCompressEnable(boolean compressEnable) {
        this.compressEnable = compressEnable;
    }

    /**
     * Sets the required length for data to be considered for compression.
     *
     * If the length of the data to be stored is not equal or larger than this
     * value, it will not be compressed.
     *
     * This defaults to 15 KB.
     *
     * @param compressThreshold
     *            required length of data to consider compression
     */
    public void setCompressThreshold(long compressThreshold) {
        this.compressThreshold = compressThreshold;
    }

    public long getMaxObjectSize() {
        return MAX_OBJECT_SIZE;
    }

    public void setMaxObjectSize(long value) {
        MAX_OBJECT_SIZE = value;
    }

    /**
     * Checks to see if key exists in cache.
     * 
     * @param key
     *            the key to look for
     * @return true if key found in cache, false if not (or if cache is down)
     */
    public boolean keyExists(String key) {
        return (this.get(key, null, true) != null);
    }

    public boolean quarantineAndRegister(String tid, String key, Integer hashCode) throws Exception {
        if (key == null) {
            log.error("null value for key passed to quarantineAndRegister()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        // build command
        StringBuilder command = new StringBuilder("qareg ").append(key + " ").append(tid);
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (line.contains(LEASE)) {
                String[] info = line.split(" ");
                int markedVal = Integer.parseInt(info[1]);

                // no lease is granted
                if (markedVal == 0) {
                    if (log.isInfoEnabled())
                        log.info("++++ cannot grant lease of key " + key + " because someone is holding");

                    return false;
                }

                if (log.isInfoEnabled())
                    log.info("++++ lease of key: " + key + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ lease of key: " + key + " from cache failed as the key was not found");
            } else {
                log.error("++++ error qareg key: " + key);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                throw new Exception(
                        "Server error on QaReg request (" + tid + " " + key + "): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return false;
    }

    /**
     * Quarantine and register for a list of keys
     * 
     * @author hieun
     * @throws Exception
     */
    public boolean quarantineAndRegister(String tid, String key) throws Exception {
        return quarantineAndRegister(tid, key, null);
    }

    public boolean release(String tid) throws Exception {
        return release(tid, 0);
    }

    public boolean release(String tid, Integer hashCode) throws Exception {
        if (tid == null) {
            log.error("null value for key passed to deleteAndRelease()");
            return false;
        }

        try {
            tid = sanitizeKey(tid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, tid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        executeRelease(tid, hashCode);

        return true;
    }

    public boolean release(String tid, Set<Integer> hashCodes) throws Exception {
        if (tid == null) {
            log.error("null value for key passed to deleteAndRelease()");
            return false;
        }

        try {
            tid = sanitizeKey(tid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, tid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        if (hashCodes != null) {
            for (Integer hashCode : hashCodes) {
                executeRelease(tid, hashCode);
            }
        } else {
            executeRelease(tid, null);
        }

        return true;
    }

    private void executeRelease(String tid, Integer hashCode) throws Exception {
        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(tid, hashCode);

        // build command
        String command = String.format("release %s\r\n", tid);

        try {
            sock.write(command.getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ release of transaction id: " + tid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ release of transaction id: " + tid + " from cache found no tid or item");

                // return sock to pool and bail here
                sock.close();
                sock = null;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ release of key: " + tid + " from cache failed as the key was not found");
                sock.close();
                sock = null;
            } else {
                log.error("++++ error release key: " + tid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                throw new Exception("Server error on DaR request (" + tid + " ): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, tid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }
    }

    public String constructKeyFromTemplate(String queryTemplate, List<String> paramList) {
        queryTemplate += ":param:";
        for (String param : paramList) {
            queryTemplate += "_" + param;
        }

        return queryTemplate;
    }

    public Object ciget(String sid, String key) throws IOException, COException {
        return ciget(sid, key, false);
    }

    public Object ciget(String sid, String key, 
            boolean asString) throws IOException, COException {
        return ciget(sid, key, asString, null);
    }

    public Object ciget(String sid, String key, boolean asString, Integer hashCode) throws IOException, COException {
        if (sid == null) {
            log.error("null value for sid passed to ciget");
            return null;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);
            log.error("failed to sanitize your key!", e);
            return null;
        }

        if (key == null) {
            log.error("null value for key passed to ciget");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);
            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "ciget " + sid + " " + key;

        Long lease_token = new Long(0L);

        // get pending lease of this key if existed
        if (this.q_lease_list.containsKey(key)) {
            lease_token = this.q_lease_list.get(key);
        } else if (this.i_lease_list.containsKey(key)) {
            lease_token = this.i_lease_list.get(key);
        }

        cmd += " " + lease_token.longValue();
        cmd += "\r\n";

        boolean value_found = false;

        // resulted object
        Object o = null;

        int backoff = INITIAL_BACKOFF_VALUE;
        
        long t = 0;

        // Keep trying to get until either the value is found or a valid
        // lease_token is returned.
        while (!value_found) {
            socket = getSocket(key, hashCode);

            try {
                if (log.isDebugEnabled())
                    log.debug("++++ memcache regci command: " + cmd);
                
                t = System.nanoTime();

                socket.write(cmd.getBytes());
                socket.flush();

                incrementCounter(numIQGet);

                while (true) {
                    String line = socket.readLine();

                    if (log.isDebugEnabled())
                        log.debug("++++ line: " + line);

                    if (line.startsWith(VALUE)) {
                        String[] info = line.split(" ");
                        int flag = Integer.parseInt(info[2]);
                        int length = Integer.parseInt(info[3]);

                        if (log.isDebugEnabled()) {
                            log.debug("++++ key: " + key);
                            log.debug("++++ flags: " + flag);
                            log.debug("++++ length: " + length);
                        }

                        // read obj into buffer
                        byte[] buf = new byte[length];
                        socket.read(buf);
                        socket.clearEOL();

                        value_found = true;

                        if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                            try {
                                // read the input stream, and write to a byte
                                // array output stream since
                                // we have to read into a byte array, but we
                                // don't know how large it
                                // will need to be, and we don't want to resize
                                // it a bunch
                                GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                                ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                                int count;
                                byte[] tmp = new byte[2048];
                                while ((count = gzi.read(tmp)) != -1) {
                                    bos.write(tmp, 0, count);
                                }

                                // store uncompressed back to buffer
                                buf = bos.toByteArray();
                                gzi.close();
                            } catch (IOException e) {

                                // if we have an errorHandler, use its hook
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                log.error("++++ IOException thrown while trying to uncompress input stream for key: "
                                        + key + " -- " + e.getMessage());
                                throw new NestedIOException(
                                        "++++ IOException thrown while trying to uncompress input stream for key: "
                                                + key,
                                                e);
                            }
                        }

                        // we can only take out serialized objects
                        if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                            if (primitiveAsString || asString) {
                                // pulling out string value
                                if (log.isInfoEnabled())
                                    log.info("++++ retrieving object and stuffing into a string.");
                                o = new String(buf, defaultEncoding);
                            } else {
                                // decoding object
                                try {
                                    o = NativeHandler.decode(buf, flag);
                                } catch (Exception e) {

                                    // if we have an errorHandler, use its hook
                                    if (errorHandler != null)
                                        errorHandler.handleErrorOnGet(this, e, key);

                                    log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                                    throw new NestedIOException(e);
                                }
                            }
                        } else {
                            // deserialize if the data is serialized
                            ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                                    classLoader);
                            try {
                                o = ois.readObject();
                                if (log.isInfoEnabled())
                                    log.info("++++ deserializing " + o.getClass());
                            } catch (Exception e) {
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                o = null;
                                log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                        + e.getMessage());
                                throw new IOException("Exception thrown while trying to deserialize for key: " + key);
                            } finally {
                                if (ois != null) {
                                    ois.close();
                                }
                            }
                        }
                    } else if (line.startsWith(LEASEVALUE)) {
                        String[] info = line.split(" ");
                        long token_value = Long.parseLong(info[3]);

                        if (!this.i_lease_list.containsKey(key)) {
                            this.i_lease_list.put(key, token_value);
                            incrementCounter(numILeaseGranted);
                        }
                        value_found = true;
                    } else if (line.startsWith(NOVALUE)) {
                        value_found = true;
                    } else if (line.startsWith(RETRY)) {
//                        System.out.println("ciget Retry "+key);
                        value_found = false;

                        if (backoff == 0) {
                            throw new COException("The server asks for retry", key);
                        } else
                            break;
                    } else if (line.startsWith(ABORT)) {
                        value_found = false;

                        throw new COException(String.format("ciget Session aborted key=%s", key), key);
                    } else if (END.equals(line)) {
                        if (log.isDebugEnabled())
                            log.debug("++++ finished reading from cache server");

                        break;
                    } else {
                        throw new IOException("iqget: Unexpected return message");
                    }
                }
            } catch (IOException e) {
                System.out.println("IO exception iqget " + key + ": ");
                e.printStackTrace();

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                // exception thrown
                log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                        + e.getMessage());

                throw e;
            } finally {
                if (socket != null) {
                    socket.close();
                    socket = null;
                }
            }

            if (backoff == 0) {
                break;
            }

            if (!value_found) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        st.add("ciget", System.nanoTime() - t);

        if (value_found) {
            return o;
        }

        return null;

    }

    public Object oqRead(String sid, String key) throws COException {
        return oqRead(sid, key, null, false);
    }

    public Object oqRead(String sid, String key, Integer hashCode, boolean asString) throws COException {
        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "oqread " + sid + " " + key + "\r\n";

        boolean value_found = false;
        // ready object
        Object o = null;

        socket = getSocket(key, hashCode);

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache oqread command: " + cmd);

            long t = System.nanoTime();
            
            socket.write(cmd.getBytes());
            socket.flush();

            while (true) {
                String line = socket.readLine();

                if (log.isDebugEnabled())
                    log.debug("++++ line: " + line);

                if (line.startsWith(VALUE)) {
                    // quarantine was successful and value was found.
                    String[] info = line.split(" ");
                    int flag = Integer.parseInt(info[2]);
                    int length = Integer.parseInt(info[3]);

                    // Handle the value
                    // read obj into buffer
                    byte[] buf = new byte[length];
                    socket.read(buf);
                    socket.clearEOL();

                    value_found = true;

                    if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                        try {
                            // read the input stream, and write to a byte array
                            // output stream since
                            // we have to read into a byte array, but we don't
                            // know how large it
                            // will need to be, and we don't want to resize it a
                            // bunch
                            GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                            ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                            int count;
                            byte[] tmp = new byte[2048];
                            while ((count = gzi.read(tmp)) != -1) {
                                bos.write(tmp, 0, count);
                            }

                            // store uncompressed back to buffer
                            buf = bos.toByteArray();
                            gzi.close();
                        } catch (IOException e) {

                            // if we have an errorHandler, use its hook
                            if (errorHandler != null)
                                errorHandler.handleErrorOnGet(this, e, key);

                            log.error("++++ IOException thrown while trying to uncompress input stream for key: " + key
                                    + " -- " + e.getMessage());
                            throw new NestedIOException(
                                    "++++ IOException thrown while trying to uncompress input stream for key: " + key,
                                    e);
                        }
                    }

                    // we can only take out serialized objects
                    if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                        if (primitiveAsString || asString) {
                            // pulling out string value
                            if (log.isInfoEnabled())
                                log.info("++++ retrieving object and stuffing into a string.");
                            o = new String(buf, defaultEncoding);
                        } else {
                            // decoding object
                            try {
                                o = NativeHandler.decode(buf, flag);
                            } catch (Exception e) {

                                // if we have an errorHandler, use its hook
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                                throw new NestedIOException(e);
                            }
                        }
                    } else {
                        // deserialize if the data is serialized
                        ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                                classLoader);
                        try {
                            o = ois.readObject();
                            if (log.isInfoEnabled())
                                log.info("++++ deserializing " + o.getClass());
                        } catch (Exception e) {
                            if (errorHandler != null)
                                errorHandler.handleErrorOnGet(this, e, key);

                            o = null;
                            log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                    + e.getMessage());
                        } finally {
                            if (ois != null) {
                                ois.close();
                            }
                        }
                    }
                } else if (line.startsWith(NOVALUE)) {
                    // Quarantine was successful and QLease granted but value
                    // was not found.
                    value_found = false;
                } else if (line.startsWith(INVALID)) {
                    // Quarantine was not successful. Throw IQException to
                    // signify this.
                    socket.close();
                    socket = null;
                    throw new COException("Failed OQRead to quarantine key: " + key, key);
                } else if (line.startsWith(ABORT)) {
                    socket.close();
                    socket = null;
                    throw new COException("Session was aborted.", key);
                } else if (END.equals(line)) {
                    if (log.isDebugEnabled())
                        log.debug("++++ finished reading from cache server");
                    break;
                }
            }
            
            st.add("oqread", System.nanoTime() - t);

            socket.close();
            socket = null;
        } catch (IOException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                    + e.getMessage());

            try {
                socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
            }
            socket = null;
        }

        if (value_found) {
            return o;
        }

        return null;
    }
    
    public boolean ewAdd(String sid, String key, Object value, Integer hashCode, boolean asString, boolean pimem) throws IOException, COException {
        return oqAdd(sid, key, value, null, hashCode, asString, pimem);
    }

    public boolean oqAdd(String sid, String key, Object value, Integer hashCode, boolean asString) throws IOException, COException {
        return oqAdd(sid, key, value, null, hashCode, asString, false);
    }

    public boolean oqAdd(String sid, String key, Object value) throws IOException, COException {
        return oqAdd(sid, key, value, null, null, false, false);
    }

    private boolean oqAdd(String sid, String key, Object value, 
            Date expiry, Integer hashCode, boolean asString, boolean isPimem)
            throws IOException, COException {
        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {
            flags |= NativeHandler.getMarkerFlag(value);
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        // now write the data to the cache server
        try {
            String cmd = null;

            if (!isPimem) {
                cmd = String.format("%s %s %s %d %d %d\r\n", "oqadd", 
                        sid, key, flags, (expiry.getTime() / 1000), val.length);
            } else {
                cmd = String.format("%s %s %s %d %d %d %d\r\n", "oqadd", 
                        sid, key, flags, (expiry.getTime() / 1000), val.length, 1);
            }

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully added for key: " + key);
            } else if (NOTSTORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not added for key: " + key);
                return false;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);
                throw new COException("oqAdd session was aborted.", key);
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
                throw new COException("oqAdd session was aborted.", key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }

            return true;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    public boolean oqWrite(String sid, String key, Integer hashCode, Object value, boolean asString) throws IOException, COException {
        return oqWrite(sid, key, value, null, hashCode, asString);
    }

    public boolean oqWrite(String sid, String key, Object value) throws IOException, COException {
        return oqWrite(sid, key, value, null, null, false);
    }

    private boolean oqWrite(String sid, String key, Object value, Date expiry, Integer hashCode, boolean asString)
            throws IOException, COException {
        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {

            if (asString) {
            } else {
                flags |= NativeHandler.getMarkerFlag(value);
            }
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        // now write the data to the cache server
        try {
            String cmd = null;

            cmd = String.format("%s %s %s %d %d %d\r\n", "oqwrite", sid, key, flags, (expiry.getTime() / 1000),
                    val.length);
            
            long t = System.nanoTime();

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);
                throw new COException("oqswap session was aborted.", key);
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
                throw new COException("oqwrite session was aborted.", key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }
            
            st.add("oqwrite", System.nanoTime() - t);

            return true;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    public boolean oqSwap(String sid, String key, Integer hashCode, Object value, boolean asString) throws IOException, COException {
        return oqSwap(sid, key, value, null, hashCode, asString);
    }

    public boolean oqSwap(String sid, String key, Integer hashCode, Object value) throws IOException, COException {
        return oqSwap(sid, key, value, null, hashCode, false);
    }

    public boolean oqSwap(String sid, String key, Object value) throws IOException, COException {
        return oqSwap(sid, key, value, null, null, false);
    }

    public SockIOPool getPool() {
        return pool;
    }

    private boolean oqSwap(String sid, String key, Object value, Date expiry, Integer hashCode, boolean asString)
            throws IOException, COException {
        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {
            flags |= NativeHandler.getMarkerFlag(value);
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        // now write the data to the cache server
        try {
            String cmd = null;

            cmd = String.format("%s %s %s %d %d %d\r\n", "oqswap", sid, key, flags, (expiry.getTime() / 1000),
                    val.length);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);
                throw new COException("oqswap session was aborted.", key);
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
                throw new COException("oqswap session was aborted.", key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }

            return true;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }
    
    public boolean validate(String sid) throws COException {
        return validate(sid, 0);
    }

    public boolean validate(String sid, Integer hashCode) throws COException {
        if (sid == null || sid.equals("")) {
            log.error("null value fr sid passed to validate");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, hashCode);

        String command = "validate " + sid + "\r\n";

        try {
            sock.write(command.getBytes());
            sock.flush();

            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ validate of session id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ validate of session id: " + sid + " from cache was a abort");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                throw new COException("validate Session aborted.", sid);
            } else {
                log.error("++++ error validate sess: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                return false;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        return false;
    }

    public boolean unleaseCO(String sid) throws COException {
        if (sid == null || sid.equals("")) {
            log.error("null value for sid passed to unleaseCO");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, null);

        // build command
        StringBuilder command;

        command = new StringBuilder("counlease ").append(sid + " ");
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ unleaseCO of session id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ unleaseCO of session id: " + sid + " from cache was a abort");

                // return sock to pool and bail here
                sock.close();
                sock = null;

                throw new COException("unleaseCO Session aborted.", sid);
            } else {
                log.error("++++ error unleaseCO sess: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                return false;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        return false;
    }

    public boolean oqReg(String sid, String key) throws COException {
        return oqReg(sid, key, null);
    }

    public boolean oqReg(String sid, String key, Integer hashCode) throws COException {
        if (sid == null) {
            log.error("null value for sid passed to oqReg");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        if (key == null) {
            log.error("null value for key passed to oqReg");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, hashCode);

        // build command
        StringBuilder command = new StringBuilder("oqreg ").append(sid + " ").append(key);
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ oqReg of session id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ oqReg of session id: " + sid + " from cache was a abort");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                throw new COException("oqreg aborted.", key);
            } else {
                log.error("++++ error oqReg sess: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                return false;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        return false;

    }
    
    public boolean dCommit(String sid, Set<Integer> hashCodes) {
        removeRedundantHashCodes(hashCodes);
        
        long t = System.nanoTime();
        
        boolean success = true;
        for (Integer hc: hashCodes) {
            if (!dCommit(sid, hc)) 
                success = false;
        }
        
        st.add("dcommit", System.nanoTime() - t);
        
        return success;
    }
    
    public boolean validate(String sid, Set<Integer> hashCodes) throws COException {
        removeRedundantHashCodes(hashCodes);
        
        boolean success = true;
        for (Integer hc: hashCodes) {
            if (!validate(sid, hc)) {
                success = false;
                break;
            }
        }
        return success;
    }
    
    public boolean dAbort(String sid, Set<Integer> hashCodes) {
        removeRedundantHashCodes(hashCodes);
        
        long t = System.nanoTime();
        
        boolean success = true;
        for (Integer hc: hashCodes) {
            if (!dAbort(sid, hc)) 
                success = false;
        }
        
        st.add("dabort", System.nanoTime() - t);
        
        return success;
    }

    private void removeRedundantHashCodes(Set<Integer> hashCodes) {
        Set<String> checkDuplicatedServer = new HashSet<>();
        Iterator<Integer> iter = hashCodes.iterator();
        while (iter.hasNext()){
            Integer hc = iter.next();
            
            String server = pool.getServer(hc);
            if (!checkDuplicatedServer.contains(server)) {
                checkDuplicatedServer.add(server);
            } else {
                iter.remove();
            }
        }
    }

    public boolean dCommit(String sid) throws COException {
        return dCommit(sid, 0);
    }

    public boolean dCommit(String sid, Integer hashCode) {
        if (sid == null) {
            log.error("null value for sid passed to dCommit");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, hashCode);

        // build command
        StringBuilder command = new StringBuilder("dcommit ").append(sid + " ");

        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dCommit of session id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dCommit of session id: " + sid + " from cache was a abort");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return false;
            } else {
                log.error("++++ error dCommit sess: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                return false;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            cleanup();
        }

        return false;
    }

    private boolean sendDeleteAndRelease(String tid, Integer hashCode) throws Exception {
        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(tid, hashCode);

        // build command
        StringBuilder command = new StringBuilder( "dar " ).append( tid );
        command.append( "\r\n" );			

        try {
            sock.write( command.toString().getBytes() );
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if ( DELETED.equals( line ) ) {
                if ( log.isInfoEnabled() )
                    log.info( "++++ dar of transaction id: " + tid + " from cache was a success" );

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (INVALID.equals( line )) { 
                if ( log.isInfoEnabled() )
                    log.info( "++++ dar of transaction id: " + tid + " from cache found no tid or item" );

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return false;				
            } else if ( NOTFOUND.equals( line ) ) {
                if ( log.isInfoEnabled() )
                    log.info( "++++ dar of key: " + tid + " from cache failed as the key was not found" );
            }
            else {
                log.error( "++++ error dar key: " + tid );
                log.error( "++++ server response: " + line );

                sock.close();
                sock = null;
                throw new Exception("Server error on DaR request (" + tid + " ): " + line + 
                        " \nCommand = " + command);
            }
        }
        catch ( IOException e ) {

            // if we have an errorHandler, use its hook
            if ( errorHandler != null )
                errorHandler.handleErrorOnDelete( this, e, tid );

            // exception thrown
            log.error( "++++ exception thrown while writing bytes to server on delete" );
            log.error( e.getMessage(), e );

            try {
                sock.trueClose();
            }
            catch ( IOException ioe ) {
                log.error( "++++ failed to close socket : " + sock.toString() );
            }

            sock = null;
        }

        if ( sock != null ) {
            sock.close();
            sock = null;
        }

        return false;
    }

    public boolean deleteAndRelease(String tid, Integer hashCode) throws Exception {
        if (tid == null) {
            log.error("null tid");
            return false;
        }

        try {
            tid = sanitizeKey(tid);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if ( errorHandler != null )
                errorHandler.handleErrorOnDelete( this, e, tid );

            log.error( "failed to sanitize your key!", e );
            return false;
        }

        return sendDeleteAndRelease(tid, hashCode);
    }

    public boolean deleteAndRelease(String tid) throws Exception {
        return deleteAndRelease(tid, null);
    }

    public boolean commit(String sid, Integer hashCode) throws Exception {
        if (sid == null) {
            log.error("null value for key passed to deleteAndRelease()");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, hashCode);

        // build command
        StringBuilder command = new StringBuilder("commit ").append(sid);
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ commit of transaction id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of transaction id: " + sid + " from cache found no tid or item");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return false;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of key: " + sid + " from cache failed as the key was not found");
            } else {
                log.error("++++ error commit key: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                throw new Exception("Server error on DaR request (" + sid + " ): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return false;
    }

    public boolean dAbort(String sid, Integer hashCode) {
        if (sid == null) {
            log.error("null value for key passed to deleteAndRelease()");
            return false;
        }

        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(sid, hashCode);

        // build command
        StringBuilder command = new StringBuilder("dabort ").append(sid);
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line) || ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ commit of transaction id: " + sid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of transaction id: " + sid + " from cache found no tid or item");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return false;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of key: " + sid + " from cache failed as the key was not found");
            } else {
                log.error("++++ "+line);
                log.error("++++ error dAbort sess: " + sid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                log.error("++++ Server error on DaR request (" + sid + " ): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            cleanup();
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return false;
    }

    /**
     * Commit for Eventual Persistence
     * 
     * @param tid
     * @param hashCodes
     * @param pending
     * @return
     */
    public boolean ewcommit(String tid, Integer hashCode, 
            boolean pending) {

        if (tid == null) {
            log.error("null value for key passed to deleteAndRelease()");
            return false;
        }

        try {
            tid = sanitizeKey(tid);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, tid);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        //		Set<String> servers = new HashSet<String>();

        //		if (hashCodes != null) {
        //			for (Integer hashCode : hashCodes) {
        String server = pool.getServer(hashCode);
        //				if (!servers.contains(server))
        //					servers.add(server);
        //			}
        //		} else {
        //			for (String server : pool.getServers()) {
        //				servers.add(server);
        //			}
        //		}

        // build command
        int p = pending ? 1 : 0;
        StringBuilder command = new StringBuilder("commit ").append(tid).append(" ").append(p);
        command.append("\r\n");

        boolean success = true;
        //		for (String server : servers) {
        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = pool.getConnection(server);

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ commit of transaction id: " + tid + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of transaction id: " + tid + " from cache found no tid or item");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                success = false;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ dar of key: " + tid + " from cache failed as the key was not found");
                success = false;
            } else {
                log.error("++++ error ewcommit key: " + tid);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                success = false;
            }
        } catch (IOException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, tid);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
            success = false;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }
        //		}

        // clean Q lease list
        q_lease_list.clear();

        return success;
    }
    
    public boolean deque(String key, int id, int numOfItems) throws Exception {
        return deque(key, id, numOfItems, null); 
    }
    
    public boolean deque(String key, int id, int numOfItems, Integer hashCode) throws Exception {
        if (key == null) {
            log.error("null value for key passed to deque()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        // build command
        StringBuilder command = new StringBuilder("deque ").append(key).append(" ").
                append(id + " ").append(numOfItems);
        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (OK.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deque of key: " + key + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (NOT_PERSISTED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deque of key: " + key + " from cache was an NOT_PERSISTED.");
                
                sock.close();
                sock = null;
                return false;
            } else {
                log.error("++++ error deleting key: " + key);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                throw new Exception(
                        "Server error on Delete request (" + key + "): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }
        
        return false;
    }

    /**
     * Deletes an object from cache given cache key.
     *
     * @param key
     *            the key to be removed
     * @return <code>true</code>, if the data was deleted successfully
     * @throws Exception
     */
    public boolean delete(String key) throws Exception {
        return delete(key, null, null);
    }

    /**
     * Deletes an object from cache given cache key and expiration date.
     * 
     * @param key
     *            the key to be removed
     * @param expiry
     *            when to expire the record.
     * @return <code>true</code>, if the data was deleted successfully
     * @throws Exception
     */
    public boolean delete(String key, Date expiry) throws Exception {
        return delete(key, null, expiry);
    }

    /**
     * Deletes an object from cache given cache key, a delete time, and an
     * optional hashcode.
     *
     * The item is immediately made non retrievable.<br/>
     * Keep in mind {@link #add(String, Object) add} and
     * {@link #replace(String, Object) replace}<br/>
     * will fail when used with the same key will fail, until the server reaches
     * the<br/>
     * specified time. However, {@link #iqset(String, Object) set} will succeed,
     * <br/>
     * and the new value will not be deleted.
     *
     * @param key
     *            the key to be removed
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @param expiry
     *            when to expire the record.
     * @return <code>true</code>, if the data was deleted successfully
     * @throws Exception
     */
    public boolean delete(String key, Integer hashCode, Date expiry) throws Exception {

        if (key == null) {
            log.error("null value for key passed to delete()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        // build command
        StringBuilder command = new StringBuilder("delete ").append(key);
        if (expiry != null)
            command.append(" " + expiry.getTime() / 1000);

        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (DELETED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deletion of key: " + key + " from cache was a success");

                // return sock to pool and bail here
                sock.close();
                sock = null;
                return true;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deletion of key: " + key + " from cache failed as the key was not found");
            } else {
                log.error("++++ error deleting key: " + key);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
                throw new Exception(
                        "Server error on Delete request (" + key + "): " + line + " \nCommand = " + command);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return false;
    }

    /***
     * Read the key and attempt to Quarantine it. The Quarantine may fail if
     * another thread/client holds a Q-lease on the key. In this case, an
     * IQException is thrown. If the value for this key exists in the cache, it
     * is returned. If not, null is returned. A null value indicates that the
     * Quarantine was successful.
     * 
     * @param key
     *            Key of the key-value pair to lookup in the cache.
     * @return The value if it exists in the cache. If not, returns null.
     * @throws IQException
     * @throws Exception
     */
    public Object quarantineAndRead(String key) throws IQException, Exception {
        return quarantineAndRead(key, null, false);
    }

    public void releaseAllLeases() throws IQException {
        if (true)
            return;

        // Failed to quarantine the key. Handle by freeing up any acquired
        // leases.
        for (String cleanup_key : q_lease_list.keySet()) {
            if (q_lease_list.get(cleanup_key) == null) {
                System.out.println("Error, lease token missing for " + cleanup_key);
            }

            if (!releaseX(cleanup_key, q_lease_list.get(cleanup_key))) {
                throw new IQException("Error releasing xLease during cleanup (after failed xLease)");
            }
        }

        // Reset the hold_list to empty
        q_lease_list = new ConcurrentHashMap<>();

        for (String k : i_lease_list.keySet()) {
            if (i_lease_list.get(k) == null) {
                System.out.println("Error, lease token missing for " + k);
            }

            if (!releaseX(k, i_lease_list.get(k))) {
                throw new IQException("Error releasing Lease during cleanup (after failed xLease)");
            } else {
                incrementCounter(numILeaseRelease);
            }
        }

        i_lease_list = new ConcurrentHashMap<String, Long>();
    }

    public void releaseILeases() {
        Set<String> keySet = new HashSet<String>();
        for (String key : i_lease_list.keySet()) {
            keySet.add(key);
        }

        for (String key : keySet) {
            if (i_lease_list.get(key) == null) {
                System.out.println("Error, lease token missing for " + key);
            }

            if (releaseX(key, i_lease_list.get(key))) {
                incrementCounter(numILeaseRelease);
            }
        }

        i_lease_list.clear();
    }

    public void releaseILeases(Integer hashCode) {
        Set<String> keySet = new HashSet<String>();
        for (String key : i_lease_list.keySet()) {
            keySet.add(key);
        }

        for (String key : keySet) {
            if (i_lease_list.get(key) == null) {
                System.out.println("Error, lease token missing for " + key);
            }

            if (releaseX(key, i_lease_list.get(key), hashCode)) {
                incrementCounter(numILeaseRelease);
            }
        }

        i_lease_list.clear();
    }

    public Object quarantineAndRead(String key, Integer hashCode, boolean asString) throws IQException {
        CLValue result = null;

        try {
            // This function is just a wrapper to handle the error case when
            // a Quarantine request fails. readAndQuarantineMain performs the
            // actual
            // logic.
            result = quarantineAndReadMain(null, key, hashCode, asString);
        } catch (IQException e) {
            releaseAllLeases();
            throw e;
        }

        return result.getValue();
    }

    /**
     * Do a QaRead for Eventual Persitence
     * 
     * @param tid
     * @param key
     * @param hashCode
     * @param asString
     * @return
     * @throws IQException
     */
    public CLValue ewread(String tid, String key, Integer hashCode, boolean asString) throws IQException {
        return quarantineAndReadMain(tid, key, hashCode, asString);
    }

    public boolean ewswap(String tid, String key, Object value) throws IQException {
        boolean ret = false;
        try {
            ret = swap(key, null, value);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public boolean ewswap(String tid, String key, Integer hashCode, Object value, boolean isPiMem) throws IQException {
        boolean ret = false;
        try {
            ret = swap(key, hashCode, value, isPiMem);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public boolean ewswap(String tid, String key, Integer hashCode, Object value) throws IQException {
        boolean ret = false;
        try {
            ret = swap(key, hashCode, value);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public boolean swap(String key, Integer hashCode, Object value) throws IOException, IQException {
        return swap(key, hashCode, value, false);
    }

    public boolean swap(String key, Integer hashCode, Object value, boolean isPiMem) throws IOException, IQException {
        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        Date expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, primitiveAsString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {

            if (primitiveAsString) {
            } else {
                flags |= NativeHandler.getMarkerFlag(value);
            }
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        Long current_token = null;

        // now write the data to the cache server
        try {
            String cmd = null;

            current_token = this.q_lease_list.get(key);

            if (current_token == null)
                throw new IQException("swap request with no lease token");

            int pi_mem = isPiMem ? 1 : 0;
            cmd = String.format("%s %s %d %d %d %d %d\r\n", "swap", key, flags, (expiry.getTime() / 1000), 
                    val.length, current_token, pi_mem);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
            } else if (NOTSTORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }

            return true;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    public Object quarantineAndRead(String tid, String key, Integer hashCode, boolean asString) throws IQException {
        Object result = null;

        try {
            // This function is just a wrapper to handle the error case when
            // a Quarantine request fails. readAndQuarantineMain performs the
            // actual
            // logic.
            result = quarantineAndReadMain(tid, key, hashCode, asString);
        } catch (IQException e) {
            releaseAllLeases();
            throw e;
        }

        return result;
    }

    private CLValue quarantineAndReadMain(String tid, String key, Integer hashCode, boolean asString)
            throws IQException {
        int pending = 0;

        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "qaread " + key;

        // get the lease token of this key granted for this client if it does
        // exist
        Long lease_token = new Long(0L);

        if (this.q_lease_list.containsKey(key)) {
            lease_token = this.q_lease_list.get(key);
        } else if (this.i_lease_list.containsKey(key)) {
            lease_token = this.i_lease_list.get(key);
        }

        // append lease token to the command
        if (lease_token.longValue() != 0L) {
            cmd += " " + lease_token.longValue();
        } else {
            cmd += " 0";
        }

        if (tid != null) {
            cmd += " " + tid;
        }

        cmd += "\r\n";

        boolean value_found = false;
        // ready object
        Object o = null;

        socket = getSocket(key, hashCode);

        boolean hot_miss = false;

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache qaread command: " + cmd);

            socket.write(cmd.getBytes());
            socket.flush();

            while (true) {
                String line = socket.readLine();
                long token_value = 0;

                if (log.isDebugEnabled())
                    log.debug("++++ line: " + line);

                if (line.startsWith(LEASE) || line.startsWith(VALUE)) {
                    // Quarantine was successful and value was found.
                    String[] info = line.split(" ");

                    int flag = Integer.parseInt(info[2]);
                    pending = Integer.parseInt(info[3]);

                    if (line.startsWith(LEASE)) {
                        token_value = Long.parseLong(info[4]);
                    }

                    int length = Integer.parseInt(info[5]);

                    if (log.isDebugEnabled()) {
                        log.debug("++++ key: " + key);
                        log.debug("++++ flags: " + flag);
                        log.debug("++++ length: " + length);
                    }

                    // Handle the QLease token_value
                    if (line.startsWith(LEASE)) {
                        if (token_value > TOKEN_HOTMISS) {
                            this.q_lease_list.put(key, token_value);
                        } else {
                            socket.close();
                            socket = null;
                            throw new IQException(
                                    "Invalid token value (" + token_value + ") observed for " + "QaRead of key:" + key);
                        }
                    }

                    // Handle the value
                    // read obj into buffer
                    byte[] buf = new byte[length];
                    socket.read(buf);
                    socket.clearEOL();

                    value_found = true;

                    if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                        try {
                            // read the input stream, and write to a byte array
                            // output stream since
                            // we have to read into a byte array, but we don't
                            // know how large it
                            // will need to be, and we don't want to resize it a
                            // bunch
                            GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                            ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                            int count;
                            byte[] tmp = new byte[2048];
                            while ((count = gzi.read(tmp)) != -1) {
                                bos.write(tmp, 0, count);
                            }

                            // store uncompressed back to buffer
                            buf = bos.toByteArray();
                            gzi.close();
                        } catch (IOException e) {

                            // if we have an errorHandler, use its hook
                            if (errorHandler != null)
                                errorHandler.handleErrorOnGet(this, e, key);

                            log.error("++++ IOException thrown while trying to uncompress input stream for key: " + key
                                    + " -- " + e.getMessage());
                            throw new NestedIOException(
                                    "++++ IOException thrown while trying to uncompress input stream for key: " + key,
                                    e);
                        }
                    }

                    // we can only take out serialized objects
                    if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                        if (primitiveAsString || asString) {
                            // pulling out string value
                            if (log.isInfoEnabled())
                                log.info("++++ retrieving object and stuffing into a string.");
                            o = new String(buf, defaultEncoding);
                        } else {
                            // decoding object
                            try {
                                o = NativeHandler.decode(buf, flag);
                            } catch (Exception e) {

                                // if we have an errorHandler, use its hook
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                                throw new NestedIOException(e);
                            }
                        }
                    } else {
                        // deserialize if the data is serialized
                        ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                                classLoader);
                        try {
                            o = ois.readObject();
                            if (log.isInfoEnabled())
                                log.info("++++ deserializing " + o.getClass());
                        } catch (Exception e) {
                            if (errorHandler != null)
                                errorHandler.handleErrorOnGet(this, e, key);

                            o = null;
                            log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                    + e.getMessage());
                        } finally {
                            if (ois != null) {
                                ois.close();
                            }
                        }
                    }
                } else if (line.startsWith(LEASEVALUE) || line.startsWith(NOVALUE)) {
                    // Quarantine was successful and QLease granted but value
                    // was not found.
                    String[] info = line.split(" ");
                    String currkey = info[1];

                    pending = Integer.parseInt(info[3]);

                    if (line.startsWith(LEASEVALUE))
                        token_value = Long.parseLong(info[4]);

                    value_found = false;

                    if (line.startsWith(LEASEVALUE)) {
                        if (token_value == TOKEN_HOTMISS) {
                            hot_miss = true;
                        } else {
                            this.q_lease_list.put(currkey, token_value);
                        }
                    }
                } else if (line.startsWith(INVALID)) {
                    // Quarantine was not successful. Throw IQException to
                    // signify this.
                    socket.close();
                    socket = null;
                    throw new IQException("Failed QaRead to quarantine key: " + key);
                } else if (END.equals(line)) {
                    if (log.isDebugEnabled())
                        log.debug("++++ finished reading from cache server");
                    break;
                }

            }

            socket.close();
            socket = null;

        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                    + e.getMessage());

            try {
                socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
            }
            socket = null;
        }

        if (hot_miss)
            throw new IQException("Failed QaRead to quarantine key: " + key);

        boolean isPending = pending == 0 ? false : true;
        return new CLValue(o, isPending);
    }

    public void swapAndRelease(String key, Object value) throws IOException {
        try {
            swapAndRelease(key, value, null, null, primitiveAsString);
        } catch (IQException e) {
            e.getMessage();
        }

        return;
    }

    public void swapAndRelease(String key, Integer hashCode, Object value) throws IOException {
        try {
            swapAndRelease(key, value, null, hashCode, primitiveAsString);
        } catch (IQException e) {
            e.getMessage();
        }

        return;
    }

    public static AtomicInteger GET_SOCK_FAILED = new AtomicInteger(0);

    private SockIOPool.SockIO getSocket(String key, Integer hashCode) {
        SockIOPool.SockIO sock = null;

        while (true) {
            // get SockIO obj from hash or from key
            sock = pool.getSock(key, hashCode);

            // return false if unable to get SockIO obj
            if (sock == null) {
                GET_SOCK_FAILED.incrementAndGet();
                if (errorHandler != null)
                    errorHandler.handleErrorOnDelete(this, new IOException("no socket to server available"), key);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            break;
        }

        return sock;
    }

    public boolean releaseX(String key, Integer hashCode) throws Exception {
        String skey = null;
        try {
            skey = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            return false;
        }

        Long lease_token = i_lease_list.get(skey);
        if (lease_token == null) {
            return false;
        }

        boolean success = releaseX(key, lease_token, hashCode);

        // TODO: do a different action if it succeeds or fails?
        // Remove entry from lease_list
        i_lease_list.remove(key);
        return success;
    }

    private boolean releaseX(String key, Long lease_token) {
        return releaseX(key, lease_token, null);
    }

    /***
     * Release an xLease on a key.
     * 
     * @param key
     * @param lease_token
     * @param hashcode
     * @return
     * @throws IQException
     * @throws IOException
     */
    private boolean releaseX(String key, Long lease_token, Integer hashCode) {
        if (key == null || lease_token == null) {
            log.error("null value for key passed to delete()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj from hash or from key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        // build command
        boolean callSuccess = false;
        StringBuilder command = new StringBuilder("unlease ").append(key);
        command.append(" " + lease_token.toString());

        command.append("\r\n");

        try {
            sock.write(command.toString().getBytes());
            sock.flush();

            if (i_lease_list.remove(key) != null) {
                incrementCounter(numIUnlease);
            }

            // if we get appropriate response back, then we return true
            String line = sock.readLine();
            if (DELETED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deletion of key: " + key + " from cache was a success");

                callSuccess = true;
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ deletion of key: " + key + " from cache failed as the key was not found");

                // It's ok if the key was not found. That means there was no
                // lease on this key.
                callSuccess = true;
            } else {
                log.error("++++ error deleting key: " + key);
                log.error("++++ server response: " + line);

                sock.close();
                sock = null;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on delete");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return callSuccess;
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public CASResponse cas(String key, Object value, long casToken) throws IOException {
        return casResponse(
                set("cas", key, value, null, null, primitiveAsString, 
                        Optional.of(casToken), Optional.empty(), false));
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public CASResponse ccas(String key, Object value, long casToken, long cost) throws IOException {
        return casResponse(
                set("ccas", key, value, null, null, primitiveAsString, 
                        Optional.of(casToken), Optional.of(cost), false));
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public CASResponse cas(String key, Object value, long casToken, Integer hashCode) throws IOException {
        return casResponse(
                set("cas", key, value, null, hashCode, primitiveAsString, 
                        Optional.of(casToken), Optional.empty(), false));
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     */
    public CASResponse cas(String key, Object value, Date expiry, long casToken) {
        try {
            return casResponse(
                    set("cas", key, value, expiry, null, primitiveAsString, 
                            Optional.of(casToken), Optional.empty(), false));
        } catch (Exception e) {
        }

        return CASResponse.ERROR;
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     */
    public CASResponse ccas(String key, Object value, Date expiry, long casToken, long cost) {
        try {
            return casResponse(
                    set("ccas", key, value, expiry, null, primitiveAsString, 
                            Optional.of(casToken), Optional.of(cost), false));
        } catch (Exception e) {
        }

        return CASResponse.ERROR;
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public CASResponse cas(String key, Object value, Date expiry, long casToken, Integer hashCode) throws IOException {
        return casResponse(
                set("cas", key, value, expiry, hashCode, primitiveAsString, 
                        Optional.of(casToken), Optional.empty(), false));
    }

    private CASResponse casResponse(Optional<String> response) {
        if (!response.isPresent()) {
            return CASResponse.ERROR;
        }
        if (EXISTS.equals(response.get())) {
            return CASResponse.INVALID_CAS_TOKEN;
        }
        if (STORED.equals(response.get())) {
            return CASResponse.SUCCESS;
        }
        if (NOTFOUND.equals(response.get())) {
            return CASResponse.NOT_FOUND;
        }
        return CASResponse.ERROR;
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean set(String key, Object value) throws IOException {
        return setResponse(set("set", key, value, null, null, primitiveAsString, 
                Optional.empty(), Optional.empty(), false));
    }
    
    public boolean set(String key, Object value, boolean asString) throws IOException {
        return setResponse(set("set", key, value, null, null, asString, 
                Optional.empty(), Optional.empty(), false));
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean cset(String key, Integer hashCode, Object value, long cost) throws IOException {
        return setResponse(set("cset", key, value, null, hashCode, primitiveAsString, 
                Optional.empty(), Optional.of(cost), false));
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean set(String key, Object value, Integer hashCode) throws IOException {
        return setResponse(
                set("set", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    public boolean set(String key, Object value, Integer hashCode, 
            boolean isPiMem) throws IOException {
        return setResponse(
                set("set", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), isPiMem));
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean set(String key, Object value, Date expiry) throws IOException {
        try {
            return setResponse(
                    set("set", key, value, expiry, null, primitiveAsString, 
                            Optional.empty(), Optional.empty(), false));
        } catch (Exception e) {
        }

        return false;
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean set(String key, Object value, Date expiry, Integer hashCode) throws IOException {
        return setResponse(
                set("set", key, value, expiry, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    public boolean append(String key, Integer hashCode, Object value, boolean pimem) throws IOException {
        return setResponse(
                set("append", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), pimem)); 
    }

    public boolean append(String key, Integer hashCode, Object value) throws IOException {
        return setResponse(
                set("append", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false)); 
    }

    public boolean append(String key, Object value) throws IOException {
        return setResponse(
                set("append", key, value, null, null, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    public boolean cappend(String key, Object value, long cost) throws IOException {
        return setResponse(
                set("cappend", key, value, null, null, primitiveAsString, 
                        Optional.empty(), Optional.of(cost), false));
    }

    public boolean prepend(String key, Object value) throws IOException {
        return setResponse(
                set("prepend", key, value, null, null, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    public List<String> check(List<String> keys, Integer hashCode) throws IOException {
        List<String> res = new LinkedList<String>();
        if (keys == null || keys.size() == 0) {
            log.error("key is null for get()");
            return res;
        }

        int i = 0;
        while (i < keys.size()) {
            try {
                String key = sanitizeKey(keys.remove(0));
                keys.add(key);
            } catch (UnsupportedEncodingException e) {
                log.error("failed to sanitize your key!", e);
                return null;
            }
            i++;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "check " + keys.size() + " ";
        for (String key : keys) {
            cmd += key + " ";
        }
        cmd += "\r\n";

        socket = getSocket(null, hashCode);

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache iqget command: " + cmd);

            socket.write(cmd.getBytes());
            socket.flush();

            String line = socket.readLine();

            if (log.isDebugEnabled())
                log.debug("++++ line: " + line);

            if (line.startsWith(OK)) {
                String[] info = line.split(" ");

                for (i = 1; i < info.length; i++) {
                    res.add(info[i]);
                }
            } else {
                throw new IOException("iqget: Unexpected return message");
            }

            socket.close();
            socket = null;

        } catch (IOException e) {
            System.out.println("IO exception checks " + keys + ": ");
            e.printStackTrace();

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + keys + " -- "
                    + e.getMessage());

            try {
                if (socket != null)
                    socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
                throw ioe;
            }

            socket = null;

            throw e;
        }

        return res;
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     * @throws IQException
     */
    public boolean iqset(String key, Object value) throws IOException, IQException {
        return iqset(key, value, null, null, primitiveAsString);
    }

    /**
     * Stores data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     * @throws IQException
     */
    public boolean iqset(String key, Object value, Integer hashCode) throws IOException, IQException {
        return iqset(key, value, null, hashCode, primitiveAsString);
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     * @throws IOException
     * @throws IQException
     */
    public boolean iqset(String key, Object value, Date expiry) throws IOException, IQException {
        return setResponse(
                set("iqset", key, value, expiry, null, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Stores data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     * @throws IQException
     */
    public boolean iqset(String key, Object value, Date expiry, Integer hashCode) throws IOException, IQException {
        return setResponse(
                set("iqset", key, value, expiry, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Adds data to the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean add(String key, Object value) throws IOException {
        return setResponse(set("add", key, value, null, null, primitiveAsString, 
                Optional.empty(), Optional.empty(), false));
    }

    /**
     * Adds data to the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean cadd(String key, Object value, long cost) throws IOException {
        return setResponse(set("cadd", key, value, null, null, primitiveAsString, 
                Optional.empty(), Optional.of(cost), false));
    }

    /**
     * Adds data to the server; the key, value, and an optional hashcode are
     * passed in.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean add(String key, Object value, Integer hashCode) throws IOException {
        return setResponse(
                set("add", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Adds data to the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean add(String key, Object value, Date expiry) throws IOException {
        return setResponse(set("add", key, value, expiry, null, primitiveAsString, 
                Optional.empty(), Optional.empty(), false));
    }

    /**
     * Adds data to the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean add(String key, Object value, Date expiry, Integer hashCode) throws IOException {
        return setResponse(
                set("add", key, value, expiry, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Updates data on the server; only the key and the value are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean replace(String key, Object value) throws IOException {
        return setResponse(
                set("replace", key, value, null, null, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Updates data on the server; only the key and the value and an optional
     * hash are specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean replace(String key, Object value, Integer hashCode) throws IOException {
        return setResponse(
                set("replace", key, value, null, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Updates data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean replace(String key, Object value, Date expiry) throws IOException {
        return setResponse(
                set("replace", key, value, expiry, null, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    /**
     * Updates data on the server; the key, value, and an expiration time are
     * specified.
     *
     * @param key
     *            key to store data under
     * @param value
     *            value to store
     * @param expiry
     *            when to expire the record
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true, if the data was successfully stored
     * @throws IOException
     */
    public boolean replace(String key, Object value, Date expiry, Integer hashCode) throws IOException {
        return setResponse(
                set("replace", key, value, expiry, hashCode, primitiveAsString, 
                        Optional.empty(), Optional.empty(), false));
    }

    public byte[] serializeObject(Object value, boolean asString) throws IOException {
        byte val[] = null;
        if (NativeHandler.isHandled(value)) {
            if (asString) {
                // useful for sharing data between java and non-java
                // and also for storing ints for the increment method
                try {
                    val = value.toString().getBytes(defaultEncoding);
                } catch (UnsupportedEncodingException ue) {
                    log.error("invalid encoding type used: " + defaultEncoding, ue);
                    return null;
                }
            } else {
                try {
                    if (log.isInfoEnabled())
                        log.info("Storing with native handler...");
                    // flags |= NativeHandler.getMarkerFlag( value );
                    val = NativeHandler.encode(value);
                } catch (Exception e) {
                    log.error("Failed to native handle obj", e);
                    return null;
                }
            }
        } else {
            // always serialize for non-primitive types
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                (new ObjectOutputStream(bos)).writeObject(value);
                val = bos.toByteArray();
                // flags |= F_SERIALIZED;
            } catch (IOException e) {
                // if we fail to serialize, then
                // we bail
                log.error("failed to serialize obj", e);
                log.error(value.toString());

                return null;
            }
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                // flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {
                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        return val;
    }

    private byte[] convertToByteArray(Object value, boolean asString, String key) {
        byte[] val = null;

        // Treat a null value as an empty object.
        if (value == null) {
            return new byte[0];
        }

        if (NativeHandler.isHandled(value)) {
            if (asString) {
                // useful for sharing data between java and non-java
                // and also for storing ints for the increment method
                try {
                    if (log.isInfoEnabled())
                        log.info("++++ storing data as a string for key: " + key + " for class: "
                                + value.getClass().getName());
                    val = value.toString().getBytes(defaultEncoding);
                } catch (UnsupportedEncodingException ue) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnSet(this, ue, key);

                    log.error("invalid encoding type used: " + defaultEncoding, ue);
                    return null;
                }
            } else {
                try {
                    if (log.isInfoEnabled())
                        log.info("Storing with native handler...");
                    // flags |= NativeHandler.getMarkerFlag( value );
                    val = NativeHandler.encode(value);
                } catch (Exception e) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnSet(this, e, key);

                    log.error("Failed to native handle obj", e);
                    return null;
                }
            }
        } else {
            // always serialize for non-primitive types
            try {
                if (log.isInfoEnabled())
                    log.info("++++ serializing for key: " + key + " for class: " + value.getClass().getName());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                (new ObjectOutputStream(bos)).writeObject(value);
                val = bos.toByteArray();
                // flags |= F_SERIALIZED;
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                // if we fail to serialize, then
                // we bail
                log.error("failed to serialize obj", e);
                log.error(value.toString());

                return null;
            }
        }

        return val;
    }

    // prepare value to put into the cache
    private byte[] prepareValue(String key, Object value, boolean asString, Integer flags) throws IOException {
        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            return val;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {

            if (asString) {
            } else {
                flags |= NativeHandler.getMarkerFlag(value);
            }
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        return val;
    }

    /**
     * Stores data to cache.
     *
     * If data does not already exist for this key on the server, or if the key
     * is being<br/>
     * deleted, the specified value will not be stored.<br/>
     * The server will automatically delete the value when the expiration time
     * has been reached.<br/>
     * <br/>
     * If compression is enabled, and the data is longer than the compression
     * threshold<br/>
     * the data will be stored in compressed form.<br/>
     * <br/>
     * As of the current release, all objects stored will use java
     * serialization.
     * 
     * @param cmdname
     *            action to take (set, add, replace)
     * @param key
     *            key to store cache under
     * @param value
     *            object to cache
     * @param expiry
     *            expiration
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @param asString
     *            store this object as a string?
     * @return true/false indicating success
     */
    private Optional<String> set(String cmdname, String key, Object value, Date expiry, Integer hashCode,
            boolean asString, Optional<Long> casToken, Optional<Long> cost, boolean isPiMem) {

        if (cmdname == null || cmdname.trim().equals("") || key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return Optional.empty();
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return Optional.empty();
        }

        if (value == null) {
            log.error("trying to store a null value to cache");
            return Optional.empty();
        }

        // get SockIO obj
        SockIOPool.SockIO sock = pool.getSock(key, hashCode);

        if (sock == null) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, new IOException("no socket to server available"), key);
            return Optional.empty();
        }

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        if (NativeHandler.isHandled(value)) {

            if (asString) {
                // useful for sharing data between java and non-java
                // and also for storing ints for the increment method
                try {
                    if (log.isInfoEnabled())
                        log.info("++++ storing data as a string for key: " + key + " for class: "
                                + value.getClass().getName());
                    flags |= NativeHandler.getMarkerFlag(value);
                    val = value.toString().getBytes(defaultEncoding);
                } catch (UnsupportedEncodingException ue) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnSet(this, ue, key);

                    log.error("invalid encoding type used: " + defaultEncoding, ue);
                    sock.close();
                    sock = null;
                    return Optional.empty();
                }
            } else {
                try {
                    if (log.isInfoEnabled())
                        log.info("Storing with native handler...");
                    flags |= NativeHandler.getMarkerFlag(value);
                    val = NativeHandler.encode(value);
                } catch (Exception e) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnSet(this, e, key);

                    log.error("Failed to native handle obj", e);

                    sock.close();
                    sock = null;
                    return Optional.empty();
                }
            }
        } else {
            // always serialize for non-primitive types
            try {
                if (log.isInfoEnabled())
                    log.info("++++ serializing for key: " + key + " for class: " + value.getClass().getName());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                (new ObjectOutputStream(bos)).writeObject(value);
                val = bos.toByteArray();
                flags |= F_SERIALIZED;
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                // if we fail to serialize, then
                // we bail
                log.error("failed to serialize obj", e);
                log.error(value.toString());

                // return socket to pool and bail
                sock.close();
                sock = null;
                return Optional.empty();
            }
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");
            }
        }

        String response = null;

        // now write the data to the cache server
        try {
            StringBuilder cmdBuilder = new StringBuilder();
            int pimem = 0;
            if (isPiMem) {
                pimem = 1;
            }
            cmdBuilder.append(
                    String.format("%s %s %d %d %d %d", cmdname, key, flags, (expiry.getTime() / 1000), val.length, pimem));
            if (casToken.isPresent()) {
                cmdBuilder.append(" ");
                cmdBuilder.append(casToken.get());
            }
            if (cost.isPresent()) {
                cmdBuilder.append(" ");
                cmdBuilder.append(cost.get());
            }
            cmdBuilder.append("\r\n");

            sock.write(cmdBuilder.toString().getBytes());
            sock.write(val);
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            response = sock.readLine();
            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmdBuilder.toString() + " (" + response + ")");

            if (STORED.equals(response)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully stored for key: " + key);
            } else if (NOTSTORED.equals(response)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not stored in cache for key: " + key);
            } else if (NOTFOUND.equals(response)) {
                if (log.isInfoEnabled())
                    log.info("++++ cas key not found: " + key);
            } else if (EXISTS.equals(response)) {
                if (log.isInfoEnabled())
                    log.info("++++ cas key been modified after the given token: " + key);
            } else {
                log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length);
                log.error("++++ server response: " + response);
                response = null;
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return Optional.ofNullable(response);
    }

    /**
     * Stores data to cache.
     *
     * If data does not already exist for this key on the server, or if the key
     * is being<br/>
     * deleted, the specified value will not be stored.<br/>
     * The server will automatically delete the value when the expiration time
     * has been reached.<br/>
     * <br/>
     * If compression is enabled, and the data is longer than the compression
     * threshold<br/>
     * the data will be stored in compressed form.<br/>
     * <br/>
     * As of the current release, all objects stored will use java
     * serialization.
     * 
     * @param cmdname
     *            action to take (set, add, replace)
     * @param key
     *            key to store cache under
     * @param value
     *            object to cache
     * @param expiry
     *            expiration
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @param asString
     *            store this object as a string?
     * @return true/false indicating success
     * @throws IQException
     */

    private boolean iqset(String key, Object value, Date expiry, Integer hashCode, boolean asString)
            throws IOException, IQException {

        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {

            if (asString) {
            } else {
                flags |= NativeHandler.getMarkerFlag(value);
            }
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        Long current_token = null;

        // now write the data to the cache server
        try {
            String cmd = null;

            current_token = this.i_lease_list.remove(key);

            if (current_token == null) {
                throw new IQException("iqset request with no lease token");
            }

            if (current_token.longValue() == -1) {
                return false; // iqget is iqget same session
            }

            cmd = String.format("%s %s %d %d %d %d\r\n", "iqset", key, flags, (expiry.getTime() / 1000), val.length,
                    current_token);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());

            sock.flush();

            incrementCounter(numIQSet);

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            // Treat this as a regular set.
            if (STORED.equals(line)) {
                incrementCounter(numILeaseRelease);

                if (log.isInfoEnabled())
                    log.info("++++ data successfully stored for key: " + key);

                return true;
            } else if (NOTSTORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not stored in cache for key: " + key);
            } else {
                log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length);
                log.error("++++ server response: " + line);
            }
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    private boolean swapAndRelease(String key, Object value, Date expiry, Integer hashCode, boolean asString)
            throws IOException, IQException {

        if (key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        if (expiry == null)
            expiry = new Date(0);

        // store flags
        int flags = 0;

        // byte array to hold data
        byte[] val;

        val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // Set the flags
        if (NativeHandler.isHandled(value)) {

            if (asString) {
            } else {
                flags |= NativeHandler.getMarkerFlag(value);
            }
        } else {
            // always serialize for non-primitive types
            flags |= F_SERIALIZED;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();
                flags |= F_COMPRESSED;

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        Long current_token = null;

        // now write the data to the cache server
        try {
            String cmd = null;

            current_token = this.q_lease_list.get(key);

            if (current_token == null)
                throw new IQException("sar request with no lease token");

            cmd = String.format("%s %s %d %d %d %d\r\n", "sar", key, flags, (expiry.getTime() / 1000), val.length,
                    current_token);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            this.q_lease_list.remove(key);

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
            } else if (NOTSTORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);
            } else if (INVALID.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }

            return true;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    /**
     * Store a counter to memcached given a key
     * 
     * @param key
     *            cache key
     * @param counter
     *            number to store
     * @return true/false indicating success
     * @throws IOException
     */
    public boolean storeCounter(String key, long counter) throws IOException {
        return setResponse(set("set", key, new Long(counter), null, null, true, 
                Optional.empty(), Optional.empty(), false));
    }

    /**
     * Store a counter to memcached given a key
     * 
     * @param key
     *            cache key
     * @param counter
     *            number to store
     * @return true/false indicating success
     * @throws IOException
     */
    public boolean storeCounter(String key, Long counter) throws IOException {
        return setResponse(set("set", key, counter, null, null, true, Optional.empty(), 
                Optional.empty(), false));
    }

    /**
     * Store a counter to memcached given a key
     * 
     * @param key
     *            cache key
     * @param counter
     *            number to store
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return true/false indicating success
     * @throws IOException
     */
    public boolean storeCounter(String key, Long counter, Integer hashCode) throws IOException {
        return setResponse(set("set", key, counter, null, hashCode, true, 
                Optional.empty(), Optional.empty(), false));
    }

    /**
     * Returns value in counter at given key as long.
     *
     * @param key
     *            cache ket
     * @return counter value or -1 if not found
     */
    public long getCounter(String key) {
        return getCounter(key, null);
    }

    /**
     * Returns value in counter at given key as long.
     *
     * @param key
     *            cache ket
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return counter value or -1 if not found
     */
    public long getCounter(String key, Integer hashCode) {

        if (key == null) {
            log.error("null key for getCounter()");
            return -1;
        }

        long counter = -1;
        try {
            counter = Long.parseLong((String) get(key, hashCode, true));
        } catch (Exception ex) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, ex, key);

            // not found or error getting out
            if (log.isInfoEnabled())
                log.info(String.format("Failed to parse Long value for key: %s", key));
        }

        return counter;
    }

    /**
     * Thread safe way to initialize and increment a counter.
     * 
     * @param key
     *            key where the data is stored
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrIncr(String key) throws IOException {
        return addOrIncr(key, 0, null);
    }

    /**
     * Thread safe way to initialize and increment a counter.
     * 
     * @param key
     *            key where the data is stored
     * @param inc
     *            value to set or increment by
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrIncr(String key, long inc) throws IOException {
        return addOrIncr(key, inc, null);
    }

    /**
     * Thread safe way to initialize and increment a counter.
     * 
     * @param key
     *            key where the data is stored
     * @param inc
     *            value to set or increment by
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrIncr(String key, long inc, Integer hashCode) throws IOException {
        boolean ret = false;
        ret = setResponse(set("add", key, new Long(inc), null, hashCode, true, 
                Optional.empty(), Optional.empty(), false));

        if (ret) {
            return inc;
        } else {
            return incrdecr("incr", key, inc, hashCode);
        }
    }

    /**
     * Thread safe way to initialize and decrement a counter.
     * 
     * @param key
     *            key where the data is stored
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrDecr(String key) throws IOException {
        return addOrDecr(key, 0, null);
    }

    /**
     * Thread safe way to initialize and decrement a counter.
     * 
     * @param key
     *            key where the data is stored
     * @param inc
     *            value to set or increment by
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrDecr(String key, long inc) throws IOException {
        return addOrDecr(key, inc, null);
    }

    /**
     * Thread safe way to initialize and decrement a counter.
     * 
     * @param key
     *            key where the data is stored
     * @param inc
     *            value to set or increment by
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return value of incrementer
     * @throws IOException
     */
    public long addOrDecr(String key, long inc, Integer hashCode) throws IOException {
        boolean ret = false;
        ret = setResponse(set("add", key, new Long(inc), null, hashCode, 
                true, Optional.empty(), Optional.empty(), false));

        if (ret) {
            return inc;
        } else {
            return incrdecr("decr", key, inc, hashCode);
        }
    }

    private boolean setResponse(Optional<String> response) {
        return response.isPresent() && STORED.equals(response.get());
    }

    /**
     * Increment the value at the specified key by 1, and then return it.
     *
     * @param key
     *            key where the data is stored
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long incr(String key) {
        return incrdecr("incr", key, 1, null);
    }

    /**
     * Increment the value at the specified key by passed in val.
     * 
     * @param key
     *            key where the data is stored
     * @param inc
     *            how much to increment by
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long incr(String key, long inc) {
        return incrdecr("incr", key, inc, null);
    }

    /**
     * Increment the value at the specified key by the specified increment, and
     * then return it.
     *
     * @param key
     *            key where the data is stored
     * @param inc
     *            how much to increment by
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long incr(String key, long inc, Integer hashCode) {
        return incrdecr("incr", key, inc, hashCode);
    }

    /**
     * Decrement the value at the specified key by 1, and then return it.
     *
     * @param key
     *            key where the data is stored
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long decr(String key) {
        return incrdecr("decr", key, 1, null);
    }

    /**
     * Decrement the value at the specified key by passed in value, and then
     * return it.
     *
     * @param key
     *            key where the data is stored
     * @param inc
     *            how much to increment by
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long decr(String key, long inc) {
        return incrdecr("decr", key, inc, null);
    }

    /**
     * Decrement the value at the specified key by the specified increment, and
     * then return it.
     *
     * @param key
     *            key where the data is stored
     * @param inc
     *            how much to increment by
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return -1, if the key is not found, the value after incrementing
     *         otherwise
     */
    public long decr(String key, long inc, Integer hashCode) {
        return incrdecr("decr", key, inc, hashCode);
    }

    /**
     * Increments/decrements the value at the specified key by inc.
     * 
     * Note that the server uses a 32-bit unsigned integer, and checks for<br/>
     * underflow. In the event of underflow, the result will be zero. Because
     * <br/>
     * Java lacks unsigned types, the value is returned as a 64-bit integer.
     * <br/>
     * The server will only decrement a value if it already exists;<br/>
     * if a value is not found, -1 will be returned.
     *
     * @param cmdname
     *            increment/decrement
     * @param key
     *            cache key
     * @param inc
     *            amount to incr or decr
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return new value or -1 if not exist
     */
    private long incrdecr(String cmdname, String key, long inc, Integer hashCode) {

        if (key == null) {
            log.error("null key for incrdecr()");
            return -1;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return -1;
        }

        // get SockIO obj for given cache key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        try {
            String cmd = String.format("%s %s %d\r\n", cmdname, key, inc);
            if (log.isDebugEnabled())
                log.debug("++++ memcache incr/decr command: " + cmd);

            sock.write(cmd.getBytes());
            sock.flush();

            // get result back
            String line = sock.readLine();

            if (line.matches("\\d+")) {

                // return sock to pool and return result
                sock.close();
                try {
                    return Long.parseLong(line);
                } catch (Exception ex) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnGet(this, ex, key);

                    log.error(String.format("Failed to parse Long value for key: %s", key));
                }
            } else if (NOTFOUND.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ key not found to incr/decr for key: " + key);
            } else {
                log.error("++++ error incr/decr key: " + key);
                log.error("++++ server response: " + line);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on incr/decr");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return -1;
    }

    private static int incrementCounter(AtomicInteger counter) {
        if (!stats)
            return 0;

        int v;
        do {
            v = counter.get();
        } while (!counter.compareAndSet(v, v + 1));
        return v + 1;
    }

    public static int getNumBackoff() {
        return numBackoff.get();
    }

    public Object get(String key) {
        return get(key, null, false);
    }

    public CASValue gets(String key) {
        return gets(key, null, false);
    }

    public String generateSID() {
        return UUID.randomUUID().toString();
    }

    public boolean finishTrans(String tid, HashMap<String, Integer> keyMap) throws IOException {
        if (tid == null) {
            log.error("tid is null for finishTrans()");
            return false;
        }

        Set<String> its = new HashSet<String>();
        HashMap<String, Set<String>> servers = new HashMap<String, Set<String>>();
        for (String key : keyMap.keySet()) {
            try {
                key = sanitizeKey(key);
                its.add(key);
                Integer hashCode = keyMap.get(key);
                String server = pool.getServer(hashCode);
                if (servers.get(server) == null)
                    servers.put(server, new HashSet<String>());

                servers.get(server).add(key);
            } catch (UnsupportedEncodingException e) {
                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                log.error("failed to sanitize your key!", e);
                return false;
            }
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;

        for (String server : servers.keySet()) {
            // get SockIO obj from hash or from key
            socket = pool.getConnection(server);

            String cmd = "ftrans " + tid + " " + servers.get(server).size();
            for (String it : servers.get(server)) {
                cmd += " " + it;
            }
            cmd += "\r\n";

            try {
                socket.write(cmd.getBytes());
                socket.flush();

                socket.readLine();

                socket.close();
                socket = null;
            } catch (IOException e) {
                System.out.println("IO exception iqget " + tid + ": ");
                e.printStackTrace();

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, tid);

                // exception thrown
                log.error("++++ exception thrown while trying to get object from cache for key: " + tid + " -- "
                        + e.getMessage());

                try {
                    if (socket != null)
                        socket.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + socket.toString());
                    throw ioe;
                }

                socket = null;

                throw e;
            }
        }

        return true;
    }

    public Object iqget(String key) throws IOException {
        return iqget(key, null, false);
    }

    /**
     * Retrieve a key from the server, using a specific hash.
     *
     * If the data was compressed or serialized when compressed, it will
     * automatically<br/>
     * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
     * <br/>
     * Non-serialized data will be returned as a string, so explicit conversion
     * to<br/>
     * numeric types will be necessary, if desired<br/>
     *
     * @param key
     *            key where data is stored
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @return the object that was previously stored, or null if it was not
     *         previously stored
     * @throws IOException
     */
    public Object iqget(String key, Integer hashCode) throws IOException {
        return iqget(key, hashCode, false);
    }

    public List<String> getPriKeys() throws IOException {
        SockIOPool.SockIO socket = null;
        String cmd = "getprik\r\n";

        String[] servers = pool.getServers();
        List<String> keys = new LinkedList<String>();
        for (String server : servers) {
            socket = pool.getConnection(server);
            if (socket != null) {
                socket.write(cmd.getBytes());
                socket.flush();

                String line = socket.readLine();
                String[] tokens = line.split(" ");
                for (int i = 1; i < tokens.length; i++) {
                    keys.add(tokens[i]);
                }

                socket.close();
            }
        }

        System.out.println(keys);

        return keys;
    }

    public CLValue ewget(String key, Integer hashCode) throws IOException {
        return ewget(key, hashCode, primitiveAsString);
    }

    public CLValue ewget(String key, Integer hashCode, boolean asString) throws IOException {
        int pending = 0;

        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "iqget " + key;

        Long lease_token = new Long(0L);

        // get pending lease of this key if existed
        if (this.q_lease_list.containsKey(key)) {
            lease_token = this.q_lease_list.get(key);
        } else if (this.i_lease_list.containsKey(key)) {
            lease_token = this.i_lease_list.get(key);
        }

        cmd += " " + lease_token.longValue();
        cmd += "\r\n";

        boolean value_found = false;

        // resulted object
        Object o = null;

        int backoff = INITIAL_BACKOFF_VALUE;

        // Keep trying to get until either the value is found or a valid
        // lease_token is returned.
        int cnt = 0;
        while (!value_found) {
            socket = getSocket(key, hashCode);

            try {
                if (log.isDebugEnabled())
                    log.debug("++++ memcache iqget command: " + cmd);

                socket.write(cmd.getBytes());
                socket.flush();

                incrementCounter(numIQGet);

                while (true) {
                    String line = socket.readLine();

                    if (log.isDebugEnabled())
                        log.debug("++++ line: " + line);

                    if (line.startsWith(VALUE)) {
                        String[] info = line.split(" ");
                        int flag = Integer.parseInt(info[2]);
                        pending = Integer.parseInt(info[3]);
                        int length = Integer.parseInt(info[4]);

                        if (log.isDebugEnabled()) {
                            log.debug("++++ key: " + key);
                            log.debug("++++ flags: " + flag);
                            log.debug("++++ length: " + length);
                        }

                        // read obj into buffer
                        byte[] buf = new byte[length];
                        socket.read(buf);
                        socket.clearEOL();

                        value_found = true;

                        if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                            try {
                                // read the input stream, and write to a byte
                                // array output stream since
                                // we have to read into a byte array, but we
                                // don't know how large it
                                // will need to be, and we don't want to resize
                                // it a bunch
                                GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                                ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                                int count;
                                byte[] tmp = new byte[2048];
                                while ((count = gzi.read(tmp)) != -1) {
                                    bos.write(tmp, 0, count);
                                }

                                // store uncompressed back to buffer
                                buf = bos.toByteArray();
                                gzi.close();
                            } catch (IOException e) {

                                // if we have an errorHandler, use its hook
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                log.error("++++ IOException thrown while trying to uncompress input stream for key: "
                                        + key + " -- " + e.getMessage());
                                throw new NestedIOException(
                                        "++++ IOException thrown while trying to uncompress input stream for key: "
                                                + key,
                                                e);
                            }
                        }

                        // we can only take out serialized objects
                        if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                            if (primitiveAsString || asString) {
                                // pulling out string value
                                if (log.isInfoEnabled())
                                    log.info("++++ retrieving object and stuffing into a string.");
                                o = new String(buf, defaultEncoding);
                            } else {
                                // decoding object
                                try {
                                    o = NativeHandler.decode(buf, flag);
                                } catch (Exception e) {

                                    // if we have an errorHandler, use its hook
                                    if (errorHandler != null)
                                        errorHandler.handleErrorOnGet(this, e, key);

                                    log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                                    throw new NestedIOException(e);
                                }
                            }
                        } else {
                            // deserialize if the data is serialized
                            ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                                    classLoader);
                            try {
                                o = ois.readObject();
                                if (log.isInfoEnabled())
                                    log.info("++++ deserializing " + o.getClass());
                            } catch (Exception e) {
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                o = null;
                                log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                        + e.getMessage());
                                throw new IOException("Exception thrown while trying to deserialize for key: " + key);
                            }
                        }
                    } else if (line.startsWith(LEASEVALUE)) {
                        String[] info = line.split(" ");
                        pending = Integer.parseInt(info[3]);
                        long token_value = Long.parseLong(info[4]);

                        if (token_value == TOKEN_HOTMISS) {
                            value_found = false;
                            incrementCounter(numBackoff);
                        } else {
                            this.i_lease_list.put(key, token_value);
                            incrementCounter(numILeaseGranted);
                            value_found = true;
                        }
                    } else if (line.startsWith(NOVALUE)) {
                        value_found = true;
                        if (!this.i_lease_list.containsKey(key))
                            this.i_lease_list.put(key, new Long(-1));
                    } else if (END.equals(line)) {
                        if (log.isDebugEnabled())
                            log.debug("++++ finished reading from cache server");

                        break;
                    } else {
                        throw new IOException("iqget: Unexpected return message "+line);
                    }

                }

                socket.close();
                socket = null;

            } catch (IOException e) {
                System.out.println("IO exception iqget " + key + ": ");
                e.printStackTrace();

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                // exception thrown
                log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                        + e.getMessage());

                try {
                    if (socket != null)
                        socket.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + socket.toString());
                    throw ioe;
                }

                socket = null;

                throw e;
            }

            if (backoff == 0) {
                break;
            }

            if (!value_found) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            cnt++;
        }

        if (cnt > maxBackoff)
            maxBackoff = cnt;

        boolean isPending = pending == 0 ? false : true;

        return new CLValue(o, isPending);
    }

    /**
     * Retrieve a key from the server, using a specific hash.
     *
     * If the data was compressed or serialized when compressed, it will
     * automatically<br/>
     * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
     * <br/>
     * Non-serialized data will be returned as a string, so explicit conversion
     * to<br/>
     * numeric types will be necessary, if desired<br/>
     *
     * @param key
     *            key where data is stored
     * @param hashCode
     *            the hash code that is used in the hash function
     * @param asString
     *            store the value as string or byte
     * @return the object that was previously stored, or null if it was not
     *         previously stored
     * @throws IOException
     */
    public Object iqget(String key, Integer hashCode, boolean asString) throws IOException {
        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd = "iqget " + key;

        Long lease_token = new Long(0L);

        // get pending lease of this key if existed
        if (this.q_lease_list.containsKey(key)) {
            lease_token = this.q_lease_list.get(key);
        } else if (this.i_lease_list.containsKey(key)) {
            lease_token = this.i_lease_list.get(key);
        }

        // if (lease_token.longValue() != 0L) {
        // cmd += " " + lease_token.longValue() + "\r\n";
        // } else {
        // cmd += " 0 " + "\r\n";
        // }

        cmd += " " + lease_token.longValue();
        cmd += "\r\n";

        boolean value_found = false;

        // resulted object
        Object o = null;

        int backoff = INITIAL_BACKOFF_VALUE;

        // Keep trying to get until either the value is found or a valid
        // lease_token is returned.
        int cnt = 0;
        while (!value_found) {
            socket = getSocket(key, hashCode);

            try {
                if (log.isDebugEnabled())
                    log.debug("++++ memcache iqget command: " + cmd);

                socket.write(cmd.getBytes());
                socket.flush();

                incrementCounter(numIQGet);

                while (true) {
                    String line = socket.readLine();

                    if (log.isDebugEnabled())
                        log.debug("++++ line: " + line);

                    if (line.startsWith(VALUE)) {
                        String[] info = line.split(" ");
                        int flag = Integer.parseInt(info[2]);
                        int length = Integer.parseInt(info[4]);

                        if (log.isDebugEnabled()) {
                            log.debug("++++ key: " + key);
                            log.debug("++++ flags: " + flag);
                            log.debug("++++ length: " + length);
                        }

                        // read obj into buffer
                        byte[] buf = new byte[length];
                        socket.read(buf);
                        socket.clearEOL();

                        value_found = true;

                        if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                            try {
                                // read the input stream, and write to a byte
                                // array output stream since
                                // we have to read into a byte array, but we
                                // don't know how large it
                                // will need to be, and we don't want to resize
                                // it a bunch
                                GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                                ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                                int count;
                                byte[] tmp = new byte[2048];
                                while ((count = gzi.read(tmp)) != -1) {
                                    bos.write(tmp, 0, count);
                                }

                                // store uncompressed back to buffer
                                buf = bos.toByteArray();
                                gzi.close();
                            } catch (IOException e) {

                                // if we have an errorHandler, use its hook
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                log.error("++++ IOException thrown while trying to uncompress input stream for key: "
                                        + key + " -- " + e.getMessage());
                                throw new NestedIOException(
                                        "++++ IOException thrown while trying to uncompress input stream for key: "
                                                + key,
                                                e);
                            }
                        }

                        // we can only take out serialized objects
                        if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                            if (primitiveAsString || asString) {
                                // pulling out string value
                                if (log.isInfoEnabled())
                                    log.info("++++ retrieving object and stuffing into a string.");
                                o = new String(buf, defaultEncoding);
                            } else {
                                // decoding object
                                try {
                                    o = NativeHandler.decode(buf, flag);
                                } catch (Exception e) {

                                    // if we have an errorHandler, use its hook
                                    if (errorHandler != null)
                                        errorHandler.handleErrorOnGet(this, e, key);

                                    log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                                    throw new NestedIOException(e);
                                }
                            }
                        } else {
                            // deserialize if the data is serialized
                            ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                                    classLoader);
                            try {
                                o = ois.readObject();
                                if (log.isInfoEnabled())
                                    log.info("++++ deserializing " + o.getClass());
                            } catch (Exception e) {
                                if (errorHandler != null)
                                    errorHandler.handleErrorOnGet(this, e, key);

                                o = null;
                                log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                        + e.getMessage());
                                throw new IOException("Exception thrown while trying to deserialize for key: " + key);
                            } finally {
                                if (ois != null) {
                                    ois.close();
                                }
                            }
                        }
                    } else if (line.startsWith(LEASEVALUE)) {
                        String[] info = line.split(" ");
                        long token_value = Long.parseLong(info[4]);

                        if (token_value == TOKEN_HOTMISS) {
                            value_found = false;
                            incrementCounter(numBackoff);
                        } else {
                            this.i_lease_list.put(key, token_value);
                            incrementCounter(numILeaseGranted);
                            value_found = true;
                        }
                    } else if (line.startsWith(NOVALUE)) {
                        value_found = true;
                        if (!this.i_lease_list.containsKey(key))
                            this.i_lease_list.put(key, new Long(-1));
                    } else if (END.equals(line)) {
                        if (log.isDebugEnabled())
                            log.debug("++++ finished reading from cache server");

                        break;
                    } else {
                        socket.close();
                        socket = null;
                        throw new IOException("iqget: Unexpected return message");
                    }

                }

                socket.close();
                socket = null;

            } catch (IOException e) {
                System.out.println("IO exception iqget " + key + ": ");
                e.printStackTrace();

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                // exception thrown
                log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                        + e.getMessage());

                try {
                    if (socket != null)
                        socket.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + socket.toString());
                    throw ioe;
                }

                socket = null;

                throw e;
            }

            if (backoff == 0) {
                break;
            }

            if (!value_found) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            cnt++;
        }

        if (cnt > maxBackoff)
            maxBackoff = cnt;

        if (value_found) {
            return o;
        }

        return null;
    }

    public Boolean oqAppend(String sid, String key, Object value, boolean asString) throws COException, IOException {
        return oqappendorprepend("oqappend", null, key, value, asString, sid);
    }

    public Boolean oqAppend(String sid, String key, Integer hashCode, Object value, boolean asString) throws COException, IOException {
        return oqappendorprepend("oqappend", hashCode, key, value, asString, sid);
    }

    public Boolean oqAppend2(String sid, String key, Object value, boolean asString) throws COException, IOException {
        return oqappendorprepend("oqappend2", null, key, value, asString, sid);
    }

    public Boolean oqAppend2(String sid, String key, Integer hashCode, Object value, boolean asString) throws COException, IOException {
        return oqappendorprepend("oqappend2", hashCode, key, value, asString, sid);
    }	

    public Boolean oqprepend(String sid, String key, Object value) throws IOException, COException {
        return oqappendorprepend("oqprepend", null, key, value, false, sid);
    }

    public Boolean oqprepend(String sid, String key, Integer hashCode, Object value) throws IOException, COException {
        return oqappendorprepend("oqprepend", hashCode, key, value, false, sid);
    }

    public Boolean iqappend(String key, Object value, String tid) throws IQException {
        CLValue ret = null;

        try {
            ret = iqappendorprepend("iqappend", null, key, value, false, tid);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return (Boolean) ret.getValue();
    }

    public Boolean iqappend(String key, Integer hashCode, Object value, String tid) throws IQException {
        CLValue val = null;

        try {
            val = iqappendorprepend("iqappend", hashCode, key, value, false, tid);
        } catch (IOException e) {
            e.printStackTrace(System.out);
            return false;
        }

        return (Boolean) val.getValue();
    }

    public CLValue ewprepend(String key, Integer hashCode, Object value, String tid) throws IQException {
        CLValue val = null;

        try {
            val = iqappendorprepend("iqprepend", null, key, value, false, tid);
        } catch (IOException e) {
            e.printStackTrace(System.out);
            return null;
        }

        return val;
    }

    public CLValue ewappend(String key, Integer hashCode, 
            Object value, String tid) throws IQException {
        try {
            return iqappendorprepend("iqappend", hashCode, key, value, false, tid);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
        return null;
    }

    public Boolean iqprepend(String key, Object value, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqappendorprepend("iqprepend", null, key, value, false, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (Boolean) val.getValue();
    }

    public Boolean iqprepend(String key, Integer hashCode, Object value, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqappendorprepend("iqprepend", hashCode, key, value, false, tid);
            return (Boolean) val.getValue();
        } catch (IQException e) {
            try {
                release(tid, hashCode);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public Long oqincr(String sid, String key) throws COException {
        try {
            return oqincrordecr("oqincr", key, null, 1, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqincr(String sid, String key, Integer hashCode) throws COException {
        try {
            return oqincrordecr("oqincr", key, hashCode, 1, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqincr(String sid, String key, long value) throws COException {
        try {
            return oqincrordecr("oqincr", key, null, value, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqincr(String sid, String key, Integer hashCode, long value) throws COException {
        try {
            return oqincrordecr("oqincr", key, hashCode, value, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqdecr(String sid, String key) throws COException {
        try {
            return oqincrordecr("oqdecr", key, null, 1, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqdecr(String sid, String key, Integer hashCode) throws COException {
        try {
            return oqincrordecr("oqdecr", key, hashCode, 1, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqdecr(String sid, String key, long value) throws COException {
        try {
            return oqincrordecr("oqdecr", key, null, value, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Long oqdecr(String sid, String key, Integer hashCode, long value) throws COException {
        try {
            return oqincrordecr("oqdecr", key, hashCode, value, sid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Boolean iqincr(String key, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqincr", key, null, 1, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    public Boolean iqincr(String key, Integer hashCode, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqincr", key, hashCode, 1, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    public Boolean iqincr(String key, long value, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqincr", key, null, value, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    public Boolean iqdecr(String key, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqdecr", key, null, 1, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    public Boolean iqdecr(String key, Integer hashCode, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqdecr", key, hashCode, 1, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    public Boolean iqdecr(String key, long value, String tid) throws IQException {
        CLValue val = null;
        try {
            val = iqincrordecr("iqdecr", key, null, value, tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return val == null ? null : (Boolean) val.getValue();
    }

    private Long oqincrordecr(String cmdname, String key, Integer hashCode, long value, String sid)
            throws IOException, COException {
        Long val = null;
        if (cmdname == null || cmdname.trim().equals("") || key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj for given cache key
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        try {
            String cmd = String.format("%s %s %s %d\r\n", cmdname, sid, key, value);

            sock.write(cmd.getBytes());
            sock.flush();

            // get result back
            while (true) {
                String line = sock.readLine();

                if (line.contains(NOVALUE)) {
                    //
                } else if (line.contains(VALUE)) {
                    // return sock to pool and return result
                    String[] tokens = line.split(" ");
                    val = Long.parseLong(tokens[1]);
                } else if (ABORT.equals(line)) {
                    sock.close();
                    throw new COException("oqincrdecr aborted.", key);
                } else if (END.equals(line)) {
                    break;
                } else {
                    log.error("++++ error oqincr/oqdecr key: " + key);
                    log.error("++++ server response: " + line);
                }
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on incr/decr");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return val;
    }

    private CLValue iqincrordecr(String cmdname, String key, Integer hashCode, long value, String tid)
            throws IOException, IQException {
        if (cmdname == null || cmdname.trim().equals("") || key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj for given cache key
        SockIOPool.SockIO sock = getSocket(key, hashCode);
        boolean isPending = false;
        try {
            String cmd;

            if (tid == null || tid == "") {
                log.error("IQ increment with no transaction id");
                return null;
            } else
                cmd = String.format("%s %s %d %s\r\n", cmdname, key, value, tid);

            if (log.isDebugEnabled())
                log.debug("++++ memcache incr/decr command: " + cmd);

            sock.write(cmd.getBytes());
            sock.flush();

            // get result back
            String line = sock.readLine();
            String[] tokens = line.split(" ");

            int pending = 0;
            if (tokens.length >= 2) {
                pending = Integer.parseInt(tokens[1]);
            }

            if (tokens.length >= 3) {
                this.q_lease_list.put(key, Long.parseLong(tokens[2]));
            }

            isPending = pending == 0 ? false : true;

            if (line.contains("OK_NOVALUE")) {
                sock.close();
                sock = null;
                return new CLValue(false, isPending);
            } else if (line.contains("OK")) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);

                sock.close();
                sock = null;
                return new CLValue(true, isPending);
            } else if (line.contains("ABORT")) {
                if (log.isInfoEnabled())
                    log.info("++++ fail to get lease iqincr/decr for key: " + key);

                sock.close();
                sock = null;

                throw new IQException("Fail to get lease iqincr/decr for key: " + key);
            } else {
                log.error("++++ error incr/decr key: " + key);
                log.error("++++ server response: " + line);
            }
        } catch (IOException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on incr/decr");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        }

        if (sock != null) {
            sock.close();
            sock = null;
        }

        return null;
    }

    private Boolean oqappendorprepend(String cmdname, Integer hashCode, String key, Object value, boolean asString,
            String sid) throws IOException, COException {
        if (cmdname == null || cmdname.trim().equals("") || key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return false;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return false;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        byte[] val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return false;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        try {
            String cmd = null;

            cmd = String.format("%s %s %s %d\r\n", cmdname, sid, key, val.length);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (STORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
                return true;
            } else if (NOTSTORED.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);
                return false;
            } else if (ABORT.equals(line)) {
                if (log.isInfoEnabled())
                    log.info("++++ sar ignored for key: " + key);
                throw new COException("oqappend_prepend session aborted.", key);
            } else {
                log.error("++++ oqappendprepend for key: " + key + ", returned: " + line);
            }
            return false;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());

                throw ioe;
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return false;
    }

    public CLValue ewincr(String tid, String key, Integer hashCode) throws IQException {
        try {
            return iqincrordecr("iqincr", key, hashCode, 1L, tid);
        } catch (IQException e) {
            try {
                release(tid, hashCode);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public CLValue ewdecr(String tid, String key, Integer hashCode) throws IQException {
        try {
            return iqincrordecr("iqdecr", key, hashCode, 1L, tid);
        } catch (IQException e) {
            try {
                release(tid, hashCode);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private CLValue iqappendorprepend(String cmdname, Integer hashCode, String key, Object value, boolean asString,
            String tid) throws IOException, IQException {
        if (cmdname == null || cmdname.trim().equals("") || key == null) {
            log.error("key is null or cmd is null/empty for set()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(key, hashCode);

        byte[] val = convertToByteArray(value, asString, key);
        if (val == null) {
            // return socket to pool and bail
            sock.close();
            sock = null;
            return null;
        }

        // now try to compress if we want to
        // and if the length is over the threshold
        if (compressEnable && val.length > compressThreshold) {

            try {
                if (log.isInfoEnabled()) {
                    log.info("++++ trying to compress data");
                    log.info("++++ size prior to compression: " + val.length);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(val, 0, val.length);
                gos.finish();
                gos.close();

                // store it and set compression flag
                val = bos.toByteArray();

                if (log.isInfoEnabled())
                    log.info("++++ compression succeeded, size after: " + val.length);
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);

                log.error("IOException while compressing stream: " + e.getMessage());
                log.error("storing data uncompressed");

                throw e;
            }
        }

        if (val.length > MAX_OBJECT_SIZE) {
            log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                    + " Value too large, max is " + MAX_OBJECT_SIZE);

            if (sock != null) {
                sock.close();
                sock = null;
            }

            throw new IOException(
                    "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
        }

        boolean isPending = false;

        try {
            String cmd = null;

            cmd = String.format("%s %s %d %s\r\n", cmdname, key, val.length, tid);

            sock.write(cmd.getBytes());

            if (val.length > 0) {
                sock.write(val);
            }
            sock.write("\r\n".getBytes());
            sock.flush();

            // get result code
            String line = sock.readLine();
            String[] tokens = line.split(" ");

            int pending = 0;
            if (tokens.length >= 2) {
                pending = Integer.parseInt(tokens[1]);
            }

            if (tokens.length >= 3) {
                this.q_lease_list.put(key, Long.parseLong(tokens[2]));
            }

            isPending = pending == 0 ? false : true;

            if (log.isInfoEnabled())
                log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");

            if (line.contains("OK_NOVALUE")) {
                sock.close();
                sock = null;
                return new CLValue(false, isPending);
            } else if (line.contains("NOT_STORED")) {
                sock.close();
                sock = null;
                return new CLValue(false, isPending);
            } else if (line.contains("OK")) {
                if (log.isInfoEnabled())
                    log.info("++++ data successfully swapped for key: " + key);

                sock.close();
                sock = null;
                return new CLValue(true, isPending);
            } else if (line.contains("NO_LEASE")) {
                if (log.isInfoEnabled())
                    log.info("++++ data not swapped in cache for key: " + key);

                sock.close();
                sock = null;
                return new CLValue(true, isPending);
            } else if (line.contains("ABORT")) {
                if (log.isInfoEnabled())
                    log.info("++++ iqappend/prepend ignored for key: " + key);

                sock.close();
                sock = null;

                throw new IQException("iqappend/prepend for key: " + key);
            } else {
                log.error("++++ sar for key: " + key + ", returned: " + line);
            }

            sock.close();
            sock = null;
            return new CLValue(false, isPending);
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());

                throw ioe;
            }

            sock = null;
        }

        return new CLValue(false, isPending);
    }

    public boolean hasLease(String key) {
        Long lease_token = i_lease_list.get(key);

        if (lease_token == null)
            lease_token = q_lease_list.get(key);

        return (lease_token != null);
    }
    
    /**
     * co_muliGet
     * @param sid
     * @param hashCode
     * @param typeOfLease 0 if C lease, 1 if O lease
     * @param asString
     * @param keys
     * @return
     * @throws COException 
     */
    public Map<String, Object> co_getMulti(String sid, Integer hashCode, 
            int typeOfLease, boolean asString, String... keys) throws COException {
        if (sid == null) {
            log.error("null value for sid passed to co_getMulti");
            return null;
        }
        
        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnDelete(this, e, sid);
            log.error("failed to sanitize your key!", e);
            return null;
        }        
        
        if (keys == null) {
            log.error("keys is null for co_getMulti");
            return null;
        }

        Map<String, Object> result = new HashMap<>();

        StringBuilder cmd = new StringBuilder();        
        Set<String> keySet = new HashSet<>(Arrays.asList(keys));
        
        int backoff = INITIAL_BACKOFF_VALUE;

        long t = System.nanoTime();
        while (keySet.size() > 0) {
            cmd.setLength(0);

            cmd.append("cogets "+sid+" "+typeOfLease);
    
            for (String key : keySet) {
                try {
                    key = sanitizeKey(key);
                } catch (UnsupportedEncodingException e) {
    
                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnGet(this, e, key);
    
                    log.error("failed to sanitize your key!", e);
                    return null;
                }
                cmd.append(" " + key);
            }
            cmd.append("\r\n");
    
            String cmdStr = cmd.toString();
    
            // get SockIO obj using cache key
            SockIOPool.SockIO socket = getSocket(keys[0], hashCode);
    
            try {
                if (log.isDebugEnabled())
                    log.debug("++++ memcache iqget command: " + cmdStr);
    
                socket.write(cmdStr.getBytes());
                socket.flush();
    
                while (true) {
                    String line = socket.readLine();
    
                    if (log.isDebugEnabled())
                        log.debug("++++ line: " + line);
    
                    if (line.startsWith(ABORT)) {
                        if (log.isInfoEnabled())
                            log.info("++++ co_getMulti returned abort: " + Arrays.toString(keys));
                        throw new COException("co_getMulti session was aborted.", Arrays.toString(keys));
                    } else if (line.startsWith(VALUE)) {
                        String[] info = line.split(" ");
                        String key = info[1];
                        int flag = Integer.parseInt(info[2]);
                        int length = Integer.parseInt(info[3]);
                        
                        // ready object
                        Object o = getValue(key, asString, socket, flag, length);
                        if (o != null) {
                            result.put(key, o);
                        }
                        
                        keySet.remove(key);
                    } else if (line.startsWith(LEASE)) {
                        String[] info = line.split(" ");
                        String key = info[1];
                        long token_value = Long.parseLong(info[2]);

                        if (!this.i_lease_list.containsKey(key)) {
                            this.i_lease_list.put(key, token_value);
                            incrementCounter(numILeaseGranted);
                        } 
                        
                        // lease grabbed, no need to retry
                        keySet.remove(key);
                    } else if (line.startsWith(RETRY)) {
                        String key = line.split(" ")[1];
                        if (backoff == 0) {
                            throw new COException("The server asks for retry", Arrays.toString(keys));
                        }
                    } else if (line.startsWith(NOVALUE)) {
                        String key = line.split(" ")[1];
                        keySet.remove(key);
                    } else if (END.equals(line)) {
                        if (log.isDebugEnabled())
                            log.debug("++++ finished reading from cache server");
                        break;
                    } else {
                        throw new IOException(line);
                    }
                }
            } catch (IOException e) {
                System.out.println("IO exception get " + Arrays.toString(keys) + ": "+e.getMessage());
                e.printStackTrace();
    
                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, keys);
    
                // exception thrown
                log.error("++++ exception thrown while trying to get object from cache for key: " + keys + " -- "
                        + e.getMessage());
    
                try {
                    if (socket != null)
                        socket.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + socket.toString());
                }
                socket = null;
            } finally {
                if (socket != null) {
                    socket.close();
                    socket = null;
                }
            }
            
            if (keySet.size() > 0) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        t = System.nanoTime() - t;
        if (typeOfLease == 0) {
            st.add("cigets", t);
        } else if (typeOfLease == 1) {
            st.add("oqreads", t);
        }

        return result;
    }
    
    public Map<String, Boolean> oqAppends(String sid, String[] keys, 
            Object[] values, Integer hashCode, boolean asString) throws IOException, COException {
        return oqAppends(sid, keys, values, null, hashCode, asString);
    }
    
    private Map<String, Boolean> oqAppends(String sid, String[] keys, Object[] values, 
            Date expiry, Integer hashCode, boolean asString) throws IOException, COException {
        if (keys == null || values == null || keys.length != values.length) {
            log.error("Size of keys does not match size of values.");
            return null;
        }
        
        if (sid == null) {
            log.error("sid is null or cmd is null/empty for oqAppends");
            return null;       
        }
        
        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return null;
        }
        
        String cmd =  "oqappends " + sid;
        
        byte[][] bytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; ++i) {
            String key = keys[i];
            
            if (key == null) {
                log.error("key is null or cmd is null/empty for oqAppends()");
                return null;
            }
    
            try {
                key = sanitizeKey(key);
            } catch (UnsupportedEncodingException e) {
                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);
    
                log.error("failed to sanitize your key!", e);
                return null;
            }
            
            if (expiry == null)
                expiry = new Date(0);

            // store flags
            int flags = 0;
            
            byte[] val = null;
            
            if (values[i] != null) {
                val = convertToByteArray(values[i], asString, key);
                if (val == null) {
                    return null;
                }
    
                // Set the flags
                if (NativeHandler.isHandled(values[i])) {
    
                    if (asString) {
                    } else {
                        flags |= NativeHandler.getMarkerFlag(values[i]);
                    }
                } else {
                    // always serialize for non-primitive types
                    flags |= F_SERIALIZED;
                }
    
                // now try to compress if we want to
                // and if the length is over the threshold
                if (compressEnable && val.length > compressThreshold) {
    
                    try {
                        if (log.isInfoEnabled()) {
                            log.info("++++ trying to compress data");
                            log.info("++++ size prior to compression: " + val.length);
                        }
                        ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                        GZIPOutputStream gos = new GZIPOutputStream(bos);
                        gos.write(val, 0, val.length);
                        gos.finish();
                        gos.close();
    
                        // store it and set compression flag
                        val = bos.toByteArray();
                        flags |= F_COMPRESSED;
    
                        if (log.isInfoEnabled())
                            log.info("++++ compression succeeded, size after: " + val.length);
                    } catch (IOException e) {
    
                        // if we have an errorHandler, use its hook
                        if (errorHandler != null)
                            errorHandler.handleErrorOnSet(this, e, key);
    
                        log.error("IOException while compressing stream: " + e.getMessage());
                        log.error("storing data uncompressed");
    
                        throw e;
                    }
                }
    
                if (val.length > MAX_OBJECT_SIZE) {
                    log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                            + " Value too large, max is " + MAX_OBJECT_SIZE);
    
                    throw new IOException(
                            "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
                }       
            }
            
            bytes[i] = val;
            if (val != null) {
                cmd += String.format(" %s %d %d %d", key, flags, (expiry.getTime() / 1000), val.length);
            } else {
                cmd += String.format(" %s %d %d %d", key, flags, (expiry.getTime() / 1000), 0);
            }
        }
        cmd += "\r\n";

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(keys[0], hashCode);

        long t = System.nanoTime();
        
        // now write the data to the cache server
        try {     
            sock.write(cmd.getBytes());

            for (int i = 0; i < bytes.length; ++i) {
                if (bytes[i] != null && bytes[i].length > 0) {
                    sock.write(bytes[i]);
                    sock.write("\r\n".getBytes());
                }
            }
            sock.flush();

            // get result code
            int i = 0;
            Map<String, Boolean> res = new HashMap<>();
            
            while (true) {
                String line = sock.readLine();
                
                if (log.isInfoEnabled())
                    log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");
                
                if (ABORT.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data not appended in cache for key: " + Arrays.toString(keys));
                    throw new COException("oqswap session was aborted.", keys[i]);
                } else if (INVALID.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ sar ignored for key: " + Arrays.toString(keys));
                    throw new COException("oqswap session was aborted.", keys[i]);
                } else if (STORED.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data successfully appended for key: " + keys[i]);
                    res.put(keys[i], true);
                } else if (NOTSTORED.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data successfully appended for key: " + keys[i]);
                    res.put(keys[i], false);
                } else if (END.equals(line)) {
                    break;
                } else {
                    log.error("++++ sar for key: " + Arrays.toString(keys) + ", returned: " + line);
                }
                
                i++;
            }
            
            t = System.nanoTime() - t;
            st.add("oqappends", t);

            return res;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, Arrays.toString(keys));

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error("Command "+cmd);
            log.error(Arrays.toString(keys));
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }
        
        t = System.nanoTime() - t;
        st.add("oqappends", t);

        return null;
    }    
    
    public Map<String, Boolean> oqSwaps(String sid, String[] keys, Object[] values, 
            Integer hashCode, boolean asString) throws IOException, COException {
        return oqSwaps(sid, keys, values, null, hashCode, asString);
    }
    
    private Map<String, Boolean> oqSwaps(String sid, String[] keys, Object[] values, 
            Date expiry, Integer hashCode, boolean asString) throws IOException, COException {
        if (keys == null || values == null || keys.length != values.length) {
            log.error("Size of keys does not match size of values.");
            return null;
        }
        
        if (sid == null) {
            log.error("sid is null or cmd is null/empty for oqAppends");
            return null;       
        }
        
        try {
            sid = sanitizeKey(sid);
        } catch (UnsupportedEncodingException e) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, sid);

            log.error("failed to sanitize your key!", e);
            return null;
        }
        
        long t = System.nanoTime();
        
        String cmd =  "oqswaps " + sid;
        
        byte[][] bytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; ++i) {
            String key = keys[i];
            
            if (key == null) {
                log.error("key is null or cmd is null/empty for oqAppends()");
                return null;
            }
    
            try {
                key = sanitizeKey(key);
            } catch (UnsupportedEncodingException e) {
                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnSet(this, e, key);
    
                log.error("failed to sanitize your key!", e);
                return null;
            }
            
            if (expiry == null)
                expiry = new Date(0);

            // store flags
            int flags = 0;
            
            byte[] val = null;
            
            if (values[i] != null) {
                val = convertToByteArray(values[i], asString, key);
                if (val == null) {
                    return null;
                }
    
                // Set the flags
                if (NativeHandler.isHandled(values[i])) {
    
                    if (asString) {
                    } else {
                        flags |= NativeHandler.getMarkerFlag(values[i]);
                    }
                } else {
                    // always serialize for non-primitive types
                    flags |= F_SERIALIZED;
                }
    
                // now try to compress if we want to
                // and if the length is over the threshold
                if (compressEnable && val.length > compressThreshold) {
    
                    try {
                        if (log.isInfoEnabled()) {
                            log.info("++++ trying to compress data");
                            log.info("++++ size prior to compression: " + val.length);
                        }
                        ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
                        GZIPOutputStream gos = new GZIPOutputStream(bos);
                        gos.write(val, 0, val.length);
                        gos.finish();
                        gos.close();
    
                        // store it and set compression flag
                        val = bos.toByteArray();
                        flags |= F_COMPRESSED;
    
                        if (log.isInfoEnabled())
                            log.info("++++ compression succeeded, size after: " + val.length);
                    } catch (IOException e) {
    
                        // if we have an errorHandler, use its hook
                        if (errorHandler != null)
                            errorHandler.handleErrorOnSet(this, e, key);
    
                        log.error("IOException while compressing stream: " + e.getMessage());
                        log.error("storing data uncompressed");
    
                        throw e;
                    }
                }
    
                if (val.length > MAX_OBJECT_SIZE) {
                    log.error("++++ error storing data in cache for key: " + key + " -- length: " + val.length
                            + " Value too large, max is " + MAX_OBJECT_SIZE);
    
                    throw new IOException(
                            "Payload too large. Max is " + MAX_OBJECT_SIZE + " whereas the payload size is " + val.length);
                }       
            }
            
            bytes[i] = val;
            if (val != null) {
                cmd += String.format(" %s %d %d %d", key, flags, (expiry.getTime() / 1000), val.length);
            } else {
                cmd += String.format(" %s %d %d %d", key, flags, (expiry.getTime() / 1000), 0);
            }
        }
        cmd += "\r\n";

        // get SockIO obj
        SockIOPool.SockIO sock = getSocket(keys[0], hashCode);

        // now write the data to the cache server
        try {     
            sock.write(cmd.getBytes());

            for (int i = 0; i < bytes.length; ++i) {
                if (bytes[i] != null && bytes[i].length > 0) {
                    sock.write(bytes[i]);
                    sock.write("\r\n".getBytes());
                }
            }
            sock.flush();

            // get result code
            int i = 0;
            Map<String, Boolean> res = new HashMap<>();
            
            while (true) {
                String line = sock.readLine();

                if (log.isInfoEnabled())
                    log.info("++++ memcache cmd (result code): " + cmd + " (" + line + ")");
                
                if (ABORT.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data not appended in cache for key: " + Arrays.toString(keys));
                    throw new COException("oqswap session was aborted.", keys[i]);
                } else if (INVALID.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ sar ignored for key: " + Arrays.toString(keys));
                    throw new COException("oqswap session was aborted.", keys[i]);
                } else if (STORED.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data successfully appended for key: " + keys[i]);
                    res.put(keys[i], true);
                } else if (NOTSTORED.equals(line)) {
                    if (log.isInfoEnabled())
                        log.info("++++ data successfully appended for key: " + keys[i]);
                    res.put(keys[i], false);
                } else if (END.equals(line)) {
                    break;
                } else {
                    log.error("++++ sar for key: " + Arrays.toString(keys) + ", returned: " + line);
                }
                
                i++;
            }
            
            t = System.nanoTime() - t;
            st.add("oqswaps", t);

            return res;
        } catch (IOException e) {
            if (errorHandler != null)
                errorHandler.handleErrorOnSet(this, e, Arrays.toString(keys));

            // exception thrown
            log.error("++++ exception thrown while writing bytes to server on set");
            log.error(e.getMessage(), e);

            try {
                sock.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + sock.toString());
            }

            sock = null;
        } finally {
            if (sock != null) {
                sock.close();
                sock = null;
            }
        }
        
        t = System.nanoTime() - t;
        st.add("oqswaps", t);

        return null;
    }        

    /**
     * Retrieve a key from the server, using a specific hash.
     *
     * If the data was compressed or serialized when compressed, it will
     * automatically<br/>
     * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
     * <br/>
     * Non-serialized data will be returned as a string, so explicit conversion
     * to<br/>
     * numeric types will be necessary, if desired<br/>
     *
     * @param key
     *            key where data is stored
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @param asString
     *            if true, then return string val
     * @return the object that was previously stored, or null if it was not
     *         previously stored
     */
    public Map<String, Object> getMulti(Integer hashCode, boolean asString, String... keys) {

        if (keys == null) {
            log.error("keys is null for get()");
            return null;
        }

        Map<String, Object> result = new HashMap<>();

        StringBuilder cmd = new StringBuilder();
        cmd.append("get");

        for (String key : keys) {
            try {
                key = sanitizeKey(key);
            } catch (UnsupportedEncodingException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                log.error("failed to sanitize your key!", e);
                return null;
            }
            cmd.append(" " + key);
        }
        cmd.append("\r\n");

        String cmdStr = cmd.toString();

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;

        socket = getSocket(keys[0], hashCode);

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache iqget command: " + cmdStr);

            socket.write(cmdStr.getBytes());
            socket.flush();

            while (true) {
                String line = socket.readLine();

                if (log.isDebugEnabled())
                    log.debug("++++ line: " + line);

                if (line.startsWith(VALUE)) {
                    String[] info = line.split(" ");
                    String key = info[1];
                    int flag = Integer.parseInt(info[2]);
                    int length = Integer.parseInt(info[3]);
                    // ready object
                    Object o = getValue(key, asString, socket, flag, length);
                    if (o != null) {
                        result.put(key, o);
                    }
                } else if (END.equals(line)) {
                    if (log.isDebugEnabled())
                        log.debug("++++ finished reading from cache server");
                    break;
                } else {
                    throw new IOException();
                }

            }

            socket.close();
            socket = null;
        } catch (IOException e) {
            System.out.println("IO exception get " + Arrays.toString(keys) + ": ");
            e.printStackTrace();

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, keys);

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + keys + " -- "
                    + e.getMessage());

            try {
                if (socket != null)
                    socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
            }
            socket = null;
        }

        return result;
    }

    /**
     * Retrieve a key from the server, using a specific hash.
     *
     * If the data was compressed or serialized when compressed, it will
     * automatically<br/>
     * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
     * <br/>
     * Non-serialized data will be returned as a string, so explicit conversion
     * to<br/>
     * numeric types will be necessary, if desired<br/>
     *
     * @param key
     *            key where data is stored
     * @param hashCode
     *            if not null, then the int hashcode to use
     * @param asString
     *            if true, then return string val
     * @return the object that was previously stored, or null if it was not
     *         previously stored
     */
    public Object get(String key, Integer hashCode, boolean asString) {

        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd;

        cmd = "get ";
        cmd += key + "\r\n";

        // ready object
        Object o = null;

        socket = getSocket(key, hashCode);

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache iqget command: " + cmd);

            socket.write(cmd.getBytes());
            socket.flush();

            while (true) {
                String line = socket.readLine();

                if (log.isDebugEnabled())
                    log.debug("++++ line: " + line);

                if (line.startsWith(VALUE)) {
                    String[] info = line.split(" ");
                    int flag = Integer.parseInt(info[2]);
                    int length = Integer.parseInt(info[3]);
                    o = getValue(key, asString, socket, flag, length);
                } else if (END.equals(line)) {
                    if (log.isDebugEnabled())
                        log.debug("++++ finished reading from cache server");
                    break;
                } else {
                    throw new IOException();
                }

            }

            socket.close();
            socket = null;
        } catch (IOException e) {
            System.out.println("IO exception get " + key + ": ");
            e.printStackTrace();
            System.exit(-1);

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                    + e.getMessage());

            try {
                if (socket != null)
                    socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
            }
            socket = null;
        }

        return o;
    }

    public CASValue gets(String key, Integer hashCode, boolean asString) {
        if (key == null) {
            log.error("key is null for get()");
            return null;
        }

        try {
            key = sanitizeKey(key);
        } catch (UnsupportedEncodingException e) {

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            log.error("failed to sanitize your key!", e);
            return null;
        }

        // get SockIO obj using cache key
        SockIOPool.SockIO socket = null;
        String cmd;

        cmd = "gets ";
        cmd += key + "\r\n";

        // ready object
        Object o = null;
        CASValue casValue = null;

        socket = getSocket(key, hashCode);

        try {
            if (log.isDebugEnabled())
                log.debug("++++ memcache iqget command: " + cmd);

            socket.write(cmd.getBytes());
            socket.flush();

            while (true) {
                String line = socket.readLine();

                if (log.isDebugEnabled())
                    log.debug("++++ line: " + line);

                if (line.startsWith(VALUE)) {
                    String[] info = line.split(" ");
                    int flag = Integer.parseInt(info[2]);
                    int length = Integer.parseInt(info[3]);
                    long casToken = Long.parseLong(info[4]);

                    if (log.isDebugEnabled()) {
                        log.debug("++++ key: " + key);
                        log.debug("++++ flags: " + flag);
                        log.debug("++++ length: " + length);
                        log.debug("++++ casToken: " + casToken);
                    }

                    o = getValue(key, asString, socket, flag, length);

                    if (o != null) {
                        casValue = new CASValue(o, casToken);
                    }

                } else if (END.equals(line)) {
                    if (log.isDebugEnabled())
                        log.debug("++++ finished reading from cache server");
                    break;
                } else {
                    System.out.println(line);
                    throw new IOException();
                }

            }

            socket.close();
            socket = null;
        } catch (IOException e) {
            System.out.println("IO exception get " + key + ": ");
            e.printStackTrace();

            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(this, e, key);

            // exception thrown
            log.error("++++ exception thrown while trying to get object from cache for key: " + key + " -- "
                    + e.getMessage());

            try {
                if (socket != null)
                    socket.trueClose();
            } catch (IOException ioe) {
                log.error("++++ failed to close socket : " + socket.toString());
            }
            socket = null;
        }

        return casValue;
    }

    private Object getValue(String key, boolean asString, SockIOPool.SockIO socket, int flag, int length)
            throws IOException, NestedIOException, UnsupportedEncodingException {
        // read obj into buffer
        byte[] buf = new byte[length];
        socket.read(buf);
        socket.clearEOL();
        Object o = null;

        if ((flag & F_COMPRESSED) == F_COMPRESSED) {
            try {
                // read the input stream, and write to a byte array
                // output stream since
                // we have to read into a byte array, but we don't
                // know how large it
                // will need to be, and we don't want to resize it a
                // bunch
                GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                int count;
                byte[] tmp = new byte[2048];
                while ((count = gzi.read(tmp)) != -1) {
                    bos.write(tmp, 0, count);
                }

                // store uncompressed back to buffer
                buf = bos.toByteArray();
                gzi.close();
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                log.error("++++ IOException thrown while trying to uncompress input stream for key: " + key + " -- "
                        + e.getMessage());
                throw new NestedIOException(
                        "++++ IOException thrown while trying to uncompress input stream for key: " + key, e);
            }
        }

        // we can only take out serialized objects
        if ((flag & F_SERIALIZED) != F_SERIALIZED) {
            if (primitiveAsString || asString) {
                // pulling out string value
                if (log.isInfoEnabled())
                    log.info("++++ retrieving object and stuffing into a string.");
                o = new String(buf, defaultEncoding);
            } else {
                // decoding object
                try {
                    o = NativeHandler.decode(buf, flag);
                } catch (Exception e) {

                    // if we have an errorHandler, use its hook
                    if (errorHandler != null)
                        errorHandler.handleErrorOnGet(this, e, key);

                    log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
                    throw new NestedIOException(e);
                }
            }
        } else {
            // deserialize if the data is serialized
            ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf), classLoader);
            try {
                o = ois.readObject();
                if (log.isInfoEnabled())
                    log.info("++++ deserializing " + o.getClass());
            } catch (Exception e) {
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                o = null;
                log.error(
                        "++++ Exception thrown while trying to deserialize for key: " + key + " -- " + e.getMessage());
            } finally {
                if (ois != null) {
                    ois.close();
                }
            }
        }
        return o;
    }

    /**
     * Retrieve multiple objects from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            String array of keys to retrieve
     * @return Object array ordered in same order as key array containing
     *         results
     */
    public Object[] getMultiArray(String[] keys) {
        return getMultiArray(keys, null, false);
    }

    /**
     * Retrieve multiple objects from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            String array of keys to retrieve
     * @param hashCodes
     *            if not null, then the Integer array of hashCodes
     * @return Object array ordered in same order as key array containing
     *         results
     */
    public Object[] getMultiArray(String[] keys, Integer[] hashCodes) {
        return getMultiArray(keys, hashCodes, false);
    }

    /**
     * Retrieve multiple objects from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            String array of keys to retrieve
     * @param hashCodes
     *            if not null, then the Integer array of hashCodes
     * @param asString
     *            if true, retrieve string vals
     * @return Object array ordered in same order as key array containing
     *         results
     */
    public Object[] getMultiArray(String[] keys, Integer[] hashCodes, boolean asString) {

        Map<String, Object> data = getMulti(keys, hashCodes, asString, false);

        if (data == null)
            return null;

        Object[] res = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            res[i] = data.get(keys[i]);
        }

        return res;
    }

    /**
     * Retrieve multiple objects from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            String array of keys to retrieve
     * @return a hashmap with entries for each key is found by the server, keys
     *         that are not found are not entered into the hashmap, but
     *         attempting to retrieve them from the hashmap gives you null.
     */
    public Map<String, Object> getMulti(String[] keys) {
        return getMulti(keys, null, false, false);
    }

    public Map<String, Object> getMulti(String[] keys, boolean asyncIO) {
        if (asyncIO) {
            return getMulti(keys);
        }
        return getMulti(null, false, keys);
    }

    /**
     * Retrieve multiple objects from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            String array of keys to retrieve
     * @return a hashmap with entries for each key is found by the server, keys
     *         that are not found are not entered into the hashmap, but
     *         attempting to retrieve them from the hashmap gives you null.
     */
    public Map<String, Object> getsMulti(String[] keys) {
        return getMulti(keys, null, false, true);
    }

    /**
     * Retrieve multiple keys from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            keys to retrieve
     * @param hashCodes
     *            if not null, then the Integer array of hashCodes
     * @return a hashmap with entries for each key is found by the server, keys
     *         that are not found are not entered into the hashmap, but
     *         attempting to retrieve them from the hashmap gives you null.
     */
    public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) {
        return getMulti(keys, hashCodes, false, false);
    }

    /**
     * Retrieve multiple keys from the memcache.
     *
     * This is recommended over repeated calls to {@link #iqget(String) get()},
     * since it<br/>
     * is more efficient.<br/>
     *
     * @param keys
     *            keys to retrieve
     * @param hashCodes
     *            if not null, then the Integer array of hashCodes
     * @param asString
     *            if true then retrieve using String val
     * @return a hashmap with entries for each key is found by the server, keys
     *         that are not found are not entered into the hashmap, but
     *         attempting to retrieve them from the hashmap gives you null.
     */
    public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes, boolean asString, boolean isGets) {

        if (keys == null || keys.length == 0) {
            log.error("missing keys for getMulti()");
            return null;
        }

        Map<String, StringBuilder> cmdMap = new HashMap<String, StringBuilder>();

        for (int i = 0; i < keys.length; ++i) {

            String key = keys[i];
            if (key == null) {
                log.error("null key, so skipping");
                continue;
            }


            Integer hash = null;
            if (hashCodes != null) {
                hash = hashCodes[i];
            }

            String cleanKey = key;
            try {
                cleanKey = sanitizeKey(key);
            } catch (UnsupportedEncodingException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                log.error("failed to sanitize your key!", e);
                continue;
            }

            // get SockIO obj from cache key
            SockIOPool.SockIO sock = getSocket(cleanKey, hash);

            // store in map and list if not already
            if (!cmdMap.containsKey(sock.getHost())) {
                if (isGets) {
                    cmdMap.put(sock.getHost(), new StringBuilder("gets"));
                } else {
                    cmdMap.put(sock.getHost(), new StringBuilder("get"));
                }
            }

            cmdMap.get(sock.getHost()).append(" " + cleanKey);

            // return to pool
            sock.close();
        }

        if (log.isInfoEnabled())
            log.info("multi get socket count : " + cmdMap.size());

        // now query memcache
        Map<String, Object> ret = new HashMap<String, Object>(keys.length);

        // now use new NIO implementation
        (new NIOLoader(this)).doMulti(asString, cmdMap, keys, ret, isGets);

        // fix the return array in case we had to rewrite any of the keys
        for (String key : keys) {

            String cleanKey = key;
            try {
                cleanKey = sanitizeKey(key);
            } catch (UnsupportedEncodingException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnGet(this, e, key);

                log.error("failed to sanitize your key!", e);
                continue;
            }

            if (!key.equals(cleanKey) && ret.containsKey(cleanKey)) {
                ret.put(key, ret.get(cleanKey));
                ret.remove(cleanKey);
            }
        }

        if (log.isDebugEnabled())
            log.debug("++++ memcache: got back " + ret.size() + " results");
        return ret;
    }

    /**
     * This method loads the data from cache into a Map.
     *
     * Pass a SockIO object which is ready to receive data and a HashMap<br/>
     * to store the results.
     * 
     * @param sock
     *            socket waiting to pass back data
     * @param hm
     *            hashmap to store data into
     * @param asString
     *            if true, and if we are using NativehHandler, return string val
     * @throws IOException
     *             if io exception happens while reading from socket
     */
    private void loadMulti(LineInputStream input, Map<String, Object> hm, boolean asString, boolean isGets)
            throws IOException {

        while (true) {
            String line = input.readLine();
            if (log.isDebugEnabled())
                log.debug("++++ line: " + line);

            if (line.startsWith(VALUE)) {
                String[] info = line.split(" ");
                String key = info[1];
                int flag = Integer.parseInt(info[2]);
                int length = Integer.parseInt(info[3]);
                Optional<Long> casToken = isGets ? Optional.of(Long.parseLong(info[4])) : Optional.empty();

                if (log.isDebugEnabled()) {
                    log.debug("++++ key: " + key);
                    log.debug("++++ flags: " + flag);
                    log.debug("++++ length: " + length);
                    if (casToken.isPresent()) {
                        log.debug("++++ cas token: " + casToken.get());
                    }
                }

                // read obj into buffer
                byte[] buf = new byte[length];
                input.read(buf);
                input.clearEOL();

                // ready object
                Object o;

                // check for compression
                if ((flag & F_COMPRESSED) == F_COMPRESSED) {
                    try {
                        // read the input stream, and write to a byte array
                        // output stream since
                        // we have to read into a byte array, but we don't know
                        // how large it
                        // will need to be, and we don't want to resize it a
                        // bunch
                        GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
                        ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);

                        int count;
                        byte[] tmp = new byte[2048];
                        while ((count = gzi.read(tmp)) != -1) {
                            bos.write(tmp, 0, count);
                        }

                        // store uncompressed back to buffer
                        buf = bos.toByteArray();
                        gzi.close();
                    } catch (IOException e) {

                        // if we have an errorHandler, use its hook
                        if (errorHandler != null)
                            errorHandler.handleErrorOnGet(this, e, key);

                        log.error("++++ IOException thrown while trying to uncompress input stream for key: " + key
                                + " -- " + e.getMessage());
                        throw new NestedIOException(
                                "++++ IOException thrown while trying to uncompress input stream for key: " + key, e);
                    }
                }

                // we can only take out serialized objects
                if ((flag & F_SERIALIZED) != F_SERIALIZED) {
                    if (primitiveAsString || asString) {
                        // pulling out string value
                        if (log.isInfoEnabled())
                            log.info("++++ retrieving object and stuffing into a string.");
                        o = new String(buf, defaultEncoding);
                    } else {
                        // decoding object
                        try {
                            o = NativeHandler.decode(buf, flag);
                        } catch (Exception e) {

                            // if we have an errorHandler, use its hook
                            if (errorHandler != null)
                                errorHandler.handleErrorOnGet(this, e, key);

                            log.error("++++ Exception thrown while trying to deserialize for key: " + key + " -- "
                                    + e.getMessage());
                            throw new NestedIOException(e);
                        }
                    }
                } else {
                    // deserialize if the data is serialized
                    ContextObjectInputStream ois = new ContextObjectInputStream(new ByteArrayInputStream(buf),
                            classLoader);
                    try {
                        o = ois.readObject();
                        if (log.isInfoEnabled())
                            log.info("++++ deserializing " + o.getClass());
                    } catch (InvalidClassException e) {
                        /*
                         * Errors de-serializing are to be expected in the case
                         * of a long running server that spans client restarts
                         * with updated classes.
                         */
                        // if we have an errorHandler, use its hook
                        if (errorHandler != null)
                            errorHandler.handleErrorOnGet(this, e, key);

                        o = null;
                        log.error("++++ InvalidClassException thrown while trying to deserialize for key: " + key
                                + " -- " + e.getMessage());
                    } catch (ClassNotFoundException e) {

                        // if we have an errorHandler, use its hook
                        if (errorHandler != null)
                            errorHandler.handleErrorOnGet(this, e, key);

                        o = null;
                        log.error("++++ ClassNotFoundException thrown while trying to deserialize for key: " + key
                                + " -- " + e.getMessage());
                    } finally {
                        if (ois != null) {
                            ois.close();
                        }
                    }
                }

                // store the object into the cache
                if (o != null) {
                    if (casToken.isPresent()) {
                        CASValue val = new CASValue(o, casToken.get());
                        hm.put(key, val);
                    } else {
                        hm.put(key, o);
                    }

                }
            } else if (END.equals(line)) {
                if (log.isDebugEnabled())
                    log.debug("++++ finished reading from cache server");
                break;
            }
        }
    }

    private String sanitizeKey(String key) throws UnsupportedEncodingException {
        return (sanitizeKeys) ? URLEncoder.encode(key, "UTF-8") : key;
    }

    /**
     * Invalidates the entire cache.
     *
     * Will return true only if succeeds in clearing all servers.
     * 
     * @return success true/false
     */
    public boolean flushAll() {
        return flushAll(null);
    }

    /**
     * Invalidates the entire cache.
     *
     * Will return true only if succeeds in clearing all servers. If pass in
     * null, then will try to flush all servers.
     * 
     * @param servers
     *            optional array of host(s) to flush (host:port)
     * @return success true/false
     */
    public boolean flushAll(String[] servers) {

        // get SockIOPool instance
        // return false if unable to get SockIO obj
        if (pool == null) {
            log.error("++++ unable to get SockIOPool instance");
            return false;
        }

        // get all servers and iterate over them
        servers = (servers == null) ? pool.getServers() : servers;

        // if no servers, then return early
        if (servers == null || servers.length <= 0) {
            log.error("++++ no servers to flush");
            return false;
        }

        boolean success = true;

        for (int i = 0; i < servers.length; i++) {

            SockIOPool.SockIO sock = pool.getConnection(servers[i]);
            if (sock == null) {
                log.error("++++ unable to get connection to : " + servers[i]);
                success = false;
                if (errorHandler != null)
                    errorHandler.handleErrorOnFlush(this, new IOException("no socket to server available"));
                continue;
            }

            // build command
            String command = "flush_all\r\n";

            try {
                sock.write(command.getBytes());
                sock.flush();

                // if we get appropriate response back, then we return true
                String line = sock.readLine();
                success = (OK.equals(line)) ? success && true : false;
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnFlush(this, e);

                // exception thrown
                log.error("++++ exception thrown while writing bytes to server on flushAll");
                log.error(e.getMessage(), e);

                try {
                    sock.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + sock.toString());
                }

                success = false;
                sock = null;
            }

            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return success;
    }

    /**
     * Retrieves stats for all servers.
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains stats with stat name as key and value as value.
     * 
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map stats() {
        return stats(null);
    }

    /**
     * Retrieves stats for passed in servers (or all servers).
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains stats with stat name as key and value as value.
     * 
     * @param servers
     *            string array of servers to retrieve stats from, or all if this
     *            is null
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map stats(String[] servers) {
        return stats(servers, "stats\r\n", STATS);
    }

    /**
     * Retrieves stats items for all servers.
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains item stats with itemname:number:field as key and value as value.
     * 
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsItems() {
        return statsItems(null);
    }

    /**
     * Retrieves stats for passed in servers (or all servers).
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains item stats with itemname:number:field as key and value as value.
     * 
     * @param servers
     *            string array of servers to retrieve stats from, or all if this
     *            is null
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsItems(String[] servers) {
        return stats(servers, "stats items\r\n", STATS);
    }

    /**
     * Retrieves stats items for all servers.
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains slabs stats with slabnumber:field as key and value as value.
     * 
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsSlabs() {
        return statsSlabs(null);
    }

    /**
     * Retrieves stats for passed in servers (or all servers).
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains slabs stats with slabnumber:field as key and value as value.
     * 
     * @param servers
     *            string array of servers to retrieve stats from, or all if this
     *            is null
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsSlabs(String[] servers) {
        return stats(servers, "stats slabs\r\n", STATS);
    }

    /**
     * Retrieves items cachedump for all servers.
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains cachedump stats with the cachekey as key and byte size and unix
     * timestamp as value.
     * 
     * @param slabNumber
     *            the item number of the cache dump
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsCacheDump(int slabNumber, int limit) {
        return statsCacheDump(null, slabNumber, limit);
    }

    /**
     * Retrieves stats for passed in servers (or all servers).
     *
     * Returns a map keyed on the servername. The value is another map which
     * contains cachedump stats with the cachekey as key and byte size and unix
     * timestamp as value.
     * 
     * @param servers
     *            string array of servers to retrieve stats from, or all if this
     *            is null
     * @param slabNumber
     *            the item number of the cache dump
     * @return Stats map
     */
    @SuppressWarnings("rawtypes")
    public Map statsCacheDump(String[] servers, int slabNumber, int limit) {
        return stats(servers, String.format("stats cachedump %d %d\r\n", slabNumber, limit), ITEM);
    }

    @SuppressWarnings("rawtypes")
    private Map stats(String[] servers, String command, String lineStart) {

        if (command == null || command.trim().equals("")) {
            log.error("++++ invalid / missing command for stats()");
            return null;
        }

        // get all servers and iterate over them
        servers = (servers == null) ? pool.getServers() : servers;

        // if no servers, then return early
        if (servers == null || servers.length <= 0) {
            log.error("++++ no servers to check stats");
            return null;
        }

        // array of stats Maps
        Map<String, Map> statsMaps = new HashMap<String, Map>();

        for (int i = 0; i < servers.length; i++) {

            SockIOPool.SockIO sock = pool.getConnection(servers[i]);
            if (sock == null) {
                log.error("++++ unable to get connection to : " + servers[i]);
                if (errorHandler != null)
                    errorHandler.handleErrorOnStats(this, new IOException("no socket to server available"));
                continue;
            }

            // build command
            try {
                sock.write(command.getBytes());
                sock.flush();

                // map to hold key value pairs
                Map<String, String> stats = new HashMap<String, String>();

                // loop over results
                while (true) {
                    String line = sock.readLine();
                    if (log.isDebugEnabled())
                        log.debug("++++ line: " + line);

                    if (line.startsWith(lineStart)) {
                        String[] info = line.split(" ", 3);
                        String key = info[1];
                        String value = info[2];

                        if (log.isDebugEnabled()) {
                            log.debug("++++ key  : " + key);
                            log.debug("++++ value: " + value);
                        }

                        stats.put(key, value);
                    } else if (END.equals(line)) {
                        // finish when we get end from server
                        if (log.isDebugEnabled())
                            log.debug("++++ finished reading from cache server");
                        break;
                    } else if (line.startsWith(ERROR) || line.startsWith(CLIENT_ERROR)
                            || line.startsWith(SERVER_ERROR)) {
                        log.error("++++ failed to query stats");
                        log.error("++++ server response: " + line);
                        break;
                    }

                    statsMaps.put(servers[i], stats);
                }
            } catch (IOException e) {

                // if we have an errorHandler, use its hook
                if (errorHandler != null)
                    errorHandler.handleErrorOnStats(this, e);

                // exception thrown
                log.error("++++ exception thrown while writing bytes to server on stats");
                log.error(e.getMessage(), e);

                try {
                    sock.trueClose();
                } catch (IOException ioe) {
                    log.error("++++ failed to close socket : " + sock.toString());
                }

                sock = null;
            }

            if (sock != null) {
                sock.close();
                sock = null;
            }
        }

        return statsMaps;
    }

    protected final class NIOLoader {
        protected Selector selector;
        protected int numConns = 0;
        protected MemcachedClient mc;
        protected Connection[] conns;

        public NIOLoader(MemcachedClient mc) {
            this.mc = mc;
        }

        private final class Connection {

            public List<ByteBuffer> incoming = new ArrayList<ByteBuffer>();
            public ByteBuffer outgoing;
            public SockIOPool.SockIO sock;
            public SocketChannel channel;
            private boolean isDone = false;

            public Connection(SockIOPool.SockIO sock, StringBuilder request) throws IOException {
                if (log.isDebugEnabled())
                    log.debug("setting up connection to " + sock.getHost());

                this.sock = sock;
                outgoing = ByteBuffer.wrap(request.append("\r\n").toString().getBytes());

                channel = sock.getChannel();
                if (channel == null)
                    throw new IOException("dead connection to: " + sock.getHost());

                channel.configureBlocking(false);
                channel.register(selector, SelectionKey.OP_WRITE, this);
            }

            public void close() {
                try {
                    if (isDone) {
                        // turn off non-blocking IO and return to pool
                        if (log.isDebugEnabled())
                            log.debug("++++ gracefully closing connection to " + sock.getHost());

                        channel.configureBlocking(true);
                        sock.close();
                        return;
                    }
                } catch (IOException e) {
                    log.warn("++++ memcache: unexpected error closing normally");
                }

                try {
                    if (log.isDebugEnabled())
                        log.debug("forcefully closing connection to " + sock.getHost());

                    channel.close();
                    sock.trueClose();
                } catch (IOException ignoreMe) {
                }
            }

            public boolean isDone() {
                // if we know we're done, just say so
                if (isDone)
                    return true;

                // else find out the hard way
                int strPos = B_END.length - 1;

                int bi = incoming.size() - 1;
                while (bi >= 0 && strPos >= 0) {
                    ByteBuffer buf = incoming.get(bi);
                    int pos = buf.position() - 1;
                    while (pos >= 0 && strPos >= 0) {
                        if (buf.get(pos--) != B_END[strPos--])
                            return false;
                    }

                    bi--;
                }

                isDone = strPos < 0;
                return isDone;
            }

            public ByteBuffer getBuffer() {
                int last = incoming.size() - 1;
                if (last >= 0 && incoming.get(last).hasRemaining()) {
                    return incoming.get(last);
                } else {
                    ByteBuffer newBuf = ByteBuffer.allocate(8192);
                    incoming.add(newBuf);
                    return newBuf;
                }
            }

            public String toString() {
                return "Connection to " + sock.getHost() + " with " + incoming.size() + " bufs; done is " + isDone;
            }
        }

        public void doMulti(boolean asString, Map<String, StringBuilder> sockKeys, String[] keys,
                Map<String, Object> ret, boolean isGets) {

            long timeRemaining = 0;
            try {
                selector = Selector.open();

                // get the sockets, flip them to non-blocking, and set up data
                // structures
                conns = new Connection[sockKeys.keySet().size()];
                numConns = 0;
                for (Iterator<String> i = sockKeys.keySet().iterator(); i.hasNext();) {
                    // get SockIO obj from hostname
                    String host = i.next();

                    SockIOPool.SockIO sock = pool.getConnection(host);

                    if (sock == null) {
                        if (errorHandler != null)
                            errorHandler.handleErrorOnGet(this.mc, new IOException("no socket to server available"),
                                    keys);
                        return;
                    }

                    conns[numConns++] = new Connection(sock, sockKeys.get(host));
                }

                // the main select loop; ends when
                // 1) we've received data from all the servers, or
                // 2) we time out
                long startTime = System.currentTimeMillis();

                long timeout = pool.getMaxBusy();
                timeRemaining = timeout;

                while (numConns > 0 && timeRemaining > 0) {
                    int n = selector.select(Math.min(timeout, 5000));
                    if (n > 0) {
                        // we've got some activity; handle it
                        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                        while (it.hasNext()) {
                            SelectionKey key = it.next();
                            it.remove();
                            handleKey(key);
                        }
                    } else {
                        // timeout likely... better check
                        // TODO: This seems like a problem area that we need to
                        // figure out how to handle.
                        log.error("selector timed out waiting for activity");
                    }

                    timeRemaining = timeout - (System.currentTimeMillis() - startTime);
                }
            } catch (IOException e) {
                // errors can happen just about anywhere above, from
                // connection setup to any of the mechanics
                handleError(e, keys);
                return;
            } finally {
                if (log.isDebugEnabled())
                    log.debug("Disconnecting; numConns=" + numConns + "  timeRemaining=" + timeRemaining);

                // run through our conns and either return them to the pool
                // or forcibly close them
                try {
                    if (selector != null)
                        selector.close();
                } catch (IOException ignoreMe) {

                }

                for (Connection c : conns) {
                    if (c != null)
                        c.close();
                }
            }

            // Done! Build the list of results and return them. If we get
            // here by a timeout, then some of the connections are probably
            // not done. But we'll return what we've got...
            for (Connection c : conns) {
                try {
                    if (c.incoming.size() > 0 && c.isDone())
                        loadMulti(new ByteBufArrayInputStream(c.incoming), ret, asString, isGets);
                } catch (Exception e) {
                    // shouldn't happen; we have all the data already
                    log.warn("Caught the aforementioned exception on " + c);
                }
            }
        }

        private void handleError(Throwable e, String[] keys) {
            // if we have an errorHandler, use its hook
            if (errorHandler != null)
                errorHandler.handleErrorOnGet(MemcachedClient.this, e, keys);

            // exception thrown
            log.error("++++ exception thrown while getting from cache on getMulti");
            log.error(e.getMessage());
        }

        private void handleKey(SelectionKey key) throws IOException {
            if (log.isDebugEnabled())
                log.debug("handling selector op " + key.readyOps() + " for key " + key);

            if (key.isReadable())
                readResponse(key);
            else if (key.isWritable())
                writeRequest(key);
        }

        public void writeRequest(SelectionKey key) throws IOException {
            ByteBuffer buf = ((Connection) key.attachment()).outgoing;
            SocketChannel sc = (SocketChannel) key.channel();

            if (buf.hasRemaining()) {
                if (log.isDebugEnabled())
                    log.debug("writing " + buf.remaining() + "B to "
                            + ((SocketChannel) key.channel()).socket().getInetAddress());

                sc.write(buf);
            }

            if (!buf.hasRemaining()) {
                if (log.isDebugEnabled())
                    log.debug("switching to read mode for server "
                            + ((SocketChannel) key.channel()).socket().getInetAddress());

                key.interestOps(SelectionKey.OP_READ);
            }
        }

        public void readResponse(SelectionKey key) throws IOException {
            Connection conn = (Connection) key.attachment();
            ByteBuffer buf = conn.getBuffer();
            int count = conn.channel.read(buf);
            if (count > 0) {
                if (log.isDebugEnabled())
                    log.debug("read  " + count + " from " + conn.channel.socket().getInetAddress());

                if (conn.isDone()) {
                    if (log.isDebugEnabled())
                        log.debug("connection done to  " + conn.channel.socket().getInetAddress());

                    key.cancel();
                    numConns--;
                    return;
                }
            }
        }
    }

    public void cleanup() {
        this.i_lease_list.clear();
        this.q_lease_list.clear();
    }
}
