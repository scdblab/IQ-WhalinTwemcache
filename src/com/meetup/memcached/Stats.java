package com.meetup.memcached;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class Stats {
    Map<String, StatItem> map;
    
    static List<Stats> stats = new ArrayList<>();
    static Semaphore sm = new Semaphore(1);
    
    private Stats() {
        map = new HashMap<>();
    }
    
    public static Stats getStats() {
        Stats s = null;
        try {
            sm.acquire();
            s = new Stats();
            stats.add(s);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            sm.release();
        }
        return s;
    }

    void add(String metric, long val) {
        StatItem item = map.get(metric);
        if (item == null) {
            item = new StatItem();
            map.put(metric, item);
        }
        item.cnt++;
        item.total += val;
    }
    
    public static void aggregate() {
        Map<String, StatItem> total = new TreeMap<>();
        
        for (Stats s : stats) {
            for (String metric: s.map.keySet()) {
                StatItem item = s.map.get(metric);
                StatItem itemTotal = total.get(metric);
                if (itemTotal == null) {
                    itemTotal = new StatItem();
                    total.put(metric, itemTotal);
                }
                itemTotal.merge(item);
            }
        }
        
        for (String metric: total.keySet()) {
            StatItem item = total.get(metric);
            double avr = item.cnt == 0 ? 0 : item.total / (double)item.cnt;
            System.out.println(String.format("Stat %s : %s", metric, avr));
        }
    }
}

class StatItem {
    int cnt = 0;
    long total = 0;
    
    public void merge(StatItem item) {
        if (item == null) return;
        cnt = cnt + item.cnt;
        total = total + item.total;
    }
}


