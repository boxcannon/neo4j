package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Random;

public class WCSketchOpt {
    public int length;
    private int[] seeds;
    private static APHash[] hashes;
    private static Random random = new Random();
    private final int ROW = 0;
    private final int COL = 2;
    private final int MINN = 2000000;
    private final int MAXX = -1;
    public InternalTransaction tx;
    public WCOBucket[][] buckets;

    private class APHash {
        public int seed;

        public void setSeed(int seed){ this.seed = seed;}

        public int hash(String key) {
            int hash = seed;
            int n = key.length();
            for (int i = 0; i < n; i++) {
                if ((i & 1) == 0) {
                    hash ^= ((hash << 7) ^ key.charAt(i) ^ (hash >> 3));
                } else {
                    hash ^= (~((hash << 11) ^ key.charAt(i) ^ (hash >> 5)));
                }
            }
            return (hash & 0x7FFFFFFF);
        }
    }

    public WCSketchOpt(int length, InternalTransaction tx){
        this.tx = tx;
        this.length = length;
        seeds = new int[4];
        hashes = new APHash[4];
        for(int i = 0; i < 4; ++i){
            seeds[i] = random.nextInt();
            hashes[i].setSeed(seeds[i]);
        }
        buckets = new WCOBucket[length][length];
    }

    public void insert(int srcID, int dstID, int relationshipReference){
        String src = String.valueOf(srcID);
        String dst = String.valueOf(dstID);
        int[] row = new int[2];
        int[] col = new int[2];
        for(int i = 0; i < 1; ++i){
            row[i] = hashes[ROW + i].hash(src);
            col[i] = hashes[COL + i].hash(dst);
        }
        WCOBucket tempBuc = buckets[row[0]][col[0]];
        int minn = MINN;
        for(int i = 0; i < 1; ++i){
            for(int j = 0; j < 1; ++j){
                int temp = buckets[row[i]][col[j]].counter;
                if(temp < minn){
                    minn = temp;
                    tempBuc = buckets[row[i]][col[j]];
                }
            }
        }

        //增添relationship内部指针修改
        tempBuc.counter++;
        updateWCPointer(tempBuc.pointer_h, relationshipReference);
        tempBuc.pointer_h = relationshipReference;

    }

    public void updateWCPointer(int oldP, int newP){
        Relationship oldR = tx.getRelationshipById(oldP);
        Relationship newR = tx.getRelationshipById(newP);
    }

    public Relationship queryRecent(int srcID, int dstID){
        String src = String.valueOf(srcID);
        String dst = String.valueOf(dstID);
        int[] row = new int[2];
        int[] col = new int[2];
        for(int i = 0; i < 1; ++i){
            row[i] = hashes[ROW + i].hash(src);
            col[i] = hashes[COL + i].hash(dst);
        }

        for(int i = 0; i < 1; ++i){
            for(int j = 0; j < 1; ++j){

            }
        }
        return null;
    }

    public Relationship queryAll(int srcID, int dstID){
        String src = String.valueOf(srcID);
        String dst = String.valueOf(dstID);
        int[] row = new int[2];
        int[] col = new int[2];
        for(int i = 0; i < 1; ++i){
            row[i] = hashes[ROW + i].hash(src);
            col[i] = hashes[COL + i].hash(dst);
        }

        for(int i = 0; i < 1; ++i){
            for(int j = 0; j < 1; ++j){

            }
        }
        return null;
    }


    private class WCOBucket {
        public int counter;
        public int pointer_h;
        //public int pointer_t;

        public WCOBucket(){
            counter = 0;
            pointer_h = -1;
        }
    }

}
