package org.neo4j.graphdb;

import org.neo4j.annotations.api.PublicApi;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;



@PublicApi
public class WCSketch {
    public String filePath;
    public int length;
    public int relationNum;
    public int recordNum;
    private int[] seeds;
    private static APHash[] hashes;
    private static Random random = new Random();
    private final int ROW = 0;
    private final int COL = 2;
    private final int MINN = 2000000;
    private final int MAXX = -1;
    public WCBucket[][] buckets;
    public ArrayList<WCRelation> relations;
    public ArrayList<WCRecord> records;

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

    public WCSketch(int length){
        this.length = length;
        relationNum = 0;
        recordNum = 0;
        seeds = new int[4];
        hashes = new APHash[4];
        for(int i = 0; i < 4; ++i){
            seeds[i] = random.nextInt();
            hashes[i].setSeed(seeds[i]);
        }
        buckets = new WCBucket[length][length];

        relations = new ArrayList<>();
        records = new ArrayList<>();
    }

    public void insert(int srcID, int dstID, WCRecord record){
        records.add(record);
        int newRec = recordNum;
        recordNum++;
        String src = String.valueOf(srcID);
        String dst = String.valueOf(dstID);
        WCRelation res = queryRel(srcID, dstID);
        if(res != null){
            record.nextRec = res.nextRec;
            res.nextRec = newRec;
            return;
        }
        int[] row = new int[2];
        int[] col = new int[2];
        for(int i = 0; i < 1; ++i){
            row[i] = hashes[ROW + i].hash(src);
            col[i] = hashes[COL + i].hash(dst);
        }
        WCBucket tempBuc = buckets[row[0]][col[0]];
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
        WCRelation relation = new WCRelation(srcID, dstID);
        relations.add(relation);
        relation.nextRel = tempBuc.pointer_h;
        relation.nextRec = newRec;
        tempBuc.pointer_h = relationNum;
        relationNum++;
        tempBuc.counter++;
    }

    public WCRelation queryRel(int srcID, int dstID){
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
                WCRelation temp = relations.get(buckets[row[i]][col[j]].pointer_h);
                WCRelation result = find(srcID, dstID, temp);
                if(result != null)
                    return result;
            }
        }
        return null;
    }

    public WCRelation find(int srcID, int dstID, WCRelation header){
        for(; header != null; header = relations.get(header.nextRel)){
            if(srcID == header.srcID && dstID == header.dstID){
                return header;
            }
        }
        return null;
    }

    public WCRecord queryRec(String src, String dst, int recID){
        return null;
    }

    private static class WCBucket{
        public int counter;
        public int pointer_h;
        //public int pointer_t;

        public WCBucket(){
            counter = 0;
            pointer_h = -1;
        }
    }

    public static class WCRelation{
        public int srcID;
        public int dstID;
        public int nextRel;
        public int nextRec;
        public WCRelation(int srcID, int dstID){
            this.srcID = srcID;
            this.dstID = dstID;
            nextRel = -1;
            nextRec = -1;
        }
    }

    public static class WCRecord{
        public int relID;
        public int nextRec;

        public WCRecord(int relID){
            this.relID = relID;
            nextRec = -1;
        }
    }

    public static int CLHtoInt(char[] c){
        int res = 0;
        for(int i = 0; i < c.length; ++i){
            res += (c[i] & 0xff) << (8 * i);
        }
        return res;
    }

    public static char[] CtoLH(int n){
        char[] c = new char[4];
        c[0] = (char) (n & 0xff);
        c[1] = (char) (n >> 8 & 0xff);
        c[2] = (char) (n >> 16 & 0xff);
        c[3] = (char) (n >> 24 & 0xff);
        return c;
    }

    public static WCRecord buildWCRecord(int relationshipID){
        return new WCRecord(relationshipID);
    }

    public static WCSketch loadFromFile(File file){
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(fis);
            char[] tempBuf = new char[4];
            isr.read(tempBuf);
            int len = CLHtoInt(tempBuf);
            isr.read(tempBuf);
            int relNum = CLHtoInt(tempBuf);
            isr.read(tempBuf);
            int recNum = CLHtoInt(tempBuf);
            WCSketch wcSketch = new WCSketch(len);
            wcSketch.relationNum = relNum;
            wcSketch.recordNum = recNum;
            wcSketch.filePath = file.getPath();
            for(int i = 0; i < len; ++i){
                for(int j = 0; j < len; ++j){
                    isr.read(tempBuf);
                    wcSketch.buckets[i][j].counter = CLHtoInt(tempBuf);
                    isr.read(tempBuf);
                    wcSketch.buckets[i][j].pointer_h = CLHtoInt(tempBuf);
                }
            }
            for(int i = 0; i < relNum; ++i){
                isr.read(tempBuf);
                int srcID = CLHtoInt(tempBuf);
                isr.read(tempBuf);
                int dstID = CLHtoInt(tempBuf);
                WCRelation tempRel = new WCRelation(srcID, dstID);
                isr.read(tempBuf);
                tempRel.nextRel = CLHtoInt(tempBuf);
                isr.read(tempBuf);
                tempRel.nextRec = CLHtoInt(tempBuf);
                wcSketch.relations.add(tempRel);
            }
            for(int i = 0; i < recNum; ++i){
                isr.read(tempBuf);
                int relID = CLHtoInt(tempBuf);
                WCRecord tempRec = new WCRecord(relID);
                isr.read(tempBuf);
                tempRec.nextRec = CLHtoInt(tempBuf);
                wcSketch.records.add(tempRec);
            }
            isr.close();
            return wcSketch;
        } catch (Exception e){
            e.printStackTrace();
            //log.error(e.getMessage());
        }
        return null;
    }
    public static void saveToFile(File file, WCSketch wcSketch){
        try {
            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            osw.write(CtoLH(wcSketch.length));
            osw.write(CtoLH(wcSketch.relationNum));
            osw.write(CtoLH(wcSketch.recordNum));
            for(int i = 0; i < wcSketch.length; ++i){
                for(int j = 0; j < wcSketch.length; ++j){
                    osw.write(CtoLH(wcSketch.buckets[i][j].counter));
                    osw.write(CtoLH(wcSketch.buckets[i][j].pointer_h));
                }
            }
            for(int i = 0; i < wcSketch.relationNum; ++i){
                osw.write(CtoLH(wcSketch.relations.get(i).srcID));
                osw.write(CtoLH(wcSketch.relations.get(i).dstID));
                osw.write(CtoLH(wcSketch.relations.get(i).nextRel));
                osw.write(CtoLH(wcSketch.relations.get(i).nextRec));
            }
            for(int i = 0; i < wcSketch.recordNum; ++i){
                osw.write(CtoLH(wcSketch.records.get(i).relID));
                osw.write(CtoLH(wcSketch.records.get(i).nextRec));
            }
            osw.close();
        } catch (Exception e){
          e.printStackTrace();
        }
    }
}


