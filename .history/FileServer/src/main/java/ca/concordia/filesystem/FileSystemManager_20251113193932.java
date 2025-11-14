package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;




public class FileSystemManager {

    private final int MAXFILES = 5; //Number of FEntry slots
    private final int MAXBLOCKS = 10; //number of blocks
    private final static FileSystemManager instance;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size
    private static final int FEntry_size = 15; //11 name + 2 size + 2 first block
    private static final int FNode_size = 4; //2 block index + 2 for  nextBlock

    private final int entryoffset;
    private final int nodeoffset;
    private final int metadatabytes;
    private final int metadatablocks;

    private FEntry[] inodeTable = new FEntry[MAXFILES]; // Array of inodes
    private final short [] fnodeBlockIndex = new short[MAXBLOCKS];
    private final short[] fnodeNext = new short[MAXBLOCKS];
    private boolean[] freeBlockList = new boolean[MAXBLOCKS]; // Bitmap for free blocks
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    public FileSystemManager(String filename, int totalSize) throws IOException {
        // Initialize the file system manager with a file
        this.entryoffset = 0;
        this.nodeoffset = MAXFILES * FEntry_size;
        this.metadatabytes = MAXFILES * FEntry_size + MAXBLOCKS * FNode_size;
        this.metadatablocks = (metadatabytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        this.disk = new RandomAccessFile(filename, "rw");

        long expectedsize = (long) MAXBLOCKS * BLOCK_SIZE;
        long currentsize = disk.length();

        if(currentsize == 0) {
            disk.setLength(expectedsize);
            initializeEmptyFileSystemOnDisk();
        }
        else if (currentsize < expectedsize){
            disk.setLength(expectedsize);
            loadmetadataFromDisk();
        }
        else{
            loadmetadataFromDisk();
        }
    }


    public void createfile(String filename) throws Exception {
        rwLock.writeLock().lock();
        try {
            validateFileName(filename);
            if (findFileIndex(filename) != -1) {
                throw new Exception("File Already exists");
            }

            int freeindex = findfreeFEntryIndex();
            if (freeindex == -1) {
                throw new Exception("No Free FEntry available");
            }
            
            FEntry entry = new FEntry(filename, (short) 0, (short) -1);
            inodeTable[freeindex] = entry;
            writeFEntryToDisk(freeindex, entry);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }
    
    public void deletefile(String filename) throws Exception {
        rwLock.writeLock().lock();
        try{
            int index = findFileIndex(filename);
            if(index==-1){
                throw new Exception("file does not Exist");
            }
            FEntry entry = inodeTable[index];
            short firstblock = entry.getFirstBlock();

            int current = firstblock;
            while (current >= 0 && current < MAXBLOCKS) {
                int next = fnodeNext[current];
                fnodeBlockIndex[current] = (short) -current;
                fnodeNext[current] = -1;
                freeBlockList[current] = (current >= metadatablocks);

                writeFNodeToDisk(current);
                if (current >= metadatablocks) zeroDataBlock(current);
                current = next;
            }
            inodeTable[index] = null;
            writeEmptyFEntryToDisk(index);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }


    public void writeFile(String filename, byte[] contents) throws Exception {
        rwLock.writeLock().lock();
        try{
            int index = findFileIndex(filename);
            if (index==-1){
                throw new Exception("file does not exist");
            }

            FEntry entry = inodeTable[index];
            int filesize = contents.length;
            int blocksneeded = (filesize==0) ? 0 : ((filesize + BLOCK_SIZE -1)/ BLOCK_SIZE);

            List<Integer> oldchain = getBlockChain(entry);
            int oldblocks = oldchain.size();

            int freeblocks = 0;
            for (int i = metadatablocks; i < MAXBLOCKS; i++){
                if (freeBlockList[i]) freeblocks++;
            }

            List<Integer> newchain = new ArrayList<>();
            int needed = blocksneeded;

            int recycleblocks = Math.min(needed, oldblocks);
            for (int i = 0; i < recycleblocks; i++){
                newchain.add(oldchain.get(i));
            }

            needed -= recycleblocks;
            if (needed > 0){
                for (int i = metadatablocks; i < MAXBLOCKS && needed > 0; i++){
                    if (freeblocklist[i]){
                        newchain.add(i);
                        needed--;
                    }
                }
            }

            if (newchain.size() != blocksneeded) throw new Exception("Internal error: could not allocate enough blocks");
            for (int i = 0; i < newchain.size(); i++){
                int index_node = newchain.get(i);
                fnodeBlockIndex[index_node] = (short) index_node;
                int index_next = (i==newchain.size() -1) ? -1 : newchain.get(i+1);
                fnodeNext[index_node] = (short) index_next;
                freeblocksList[index_node] = false;
                writeFNodeToDisk(index_node);
            }

            for (int i = recycleblocks; i < oldblocks; i++){
                int index_node = oldchain.get(i);
                fnodeBlockIndex[index_node] = (short) -index_node;
                fnodeNext[index_node] = -1;
                freeBlockList[index_node] = (index_node >= metadatablocks);
                writeFNodeToDisk(index_node);
                if (index_node >= metadatablocks) zeroDataBlock(index_node);
            }
            
        }
    }
}

        
//test



 /*       if(instance == null) {
            //TODO Initialize the file system
        } else {
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }

    }

    public void createFile(String fileName) throws Exception {
        // TODO
        throw new UnsupportedOperationException("Method not implemented yet.");
    }


    // TODO: Add readFile, writeFile and other required methods,
} */
