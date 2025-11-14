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
    //private final static FileSystemManager instance;
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
    private boolean[] freeblocklist = new boolean[MAXBLOCKS]; // Bitmap for free blocks
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
            ini_empty_filesystem_OD();
        }
        else if (currentsize < expectedsize){
            disk.setLength(expectedsize);
            load_metadata_FD();
        }
        else{
            load_metadata_FD();
        }
    }


    public void createfile(String filename) throws Exception {
        rwLock.writeLock().lock();
        try {
            check_filename(filename);
            if (find_file_index(filename) != -1) {
                throw new Exception("File Already exists");
            }

            int freeindex = free_FEntry_index();
            if (freeindex == -1) {
                throw new Exception("No Free FEntry available");
            }
            
            FEntry entry = new FEntry(filename, (short) 0, (short) -1);
            inodeTable[freeindex] = entry;
            write_FEntry_OD(freeindex, entry);
        }

        finally {
            rwLock.writeLock().unlock();
        }
    }
    
    public void deletefile(String filename) throws Exception {
        rwLock.writeLock().lock();
        try{
            int index = find_file_index(filename);
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
                freeblocklist[current] = (current >= metadatablocks);

                write_FNode_OD(current);
                if (current >= metadatablocks) empty_data_block(current);
                current = next;
            }
            inodeTable[index] = null;
            write_empty_FEntry_OD(index);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }


    public void writeFile(String filename, byte[] contents) throws Exception {
        rwLock.writeLock().lock();
        try{
            int index = find_file_index(filename);
            if (index==-1){
                throw new Exception("file does not exist");
            }

            FEntry entry = inodeTable[index];
            int filesize = contents.length;
            int blocksneeded = (filesize==0) ? 0 : ((filesize + BLOCK_SIZE -1)/ BLOCK_SIZE);

            List<Integer> oldchain = get_block_chain(entry);
            int oldblocks = oldchain.size();

            int freeblocks = 0;
            for (int i = metadatablocks; i < MAXBLOCKS; i++){
                if (freeblocklist[i]) freeblocks++;
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
                freeblocklist[index_node] = false;
                write_FNode_OD(index_node);
            }

            for (int i = recycleblocks; i < oldblocks; i++){
                int index_node = oldchain.get(i);
                fnodeBlockIndex[index_node] = (short) -index_node;
                fnodeNext[index_node] = -1;
                freeblocklist[index_node] = (index_node >= metadatablocks);
                write_FNode_OD(index_node);
                if (index_node >= metadatablocks) empty_data_block(index_node);
            }

            int offset =0;
            for (int i=0; i<newchain.size(); i++){
                int index_node = newchain.get(i);
                int index_write = Math.min(BLOCK_SIZE, filesize - offset);
                write_data_block(index_node, contents, offset, index_write);
                offset += index_write;
            }

            short firstblock = newchain.isEmpty() ? (short) -1 : newchain.get(0).shortValue();
            entry.setFilesize((short) filesize);
            inodeTable[index] = new FEntry(entry.getFilename(), (short) filesize, firstblock);
            write_FEntry_OD(index, inodeTable[index]);
        }

        finally{
            rwLock.writeLock().unlock();
        }
    }



    public byte[] readfile(String filename) throws Exception {
        rwLock.readLock().lock();
        try{
            int index = find_file_index(filename);
            if (index == -1){
                throw new Exception("file does not exist");
            }

            FEntry entry = inodeTable[index];
            int filesize = entry.getFilesize();
            if (filesize<=0) return new byte[0];
            List<Integer> chain = get_block_chain(entry);
            byte[] result = new byte[filesize];
            int offset =0;

            for (int index_node : chain){
                int toread = Math.min(BLOCK_SIZE,  filesize - offset);
                if (toread <=0) break;
                read_data_block(index_node, result, offset, toread);
                offset += toread;
            }

            return result;
        }

        finally{
            rwLock.readLock().unlock();
        }
    }

    public String[] listFiles(){
        rwLock.readLock().lock();
        try{
            List<String> filenames = new ArrayList<>();
            for (FEntry entry : inodeTable) {
                if (entry != null) {
                    filenames.add(entry.getFilename());
                }
            }
            return filenames.toArray(new String[0]);
        }

        finally{
            rwLock.readLock().unlock();
        }
    }


    private void ini_empty_filesystem_OD() throws IOException { //OD => on disk
        for (int i=0; i<MAXFILES; i++){
            inodeTable[i] = null;
            write_empty_FEntry_OD(i);
        }

        for (int i=0; i < MAXBLOCKS; i++){
            if (i < metadatablocks){
                fnodeBlockIndex[i] = (short) i;
                fnodeNext[i] = -1;
                freeblocklist[i] = false;
            }

            else{
                fnodeBlockIndex[i] = (short) -i;
                fnodeNext[i] = -1;
                freeblocklist[i] = true;
            }
            write_FNode_OD(i);
        }
    }


    private void load_metadata_FD() throws IOException { //FD => from disk
        for (int i=0; i<MAXFILES; i++){
            inodeTable[i] = read_FEntry_FD(i);
        }

        for (int i=0; i<MAXBLOCKS; i++) {
            long pos = nodeoffset + (long) i * FNode_size;
            disk.seek(pos);
            short blockindex = disk.readShort();
            short nextblock = disk.readShort();
            fnodeBlockIndex[i] = blockindex;
            fnodeNext[i] = nextblock;

            boolean free = (blockindex < 0);
            freeblocklist[i] = free && (i >= metadatablocks);
        }
    }


    private void write_FEntry_OD(int index, FEntry entry) throws IOException {
        long pos = entryoffset + (long) index * FEntry_size;
        disk.seek(pos);
        byte[] name_byte = new byte[11];
        if(entry != null && entry.getFilename() != null){
            byte[] raw = entry.getFilename().getBytes(StandardCharsets.US_ASCII);
            int len = Math.min(raw.length, 11);
            System.arraycopy(raw, 0, name_byte, 0, len);
        }

        disk.write(name_byte);
        short filesize = (entry==null) ? 0 : entry.getFilesize();
        short firstblock = (entry==null) ? -1 : entry.getFirstBlock();
        disk.writeShort(filesize);
        disk.writeShort(firstblock);
    }



    private void write_empty_FEntry_OD(int index) throws IOException {
        long pos = entryoffset + (long) index * FEntry_size;
        disk.seek(pos);
        disk.write (new byte[11]); //for name
        disk.writeShort(0);
        disk.writeShort((short) -1);

    }


    private FEntry read_FEntry_FD(int index) throws IOException {
        long pos = entryoffset + (long) index * FEntry_size;
        disk.seek(pos);
        byte[] name_byte = new byte[11];
        disk.readFully(name_byte);
        short filesize = disk.readShort();
        short firstblock = disk.readShort();
        int namelen = 0;
        while (namelen < 11 && name_byte[namelen] != 0) { //make sure name is not already in use
            namelen++;
        }

        if (namelen == 0) return null;

        String filename = new String(name_byte, 0, namelen, StandardCharsets.US_ASCII);
        return new FEntry(filename, filesize, firstblock);

    }


    private void write_FNode_OD(int index) throws IOException {
        long pos = nodeoffset + (long) index * FNode_size;
        disk.seek(pos);
        disk.writeShort(fnodeBlockIndex[index]);
        disk.writeShort(fnodeNext[index]);
    }


    private void write_data_block (int index_block, byte[] src, int offset, int length) throws IOException {
        long pos = (long) index_block * BLOCK_SIZE;
        disk.seek(pos);
        disk.write(src, offset, length);
        if (length < BLOCK_SIZE) {
            disk.write (new byte[BLOCK_SIZE - length]);
        }
    }


    private void read_data_block (int index_block, byte[] dst, int offset, int length) throws IOException {
        long pos = (long) index_block * BLOCK_SIZE;
        disk.seek(pos);
        disk.write(new byte[BLOCK_SIZE]);
    }


    private void empty_data_block (int index_block) throws IOException { //erases the contents of the data block and changes it to zeroes
        long pos = (long) index_block * BLOCK_SIZE;
        disk.seek(pos);
        disk.write(new byte[BLOCK_SIZE]);
    }



    private void check_filename(String filename) throws Exception {
        if (filename==null || filename.isEmpty()){
            throw new Exception ("filename canot be emtpy");
        }

        if (filename.length() > 11){
            throw new Exception ("filename to long");

        }
    }



    private int find_file_index (String name){
        for (int i=0; i<MAXFILES; i++){
            if (inodeTable[i] != null && inodeTable[i].getFilename().equals(name)) return i;
        }

        return -1;
    }



    private int free_FEntry_index(){
        for (int i=0; i < MAXFILES; i++){
            if (inodeTable[i] == null) return i;
        }
        return -1;
    }



    private List<Integer> get_block_chain(FEntry entry){
        List<Integer> chain = new ArrayList<>();
        if (entry == null) return chain;

        int current = entry.getFirstBlock();
        while (current >= 0 && current < MAXBLOCKS){
            chain.add(current);
            int next = fnodeNext[current];
            if (next == current) break;

            current = next;
        }
        return chain;
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
