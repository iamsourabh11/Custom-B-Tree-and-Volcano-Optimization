package storage;

import java.util.ArrayList;
import java.util.List;

/*
    * A DB is simply a collection of files.
    * Files could be of 2 types - Relational Files or Index Trees
 */
public class DB {
    
    // This is private :)
    private List<AbstractFile<? extends AbstractBlock>> files;

    public DB() {
        files = new ArrayList<>();
    }

    public int addFile(AbstractFile<? extends AbstractBlock> file) {

        files.add(file);
        return files.size() - 1;

    }
    
    public byte[] get_data(int file_id, int block_id, int offset, int length){
        if(file_id >= files.size()){
            return null;
        }
        return files.get(file_id).get_data(block_id, offset, length);
    }

    public byte[] get_data(int file_id, int block_id){
        if(file_id >= files.size()){
            return null;
        }
        return files.get(file_id).get_data(block_id);
    }

    // only applicable for relational files
    public int get_num_records(int file_id){
        if(file_id >= files.size()){
            return -1;
        }
        AbstractFile<? extends AbstractBlock> file = files.get(file_id);
        if(file instanceof File){
            return ((File) file).get_num_records();
        }
        return -1;
    }

    public void write_data(int file_id, int block_id, int offset, byte[] data){
        if(file_id >= files.size()){
            return;
        }
        files.get(file_id).write_data(block_id, offset, data);
        return;
    }
    
}
