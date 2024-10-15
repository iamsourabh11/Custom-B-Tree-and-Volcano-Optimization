package storage;

import java.util.ArrayList;
import java.util.List;

// Abstract File class - will be extended by record file
public abstract class AbstractFile <T extends AbstractBlock> {
    
    protected List<T> blocks;

    public AbstractFile(List<T> blocks) {
        this.blocks = blocks;
    }

    public AbstractFile() {
        blocks = new ArrayList<>();
    }

    public void add_block(T block) {
        blocks.add(block);
    }

    public byte[] get_data(int block_id){
        if(block_id >= blocks.size()){
            return null;
        }
        return blocks.get(block_id).get_data();
    }

    public byte[] get_data(int block_id, int offset, int length){
        if(block_id >= blocks.size()){
            return null;
        }
        return blocks.get(block_id).get_data(offset, length);
    }

    public void write_data(int block_id, int offset, byte[] data){
        if(block_id >= blocks.size()){
            return;
        }
        blocks.get(block_id).write_data(offset, data);
    }
}
