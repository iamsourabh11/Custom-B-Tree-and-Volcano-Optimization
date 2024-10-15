package storage;

// Abstract Block class - 4kB fixed size blocks
// will be extended by record file block

public abstract class AbstractBlock {
    
    protected static final int block_capacity = 4096; // 4KB fixed size blocks
    protected byte[] data;

    protected AbstractBlock(byte[] data) {
        this.data = new byte[block_capacity];
        
        // if data is larger than block_capacity, only copy block_capacity bytes
        int bytes_to_copy = Math.min(data.length, block_capacity);
        System.arraycopy(data, 0, this.data, 0, bytes_to_copy);
        return;
    }

    protected AbstractBlock(){
        this.data = new byte[block_capacity];
    }

    public int get_block_capacity() {
        return block_capacity;
    }

    public byte[] get_data() {
        return data;
    }

    public byte[] get_data(int offset, int length) {
        if(offset + length > data.length){
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, length);
        return result;
    }

    public void write_data(int offset, byte[] data_to_write){
        if(offset + data_to_write.length > block_capacity){
            return;
        }
        System.arraycopy(data_to_write, 0, data, offset, data_to_write.length);
        return;
    }
}
