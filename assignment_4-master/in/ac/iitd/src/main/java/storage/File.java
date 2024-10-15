package storage;

/*
    * A File is simply a collection of blocks
    * The 0th block in the file is always the metadata block (contains schema)
    * For structure of schema block and data block, follow the assignment doc
 */

public class File extends AbstractFile<Block> {

    public boolean add_record_to_last_block(byte[] bytes){

        // if file has only one block - the metadata block, return false
        if(blocks.size() == 1){
            return false;
        }

        Block lastBlock = blocks.get(blocks.size() - 1);

        // first 2 bytes in this block are the number of records in the block
        byte[] numRecordsBytes = lastBlock.get_data(0, 2);
        int numRecords = (numRecordsBytes[0] << 8) | (numRecordsBytes[1] & 0xFF);

        // read the offset of the last record in the block
        byte[] offsetBytes = lastBlock.get_data(2 + (numRecords - 1) * 2, 2);
        int offset = (offsetBytes[0] << 8) | (offsetBytes[1] & 0xFF);

        int usedBytes = 2 + 2 * numRecords + (lastBlock.get_block_capacity() - offset);
        // calculate length of free bytes in the block
        int freeBytes = lastBlock.get_block_capacity() - usedBytes;

        // if the record is larger than the free bytes in the block, return false
        if(bytes.length + 2 > freeBytes){
            return false;
        }

        // add the record to the block
        int new_offset = offset - bytes.length;

        // convert the new offset to 2 bytes
        byte[] new_offset_bytes = new byte[2];
        new_offset_bytes[0] = (byte) (new_offset >> 8);
        new_offset_bytes[1] = (byte) new_offset;
        lastBlock.write_data(2 + 2 * numRecords, new_offset_bytes);
        lastBlock.write_data(new_offset, bytes);

        numRecords++;
        byte[] new_num_records_bytes = new byte[2];
        new_num_records_bytes[0] = (byte) (numRecords >> 8);
        new_num_records_bytes[1] = (byte) numRecords;
        lastBlock.write_data(0, new_num_records_bytes);

        return true;
    }

    public boolean add_record_to_new_block(byte[] bytes){
        
        if(blocks.isEmpty()){
            return false;
        } // sanity check

        if(bytes.length + 4 > blocks.get(0).get_block_capacity()){
            return false;
        } // sanity check

        // create a new block, 
        Block newBlock = new Block();

        // write 1 to first 2 bytes
        byte[] numRecordsBytes = new byte[2];
        numRecordsBytes[0] = 0;
        numRecordsBytes[1] = 1;
        newBlock.write_data(0, numRecordsBytes);

        int offset = newBlock.get_block_capacity() - bytes.length;
        // convert the offset to 2 bytes
        byte[] offsetBytes = new byte[2];
        offsetBytes[0] = (byte) (offset >> 8);
        offsetBytes[1] = (byte) offset;
        newBlock.write_data(2, offsetBytes);
        newBlock.write_data(offset, bytes);
        blocks.add(newBlock);
        return true;
    }

    public int get_num_records(){
        if(blocks.isEmpty()){
            return -1;
        }
        int numRecords = 0;
        for(int i = 1; i < blocks.size(); i++){
            byte[] numRecordsBytes = blocks.get(i).get_data(0, 2);
            numRecords += (numRecordsBytes[0] << 8) | (numRecordsBytes[1] & 0xFF);
        }
        return numRecords;
    }
    
}
