package manager;

import storage.DB;
import storage.File;
import storage.Block;
import utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.log4j.Logger;

import java.util.Iterator;

// DB362 Storage Manager
public class StorageManager {

    private static final StorageManager INSTANCE = new StorageManager();

    private static final Logger logger = Logger.getLogger(StorageManager.class);

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    // Note this change from A3 - this constructor is now private
    private StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    public static StorageManager getInstance() {
        return INSTANCE;
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        logger.trace("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        // assert(file_to_fileid.get(table_name) == null);

        // if already loaded file, then return
        if(file_to_fileid.get(table_name) != null) {
            return;
        }

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        logger.trace("Done writing file");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                    variable_length.add(1);
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                logger.error("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public List<Object[]> get_records_from_block(String table_name, int block_id){
        if(!check_file_exists(table_name)) {
            return null;
        }
        byte[] block = get_data_block(table_name, block_id);
        if(block == null) {
            return null;
        }

        // first 2 bytes are number of records
        int num_records = (block[0] << 8) | (block[1] & 0xFF);
        List<Object[]> records = new ArrayList<>();

        byte[] schema_block = db.get_data(file_to_fileid.get(table_name), 0);

        int num_columns = get_num_columns(schema_block);
        int fixed_num_columns = get_fixed_num_columns(schema_block);
        int nullbitmap_offset = get_nullbitmap_offset_in_record(schema_block);

        for(int i = 0 ; i < num_records ; i ++){
            int offset = (block[2 + 2 * i + 1] & 0xFF) | ((block[2 + 2 * i] & 0xFF) << 8);
            Object[] record = deserialize_record(block, offset, fixed_num_columns, num_columns - fixed_num_columns, nullbitmap_offset, schema_block);
            records.add(record);
        }
        return records;
    }

    private Object[] deserialize_record(byte[] block, int offset, int fixed_num_columns, int variable_num_columns, int nullbitmap_offset, byte[] schema_block){

        Object[] record = new Object[fixed_num_columns + variable_num_columns];
        
        // first we have offset and length of variable length fields
        int bitmap_idx = fixed_num_columns % 8;
        int idx = 0;
        for(int i = 0 ; i < variable_num_columns ; i ++) {

            if((block[offset + nullbitmap_offset + (fixed_num_columns + i) / 8] & (1 << (7 - bitmap_idx))) == 0){
                int var_offset = (block[offset + idx] & 0xFF) | ((block[offset + idx + 1] & 0xFF) << 8); 
                int var_length = (block[offset + idx + 2] & 0xFF) | ((block[offset + idx + 3] & 0xFF) << 8);
                // read the variable length field
                byte[] var_field = new byte[var_length];
                for(int j = 0 ; j < var_length ; j ++){
                    var_field[j] = block[offset + var_offset + j];
                }
                String var_field_str = new String(var_field);
                record[fixed_num_columns + i] = var_field_str;
            }
            else{
                record[fixed_num_columns + i] = null;
            }
            idx += 4;
            bitmap_idx++;
            if(bitmap_idx == 8){
                bitmap_idx = 0;
            }
        }

        // read fixed length fields
        bitmap_idx = 0;
        for(int i = 0 ; i < fixed_num_columns ; i ++){
            // get type of this column from schema
            int type_offset = (schema_block[2 + 2 * i] & 0xFF) | ((schema_block[3 + 2 * i] & 0xFF) << 8);
            int type = schema_block[type_offset] & 0xFF;
            if(type == ColumnType.INTEGER.ordinal()){
                if((block[offset + nullbitmap_offset + i / 8] & (1 << (7 - bitmap_idx))) == 0){
                    int val = (block[offset + idx] & 0xFF) | ((block[offset + idx + 1] & 0xFF) << 8) | ((block[offset + idx + 2] & 0xFF) << 16) | ((block[offset + idx + 3] & 0xFF) << 24);
                    record[i] = val;
                }
                else{
                    record[i] = null;
                }
                idx += 4;
            }
            else if(type == ColumnType.BOOLEAN.ordinal()){
                if((block[offset + nullbitmap_offset + i / 8] & (1 << (7 - bitmap_idx))) == 0){
                    record[i] = block[offset + idx] == 1;
                }
                else{
                    record[i] = null;
                }
                idx += 1;
            }
            else if(type == ColumnType.FLOAT.ordinal()){
                if((block[offset + nullbitmap_offset + i / 8] & (1 << (7 - bitmap_idx))) == 0){
                    int val = (block[offset + idx] & 0xFF) | ((block[offset + idx + 1] & 0xFF) << 8) | ((block[offset + idx + 2] & 0xFF) << 16) | ((block[offset + idx + 3] & 0xFF) << 24);
                    record[i] = Float.intBitsToFloat(val);
                }
                else{
                    record[i] = null;
                }
                idx += 4;
            }
            else if(type == ColumnType.DOUBLE.ordinal()){
                if((block[offset + nullbitmap_offset + i / 8] & (1 << (7 - bitmap_idx))) == 0){
                    long val = ((long) block[offset + idx] & 0xFF) | (((long) block[offset + idx + 1] & 0xFF) << 8) | (((long) block[offset + idx + 2] & 0xFF) << 16) | (((long) block[offset + idx + 3] & 0xFF) << 24) | (((long) block[offset + idx + 4] & 0xFF) << 32) | (((long) block[offset + idx + 5] & 0xFF) << 40) | (((long) block[offset + idx + 6] & 0xFF) << 48) | (((long) block[offset + idx + 7] & 0xFF) << 56);
                    Double double_val = Double.longBitsToDouble(val);
                    record[i] = double_val;
                }
                else{
                    record[i] = null;
                }
                idx += 8;
            }
            bitmap_idx++;
            if(bitmap_idx == 8){
                bitmap_idx = 0;
            }
        }
        return record;
    }

    private int get_nullbitmap_offset_in_record(byte[] schema_block){
        int num_columns = (schema_block[0] & 0xFF) | ((schema_block[1] & 0xFF) << 8);
        int idx = 0;
        for(int i = 0 ; i < num_columns ; i ++){
            int offset = (schema_block[2 + 2 * i] & 0xFF) | ((schema_block[3 + 2 * i] & 0xFF) << 8);
            int type = schema_block[offset] & 0xFF;
            if(type == ColumnType.VARCHAR.ordinal()){
                idx += 4;
            }
            else if(type == ColumnType.INTEGER.ordinal()){
                idx += 4;
            }
            else if(type == ColumnType.BOOLEAN.ordinal()){
                idx += 1;
            }
            else if(type == ColumnType.FLOAT.ordinal()){
                idx += 4;
            }
            else if(type == ColumnType.DOUBLE.ordinal()){
                idx += 8;
            }
        }
        return idx;
    }

    private int get_num_columns(byte[] schema_block){
        return (schema_block[0] & 0xFF) | ((schema_block[1] & 0xFF) << 8);
    }

    private int get_fixed_num_columns(byte[] schema_block){
        int num_columns = (schema_block[0] & 0xFF) | ((schema_block[1] & 0xFF) << 8);
        for(int i = 0 ; i < num_columns ; i ++){
            int offset = (schema_block[2 + 2 * i] & 0xFF) | ((schema_block[3 + 2 * i] & 0xFF) << 8);
            int type = schema_block[offset] & 0xFF;
            if(type == ColumnType.VARCHAR.ordinal()){
                return i;
            }
        }
        return num_columns;
    }

    public DB getDb() {
        return db;
    }

}