package rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;

import convention.PConvention;
import manager.StorageManager;

import java.util.List;


public class PTableScan extends TableScan implements PRel {

    public PTableScan(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        List<RelHint> hints,
                        RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    public static PTableScan create(RelOptCluster cluster, RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        Class elementType = Object.class;
        final RelTraitSet traitSet = cluster.traitSetOf(PConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                    if (table != null) {
                        return table.getStatistic().getCollations();
                    }
                    return ImmutableList.of();
                });
        return new PTableScan(cluster, traitSet, ImmutableList.of(), relOptTable);
    }

    @Override
    public String toString() {
        return "PTableScan ["+ getQualifiedName() + "]";
    }

    public String getQualifiedName() {
        return table.getQualifiedName().get(1);
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }

    public List<String> getColumnNames() {
        return table.getRowType().getFieldNames();
    }

    private StorageManager storageManager;
    private int curr_block_id;
    private List<Object[]> curr_block;
    private int curr_idx;
    
    private int num_fixed_length_columns;

    @Override
    public boolean open(){

        logger.trace("Opening PTableScan");

        storageManager = StorageManager.getInstance();
        curr_block_id = 1;
        curr_block = storageManager.get_records_from_block(getTableName(), curr_block_id);
        curr_idx = 0;

        num_fixed_length_columns = 0;
        for(int i = 0; i < table.getRowType().getFieldCount(); i++){
            if(!table.getRowType().getFieldList().get(i).getType().getSqlTypeName().getName().equals("VARCHAR")){
                num_fixed_length_columns++;
            }
        }

        return true;
    }

    @Override
    public void close(){
        logger.trace("Closing PTableScan");
        return;
    }

    @Override
    public Object[] next(){

        logger.trace("Getting next row from PTableScan");

        if(curr_idx < curr_block.size()){
            return transform_row(curr_block.get(curr_idx++));
        }
        curr_block_id++;
        curr_block = storageManager.get_records_from_block(getTableName(), curr_block_id);
        curr_idx = 0;
        if(curr_block == null || curr_block.size() == 0){
            return null;
        }
        return transform_row(curr_block.get(curr_idx++));
    }

    @Override
    public boolean hasNext(){

        logger.trace("Checking if PTableScan has next");

        if(curr_idx < curr_block.size()){
            return true;
        }
        curr_block_id++;
        curr_block = storageManager.get_records_from_block(getTableName(), curr_block_id);
        curr_idx = 0;
        if(curr_block == null || curr_block.size() == 0){
            return false;
        }
        return true;
    }

    // changes the order of columns to match the order in the table
    private Object[] transform_row(Object[] row){
        Object[] new_row = new Object[row.length];
        int fixed_ptr = 0, variable_ptr = num_fixed_length_columns;
        for(int i = 0; i < row.length; i++){
            if(table.getRowType().getFieldList().get(i).getType().getSqlTypeName().getName().equals("VARCHAR")){
                new_row[i] = row[variable_ptr++];
            } else {
                new_row[i] = row[fixed_ptr++];
            }
        }
        return new_row;
    }
}