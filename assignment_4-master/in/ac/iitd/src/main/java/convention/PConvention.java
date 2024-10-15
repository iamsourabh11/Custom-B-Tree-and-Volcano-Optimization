package convention;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;

import rel.PRel;

public enum PConvention implements Convention {
    INSTANCE;

    @Override
    public Class getInterface() {
        return PRel.class;
    }

    @Override
    public String getName() {
        return toString();
    }

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return false;
    }


    @Override
    public RelTraitDef getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait relTrait) {
        return this == relTrait;
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {

    }


    @Override
    public String toString() {
        return "PConvention";
    }
}