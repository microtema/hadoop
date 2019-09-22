package de.microtema.hadoop;

import de.seven.fate.model.builder.AbstractModelBuilder;
import de.seven.fate.model.builder.ModelBuilderFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

public class IntWritableModelBuilder extends AbstractModelBuilder<IntWritable> {

    @Override
    public IntWritable min() {

        return new IntWritable(ModelBuilderFactory.min(Integer.class));
    }

    @Override
    public List<IntWritable> list(int size, boolean skip, boolean required) {

        List<IntWritable> list = new ArrayList<>();

        while (list.size() < size) {

            IntWritable model = min();

            list.add(model);
        }

        return list;
    }
}
