package de.microtema.hadoop;

import de.seven.fate.model.builder.annotation.Inject;
import de.seven.fate.model.builder.annotation.Model;
import de.seven.fate.model.builder.annotation.Models;
import de.seven.fate.model.builder.util.FieldInjectionUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WordCounterMapReducerTest {

    @Inject
    WordCounterMapReducer sut;

    @Inject
    IntWritableModelBuilder builder;

    @Mock
    Text key;

    @Mock
    Iterator<IntWritable> values;

    @Mock
    OutputCollector<Text, IntWritable> output;

    @Mock
    Reporter reporter;

    @Captor
    ArgumentCaptor<Text> textCaptor;

    @Captor
    ArgumentCaptor<IntWritable> intWritableFooCaptor;

    @Models
    List<IntWritable> intWritables;

    @Before
    public void setUp() {
        FieldInjectionUtil.injectFields(this);
    }

    @Test
    public void reduceWithEmptyValues() throws IOException {

        when(values.hasNext()).thenReturn(false);

        // when
        sut.reduce(key, values, output, reporter);

        // then
        verify(output).collect(textCaptor.capture(), intWritableFooCaptor.capture());

        assertSame(key, textCaptor.getValue());
        assertEquals(0, intWritableFooCaptor.getValue().get());
    }

    @Test
    public void reduceWithMultipleValues() throws IOException {

        // given
        values = intWritables.iterator();

        // when
        sut.reduce(key, values, output, reporter);

        // then
        verify(output).collect(textCaptor.capture(), intWritableFooCaptor.capture());

        assertSame(key, textCaptor.getValue());
        assertEquals(intWritables.stream().map(IntWritable::get).mapToInt(Integer::intValue).sum(), intWritableFooCaptor.getValue().get());
    }
}
