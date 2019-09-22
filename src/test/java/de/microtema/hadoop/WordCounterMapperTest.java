package de.microtema.hadoop;

import de.seven.fate.model.builder.annotation.Inject;
import de.seven.fate.model.builder.annotation.Models;
import de.seven.fate.model.builder.util.FieldInjectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WordCounterMapperTest {

    @Inject
    WordCounterMapper sut;

    @Mock
    LongWritable longWritable;

    @Mock
    Text value;

    @Mock
    OutputCollector<Text, IntWritable> outputCollector;

    @Mock
    Reporter reporter;

    @Captor
    ArgumentCaptor<Text> textCaptor;

    @Captor
    ArgumentCaptor<IntWritable> intWritableFooCaptor;

    @Models
    List<String> words;

    @Before
    public void setUp() {
        FieldInjectionUtil.injectFields(this);
    }

    @Test
    public void mapWithEmptyText() throws IOException {

        when(value.toString()).thenReturn(StringUtils.EMPTY);

        sut.map(longWritable, value, outputCollector, reporter);

        verify(outputCollector, times(0)).collect(any(Text.class), any(IntWritable.class));
    }

    @Test
    public void mapWithMultipleWordText() throws IOException {

        when(value.toString()).thenReturn(words.stream().map(String::toString).collect(Collectors.joining("\n")));

        sut.map(longWritable, value, outputCollector, reporter);

        verify(outputCollector, times(words.size())).collect(textCaptor.capture(), intWritableFooCaptor.capture());
    }
}
