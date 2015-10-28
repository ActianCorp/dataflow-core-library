package com.actian.services.dataflow.operators;

import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.assertion.AssertEqual;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.types.RecordTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.record;
import java.net.URL;
import org.junit.Test;

public class SubJobExecutorTest {
    
    public SubJobExecutorTest() {
    }

    @Test
    public void testExecution() {
        // Setup graph
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_subgraph");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("graph"),STRING("body"));
        RecordTokenList data = new RecordTokenList(dataType, 1);
        URL job = SubJobExecutorTest.class.getResource("/graphs/CharCount.dr");

        data.append(new RecordToken(dataType, StringToken.parse(job.getFile()), StringToken.parse("monkeys and cheetas")));
        er.setInput(data);

        SubJobExecutor sub = g.add(new SubJobExecutor());
        sub.setOutputType(record(STRING("rot13ed"),STRING("charCount")));

        RecordTokenList expectedData = new RecordTokenList(sub.getOutputType(),1);
        expectedData.append(new RecordToken(sub.getOutputType(),StringToken.parse("zbaxrlf naq purrgnf"),StringToken.parse("19")));

        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        AssertEqual p = g.add(new AssertEqual());
        
        g.connect(sub.getOutput(),p.getActualInput());
        g.connect(expectedRecords.getOutput(),p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), sub.getInput());
        g.connect(sub.getOutput(), lr.getInput());
        g.run();
    }
    
}
