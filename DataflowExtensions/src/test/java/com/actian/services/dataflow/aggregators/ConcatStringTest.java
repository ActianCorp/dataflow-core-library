package com.actian.services.dataflow.aggregators;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.graphs.LogicalGraphInstance;
import com.pervasive.datarush.operators.assertion.AssertEqual;
import com.pervasive.datarush.operators.group.Group;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.types.RecordTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.record;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConcatStringTest {
    
    public ConcatStringTest() {
    }

    @Test
    public void testConcatBlank() {
        // Setup graph

        RecordTokenType dataType = record(STRING("fruit"), STRING("color"));
        RecordTokenList data = new RecordTokenList(dataType, 3);
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Red")));
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Green")));
        data.append(new RecordToken(dataType, StringToken.parse("Pear"), StringToken.parse("Green")));

        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_concat");
        EmitRecords er = g.add(new EmitRecords());
        er.setInput(data);

        Group grp = new Group();
        grp.setAggregations("concat(color,\"\") as colors");
        grp.setKeys(new String[]{"fruit"});
        grp = g.add(grp);

        AssertEqual p = g.add(new AssertEqual());
        RecordTokenList expectedData = new RecordTokenList(record(STRING("fruit"),STRING("colors")),2);
        expectedData.append(new RecordToken(expectedData.getType(),StringToken.parse("Apple"),StringToken.parse("RedGreen")));
        expectedData.append(new RecordToken(expectedData.getType(),StringToken.parse("Pear"),StringToken.parse("Green")));
        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        g.connect(grp.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(), p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), grp.getInput());
        g.connect(grp.getOutput(), lr.getInput());
        // Compile & run
        LogicalGraphInstance gi = g.compile(EngineConfig.engine().parallelism(1));
        gi.run();
    }

    @Test
    public void testConcatComma() {
        // Setup graph

        RecordTokenType dataType = record(STRING("fruit"), STRING("color"));
        RecordTokenList data = new RecordTokenList(dataType, 3);
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Red")));
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Green")));
        data.append(new RecordToken(dataType, StringToken.parse("Pear"), StringToken.parse("Green")));

        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_concat");
        EmitRecords er = g.add(new EmitRecords());
        er.setInput(data);

        Group grp = new Group();
        grp.setAggregations("concat(color,\",\") as colors");
        grp.setKeys(new String[]{"fruit"});
        grp = g.add(grp);

        AssertEqual p = g.add(new AssertEqual());
        RecordTokenList expectedData = new RecordTokenList(record(STRING("fruit"),STRING("colors")),2);
        expectedData.append(new RecordToken(expectedData.getType(),StringToken.parse("Apple"),StringToken.parse("Red,Green")));
        expectedData.append(new RecordToken(expectedData.getType(),StringToken.parse("Pear"),StringToken.parse("Green")));
        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        g.connect(grp.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(), p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), grp.getInput());
        g.connect(grp.getOutput(), lr.getInput());
        // Compile & run
        LogicalGraphInstance gi = g.compile(EngineConfig.engine().parallelism(1));
        gi.run();
    }
    
}
