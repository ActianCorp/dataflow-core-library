package com.actian.services.dataflow.aggregators;

import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.record;

import org.junit.Test;

import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.graphs.LogicalGraphInstance;
import com.pervasive.datarush.operators.assertion.AssertEqual;
import com.pervasive.datarush.operators.group.Group;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.IntToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.types.RecordTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.INT;

public class DistinctCountTest {

    @Test
    public void testValues() {
        // Setup graph

        RecordTokenType dataType = record(STRING("fruit"), STRING("color"));
        RecordTokenList data = new RecordTokenList(dataType, 5);
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Green")));
        data.append(new RecordToken(dataType, StringToken.parse("Pear"), StringToken.parse("Green")));
        data.append(new RecordToken(dataType, StringToken.parse("Apple"), StringToken.parse("Green")));

        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_count_distinct");
        EmitRecords er = g.add(new EmitRecords());
        er.setInput(data);

        Group grp = new Group();
        grp.setAggregations("dstrcount(fruit) as cnt_dis");
        grp = g.add(grp);

        AssertEqual p = g.add(new AssertEqual());
        RecordTokenList expectedData = new RecordTokenList(record(INT("cnt_dis")),1);
        expectedData.append(new RecordToken(expectedData.getType(),IntToken.parse("2")));


        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        g.connect(grp.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(), p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), grp.getInput());
        g.connect(grp.getOutput(), lr.getInput());
        // Compile & run
        LogicalGraphInstance gi = g.compile();
        gi.run();

    }


}
