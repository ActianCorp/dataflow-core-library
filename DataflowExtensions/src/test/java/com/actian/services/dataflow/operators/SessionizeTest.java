package com.actian.services.dataflow.operators;

import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.TIMESTAMP;
import static com.pervasive.datarush.types.TokenTypeConstant.record;


import org.junit.Test;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.assertion.AssertEqual;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.IntToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.tokens.scalar.TimestampToken;
import com.pervasive.datarush.types.RecordTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.INT;

public class SessionizeTest {

    @Test
    public void testSession() {
        // Setup graph
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_sessionize");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("id"), TIMESTAMP("dt"));
        RecordTokenList data = new RecordTokenList(dataType, 4);
        data.append(new RecordToken(dataType, StringToken.parse("id1"), TimestampToken.parse("2015-01-01T00:00:10Z")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), TimestampToken.parse("2015-01-01T00:00:00Z")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), TimestampToken.parse("2015-01-01T00:00:30Z")));
        data.append(new RecordToken(dataType, StringToken.parse("id2"), TimestampToken.parse("2015-01-01T00:00:00Z")));
        er.setInput(data);

        Sessionize s = g.add(new Sessionize());
        s.setKeyColumn("id");
        s.setTimeColumn("dt");
        s.setInterval(1 * 1000); // 10 secs

        RecordTokenList expectedType = new RecordTokenList(record(STRING("id"),TIMESTAMP("dt"),INT("session_id")),4);
        expectedType.append(new RecordToken(expectedType.getType(),
            StringToken.parse("id1"),TimestampToken.parse("2015-01-01T00:00:30Z"),IntToken.parse("1")));
        expectedType.append(new RecordToken(expectedType.getType(),
            StringToken.parse("id1"),TimestampToken.parse("2015-01-01T00:00:10Z"),IntToken.parse("2")));
        expectedType.append(new RecordToken(expectedType.getType(),
            StringToken.parse("id1"),TimestampToken.parse("2015-01-01T00:00:00Z"),IntToken.parse("3")));
        expectedType.append(new RecordToken(expectedType.getType(),
            StringToken.parse("id2"),TimestampToken.parse("2015-01-01T00:00:00Z"),IntToken.parse("1")));

        EmitRecords expectedRecords = g.add( new EmitRecords(expectedType));
        AssertEqual p = g.add(new AssertEqual());

        g.connect(s.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(),p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), s.getInput());
        g.connect(s.getOutput(), lr.getInput());
        // Compile & run
        g.run(EngineConfig.engine().parallelism(1));
    }

}
