package com.actian.services.dataflow.operators;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.graphs.LogicalGraphInstance;
import com.pervasive.datarush.operators.assertion.AssertEqual;
import com.pervasive.datarush.operators.assertion.AssertRowCount;
import com.pervasive.datarush.operators.assertion.AssertSorted;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.sort.Sort;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.TokenOrder;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.record.SortKey;
import com.pervasive.datarush.tokens.scalar.IntToken;
import com.pervasive.datarush.tokens.scalar.LongToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.types.RecordTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.INT;
import static com.pervasive.datarush.types.TokenTypeConstant.LONG;
import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.record;
import org.junit.Test;

public class LeadLagTest {
    
    public LeadLagTest() {
    }

    @Test
    public void testLeadLag() {
        
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_leadlag");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("id"), STRING("value"),INT("offset"));
        RecordTokenList data = new RecordTokenList(dataType, 4);
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("apple"),IntToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id2"), StringToken.parse("apricot"),IntToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("date"),IntToken.parse("4")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("cherry"),IntToken.parse("3")));
        er.setInput(data);

        LeadLag ll = g.add(new LeadLag());
        ll.setKeys(new String[]{"id"});
        ll.setValueField("offset");

        RecordTokenList expectedData = new RecordTokenList(
            record(STRING("id"),STRING("value"),INT("offset"),
                   STRING("id_2"),STRING("value_2"),INT("offset_2"),
                   STRING("id_3"),STRING("value_3"),INT("offset_3")),4);

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("apple"),IntToken.parse("1"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL,
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3")));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3"),
            StringToken.parse("id1"),StringToken.parse("apple"),IntToken.parse("1"),
            StringToken.parse("id1"),StringToken.parse("date"),IntToken.parse("4")));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("date"),IntToken.parse("4"),
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id2"),StringToken.parse("apricot"),IntToken.parse("1"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL,
            StringToken.NULL,StringToken.NULL,IntToken.NULL));

        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        AssertEqual p = g.add(new AssertEqual());
        g.connect(ll.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(),p.getExpectedInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), ll.getInput());
        g.connect(ll.getOutput(), lr.getInput());
        // Compile & run
        g.run(EngineConfig.engine().parallelism(1));
    }
    
    @Test
    public void testEmpty() {
        
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_empty");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("id"), STRING("value"),INT("offset"));
        RecordTokenList data = new RecordTokenList(dataType, 1);
        er.setInput(data);

        LeadLag ll = g.add(new LeadLag());
        ll.setKeys(new String[]{"id"});
        ll.setValueField("offset");

        AssertRowCount rc = g.add(new AssertRowCount(0));
        g.connect(ll.getOutput(), rc.getInput());

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), ll.getInput());
        g.connect(ll.getOutput(), lr.getInput());
        // Compile & run
        g.run();
    }
    
    @Test
    public void testNullValues() {
        
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_null");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("id"), STRING("value"),LONG("offset"));
        RecordTokenList data = new RecordTokenList(dataType, 5);
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("apple"),LongToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id2"), StringToken.parse("apricot"),LongToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("date"),LongToken.parse("4")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("cherry"),LongToken.NULL));
        data.append(new RecordToken(dataType, StringToken.parse("id3"), StringToken.parse("pear"),LongToken.parse("1")));
        er.setInput(data);

        LeadLag ll = g.add(new LeadLag());
        ll.setKeys(new String[]{"id"});
        ll.setValueField("offset");

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), ll.getInput());
        g.connect(ll.getOutput(), lr.getInput());
        // Compile & run
        g.run();
    }
    
    @Test
    public void testSorted() {
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_sorted");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(STRING("id"), STRING("value"),LONG("offset"));
        RecordTokenList data = new RecordTokenList(dataType, 5);
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("apple"),LongToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("date"),LongToken.parse("4")));
        data.append(new RecordToken(dataType, StringToken.parse("id1"), StringToken.parse("cherry"),LongToken.NULL));
        data.append(new RecordToken(dataType, StringToken.parse("id2"), StringToken.parse("apricot"),LongToken.parse("1")));
        data.append(new RecordToken(dataType, StringToken.parse("id3"), StringToken.parse("pear"),LongToken.parse("1")));
        er.setInput(data);
        Sort s = g.add(new Sort());
        s.setSortKeys(new SortKey("id", TokenOrder.DESCENDING));

        
        LeadLag ll = g.add(new LeadLag());
        ll.setKeys(new String[]{"id"});
        ll.setValueField("offset");

        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), s.getInput());
        g.connect(s.getOutput(),ll.getInput());
        g.connect(ll.getOutput(), lr.getInput());

        AssertSorted p = g.add(new AssertSorted(SortKey.desc("id")));
        g.connect(ll.getOutput(), p.getInput());
        // Compile & run
        EngineConfig config = EngineConfig.engine();
        
        LogicalGraphInstance gi = g.compile(config);
        g.run(EngineConfig.engine().parallelism(1));

    }
    
}
