import com.google.common.collect.Lists;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.aggregators.*;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
class Bipartitematching {
  static class Choicer{
    static int n;
    Choicer(){
      n = 0;
    }
    static int choice(int x, int y) { n++; return Math.random() <= 1.0/n ? y : x; }
  }
  static class OperatorMapper {
    static int choice(int x, int y) { return Math.random() < 0.5 ? y : y; }
    static boolean choice(boolean x, boolean y) { return Math.random() < 0.5 ? y : y; }
    static double choice(double x, double y) { return Math.random() < 0.5 ? y : y; }
    static String choice(String x, String y) { return Math.random() < 0.5 ? y : y; }
    static int mod(int x, int y) { return x % y; }
    static long mod(long x, long y) { return x % y; }
  }
public static class AggData implements Writable
{
  BooleanWritable agg_g_v_X43b;
  public AggData()
  {
    this.agg_g_v_X43b = new BooleanWritable();
  }
  public void write(DataOutput out) throws IOException
  {
    agg_g_v_X43b.write(out);
  }
  public void readFields(DataInput in) throws IOException
  {
    agg_g_v_X43b.readFields(in);
  }
};
public static class MsgData implements Writable
{
  Text tag;
  IntWritable agg_X431;
  IntWritable agg_X430;
  IntWritable agg_X42f;
  IntWritable vid;
  public String toString(){
    return "["+tag+" "+agg_X431.get()+" "+agg_X430.get()+" "+agg_X42f.get()+" "+vid.get()+"]";    
  }

  public MsgData()
  {
    this.tag = new Text();
    this.agg_X431 = new IntWritable();
    this.agg_X430 = new IntWritable();
    this.agg_X42f = new IntWritable();
    this.vid = new IntWritable();
  }
  public void write(DataOutput out) throws IOException
  {
    tag.write(out);
    agg_X431.write(out);
    agg_X430.write(out);
    agg_X42f.write(out);
    vid.write(out);
  }
  public void readFields(DataInput in) throws IOException
  {
    tag.readFields(in);
    agg_X431.readFields(in);
    agg_X430.readFields(in);
    agg_X42f.readFields(in);
    vid.readFields(in);
  }
};
public static class VertData implements Writable
{
  IntWritable phase_bipartitematching;
  IntWritable subphase_bipartitematching;
  IntWritable step_bipartitematching;
  IntWritable data_g1_X42c_X435_match_X41a;
  IntWritable data_g2_X42d_X436_match_X41a;
  IntWritable data_g3_X42e_X437_match_X41a;
  IntWritable step_g_v_X43b;
  IntWritable val;
  IntWritable data_g_v_X43b_match_X41a_prev;
  IntWritable data_g_v_X43b_match_X41a_curr;
  public VertData()
  {
    this.phase_bipartitematching = new IntWritable();
    this.subphase_bipartitematching = new IntWritable();
    this.step_bipartitematching = new IntWritable();
    this.data_g1_X42c_X435_match_X41a = new IntWritable();
    this.data_g2_X42d_X436_match_X41a = new IntWritable();
    this.data_g3_X42e_X437_match_X41a = new IntWritable();
    this.step_g_v_X43b = new IntWritable();
    this.val = new IntWritable();
    this.data_g_v_X43b_match_X41a_prev = new IntWritable();
    this.data_g_v_X43b_match_X41a_curr = new IntWritable();
  }
  public void write(DataOutput out) throws IOException
  {
    phase_bipartitematching.write(out);
    subphase_bipartitematching.write(out);
    step_bipartitematching.write(out);
    data_g1_X42c_X435_match_X41a.write(out);
    data_g2_X42d_X436_match_X41a.write(out);
    data_g3_X42e_X437_match_X41a.write(out);
    step_g_v_X43b.write(out);
    val.write(out);
    data_g_v_X43b_match_X41a_prev.write(out);
    data_g_v_X43b_match_X41a_curr.write(out);
  }
  public void readFields(DataInput in) throws IOException
  {
    phase_bipartitematching.readFields(in);
    subphase_bipartitematching.readFields(in);
    step_bipartitematching.readFields(in);
    data_g1_X42c_X435_match_X41a.readFields(in);
    data_g2_X42d_X436_match_X41a.readFields(in);
    data_g3_X42e_X437_match_X41a.readFields(in);
    step_g_v_X43b.readFields(in);
    val.readFields(in);
    data_g_v_X43b_match_X41a_prev.readFields(in);
    data_g_v_X43b_match_X41a_curr.readFields(in);
  }
};
public static class MasterComputation extends DefaultMasterCompute
{
  public void initialize() throws InstantiationException, IllegalAccessException
  {
    registerAggregator("agg_g_v_X43b", BooleanAndAggregator.class);
  }
  public void compute()
  {
  }
};
public static class VertexComputation extends BasicComputation<IntWritable,VertData,IntWritable,MsgData>
{
  public void compute(Vertex<IntWritable, VertData, IntWritable> vertex, Iterable<MsgData> messages)
  {
    VertData _v = vertex.getValue();
    if (getSuperstep() + 1 == 1)
    {
      _v.phase_bipartitematching = new IntWritable(3);
      _v.subphase_bipartitematching = new IntWritable(0);
    }
    switch (((_v.phase_bipartitematching).get()) * 10 + ((_v.subphase_bipartitematching).get()))
    {
    case 0:
      {
        {
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(0);
          _v.subphase_bipartitematching = new IntWritable(1);
          {
            
            for (Edge<IntWritable, IntWritable> edge : vertex.getEdges())
            if (((_v.data_g_v_X43b_match_X41a_prev).get() == -1))
            {
              MsgData msg;
              msg = new MsgData();
              msg.agg_X431 = new IntWritable((vertex.getId()).get());
              sendMessage(edge.getTargetVertexId(), msg);
            }
          }
          break;
        }
      }
      break;
    case 1:
      {
        int agg_X431;
        int var_X43e;
        {
          agg_X431 = -1;
	  Choicer choicer = new Choicer();
          for (MsgData msg : messages)
            agg_X431 = choicer.choice(agg_X431, (msg.agg_X431).get());
          var_X43e = ((((vertex.getId()).get()%2 == 0) && ((_v.data_g_v_X43b_match_X41a_prev).get() == -1))? agg_X431: (_v.data_g_v_X43b_match_X41a_prev).get());
          _v.data_g1_X42c_X435_match_X41a = new IntWritable(var_X43e);
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(1);
          _v.subphase_bipartitematching = new IntWritable(0);
          {
          }
          break;
        }
      }
      break;
    case 10:
      {
        {
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(1);
          _v.subphase_bipartitematching = new IntWritable(1);
          {
            
            for (Edge<IntWritable, IntWritable> edge : vertex.getEdges())
            {
              MsgData msg;
              msg = new MsgData();
              msg.agg_X430 = new IntWritable((_v.data_g1_X42c_X435_match_X41a).get());
    	      msg.vid = new IntWritable(vertex.getId().get());
              sendMessage(edge.getTargetVertexId(), msg);
            }
          }
          break;
        }
      }
      break;
    case 11:
      {
        int agg_X430;
        int var_X43d;
        {
          agg_X430 = -1;
	  Choicer choicer = new Choicer();
          for (MsgData msg : messages)
	    if((msg.agg_X430).get() == (vertex.getId()).get())
              agg_X430 = choicer.choice(agg_X430, (msg.vid).get());
          var_X43d = ((((vertex.getId()).get()%2 == 1) && ((_v.data_g1_X42c_X435_match_X41a).get() == -1))? agg_X430: (_v.data_g1_X42c_X435_match_X41a).get());
          _v.data_g2_X42d_X436_match_X41a = new IntWritable(var_X43d);
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(2);
          _v.subphase_bipartitematching = new IntWritable(0);
          {
          }
          break;
        }
      }
      break;
    case 20:
      {
        {
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(2);
          _v.subphase_bipartitematching = new IntWritable(1);
          {
            
            for (Edge<IntWritable, IntWritable> edge : vertex.getEdges())
            {
              MsgData msg;
              msg = new MsgData();
              msg.agg_X42f = new IntWritable((_v.data_g2_X42d_X436_match_X41a).get());
	      msg.vid = new IntWritable(vertex.getId().get());
              sendMessage(edge.getTargetVertexId(), msg);
	      //System.out.println(""+vertex.getId().get()+"->"+edge.getTargetVertexId().get()+":"+msg);
            }
          }
          break;
        }
      }
      break;
    case 21:
      {
        int agg_X42f;
        int var_X43c;
        {
          agg_X42f = -1;
	  Choicer choicer = new Choicer();
          for (MsgData msg : messages){
	    if((msg.agg_X42f).get() == (vertex.getId()).get())
               agg_X42f = choicer.choice(agg_X42f, (msg.vid).get());
          //System.out.println("  "+vertex.getId().get()+"<-"+msg.vid.get()+":"+msg+" "+agg_X42f);
	  }
          var_X43c = ((((vertex.getId()).get()%2 == 0) && ((_v.data_g2_X42d_X436_match_X41a).get() != -1))? agg_X42f: (_v.data_g2_X42d_X436_match_X41a).get());
          _v.data_g3_X42e_X437_match_X41a = new IntWritable(var_X43c);
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(3);
          _v.subphase_bipartitematching = new IntWritable(2);
          {
          }
          break;
        }
      }
      break;
    case 30:
      {
        {
          _v.step_g_v_X43b = new IntWritable(0);
          _v.data_g_v_X43b_match_X41a_curr = new IntWritable(-1);
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(3);
          _v.subphase_bipartitematching = new IntWritable(1);
          {
            boolean agg_g_v_X43b;
            {
              agg_g_v_X43b = (((_v.data_g_v_X43b_match_X41a_prev).get() == (_v.data_g_v_X43b_match_X41a_curr).get()));
              aggregate("agg_g_v_X43b", new BooleanWritable(agg_g_v_X43b));
            }
          }
          break;
        }
      }
      break;
    case 31:
      {
        boolean agg_g_v_X43b;
        {
          agg_g_v_X43b = ((BooleanWritable)(getAggregatedValue("agg_g_v_X43b"))).get();
        }
        if ((((_v.step_g_v_X43b).get() > 0) && agg_g_v_X43b))
        {
          vertex.voteToHalt();
          {
            _v.data_g_v_X43b_match_X41a_prev = new IntWritable((_v.data_g_v_X43b_match_X41a_curr).get());
            _v.step_g_v_X43b = new IntWritable(((_v.step_g_v_X43b).get() + 1));
          }
          break;
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(0);
          _v.subphase_bipartitematching = new IntWritable(0);
          {
            _v.data_g_v_X43b_match_X41a_prev = new IntWritable((_v.data_g_v_X43b_match_X41a_curr).get());
            _v.step_g_v_X43b = new IntWritable(((_v.step_g_v_X43b).get() + 1));
          }
          break;
        }
      }
      break;
    case 32:
      {
        int var_X45d;
        {
          var_X45d = (_v.data_g3_X42e_X437_match_X41a).get();
          _v.data_g_v_X43b_match_X41a_curr = new IntWritable(var_X45d);
        }
        if (true)
        {
          _v.phase_bipartitematching = new IntWritable(3);
          _v.subphase_bipartitematching = new IntWritable(1);
          {
            boolean agg_g_v_X43b;
            {
              agg_g_v_X43b = (((_v.data_g_v_X43b_match_X41a_prev).get() == (_v.data_g_v_X43b_match_X41a_curr).get()));
              aggregate("agg_g_v_X43b", new BooleanWritable(agg_g_v_X43b));
            }
          }
          break;
        }
      }
      break;
    }
    vertex.setValue(_v);
  }
};
  public static class InputFormat extends TextVertexInputFormat<IntWritable, VertData, IntWritable> {
    @Override public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
      return new VertexReader();
    }
    class VertexReader extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {
      @Override protected JSONArray preprocessLine(Text line) throws JSONException {
        return new JSONArray(line.toString());
      }
      @Override protected IntWritable getId(JSONArray jsonVertex) throws JSONException, IOException {
        return new IntWritable(jsonVertex.getInt(0));
      }
      @Override protected VertData getValue(JSONArray jsonVertex) throws JSONException, IOException {
        VertData ret = new VertData();
        ret.val = new IntWritable(jsonVertex.getInt(1));

        return ret;
      }
      @Override protected Iterable<Edge<IntWritable, IntWritable>> getEdges(JSONArray jsonVertex) throws JSONException, IOException {
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
        List<Edge<IntWritable, IntWritable>> edges = Lists.newArrayListWithCapacity(jsonEdgeArray.length());
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
          JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
          edges.add(EdgeFactory.create(new IntWritable (jsonEdge.getInt(0)), new IntWritable(jsonEdge.getInt(1))));
        }
        return edges;
      }
      @Override protected Vertex<IntWritable, VertData, IntWritable>handleException(Text line, JSONArray jsonVertex, JSONException e) {
        throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
      }
    }
  }
  public static class OutputFormat extends TextVertexOutputFormat<IntWritable, VertData, IntWritable> {
    @Override public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
      return new VertexWriter();
    }
    private class VertexWriter extends TextVertexWriterToEachLine {
      @Override public Text convertVertexToLine(Vertex<IntWritable, VertData, IntWritable> vertex) throws IOException {
        return new Text("" + vertex.getId().get()
          + " " + vertex.getValue().data_g1_X42c_X435_match_X41a.get()
          + " " + vertex.getValue().data_g2_X42d_X436_match_X41a.get()
          + " " + vertex.getValue().data_g3_X42e_X437_match_X41a.get()
          + " " + vertex.getValue().data_g_v_X43b_match_X41a_prev.get()
          );
      }
    }
  }
}
