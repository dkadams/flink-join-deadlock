package com.example.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class JoinDeadlock {
  private static final Logger log = LoggerFactory.getLogger(JoinDeadlock.class);


  private static final int SCALE = 1000; // 1000 seems to be the lowest value to trigger the issue.
  private static final int REF_DATA_COUNT = 5 * SCALE;
  private static final int TEST_DATA_COUNT = 500 * SCALE;

  private final int joinCount;
  private final ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
  private final Random random = new Random(43817399);

  private JoinDeadlock(final int joinCount) {
    this.joinCount = joinCount;
  }

  public static void main(String[] args) throws Exception {
    final int joinCount = args.length > 1 ? Integer.valueOf(args[0]) : 10;
    new JoinDeadlock(joinCount).execute();
  }

  private void execute() throws Exception {
    final DataSet<Tuple3<Integer, String, String>> referenceData = getReferenceData();

    DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> result = getTestData();
    for (int i = 0; i < joinCount; i++) {
      result = addJoin(referenceData, result, i);
    }

    output(result, "join-deadlock-result.txt");
    executionEnvironment.execute();
  }

  private DataSet<Tuple3<Tuple, Integer[], TestIssue[]>>
  addJoin(final DataSet<Tuple3<Integer, String, String>> referenceData, final DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> testData, final int fieldNumber) {
    final String[] joinFieldKeyExpression = {"f0.f" + fieldNumber};
    final DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> nullJoinField
        = testData.flatMap(new KeyFlatMapFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Tuple, Integer[], TestIssue[]>>(joinFieldKeyExpression, getTestDataTypeInfo()) {
      @Override
      public void flatMap(final Tuple3<Tuple, Integer[], TestIssue[]> value, final Collector<Tuple3<Tuple, Integer[], TestIssue[]>> out) throws Exception {
        if (anyKeyFieldsNull(value)) {
          value.f2[fieldNumber] = TestIssue.NULL;
          out.collect(value);
        }
      }
    }).name("FilterNulls#" + fieldNumber).returns(getTestDataTypeInfo());

    //output(nullJoinField, "nullJoinField" + fieldNumber + ".txt");

    final DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> partiallyValidated
        = testData.flatMap(new KeyFlatMapFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Tuple, Integer[], TestIssue[]>>(joinFieldKeyExpression, getTestDataTypeInfo()) {
      @Override
      public void flatMap(final Tuple3<Tuple, Integer[], TestIssue[]> value, final Collector<Tuple3<Tuple, Integer[], TestIssue[]>> out) throws Exception {
        if (!anyKeyFieldsNull(value)) out.collect(value);
      }
    }).name("FilterNonNulls#" + fieldNumber).returns(getTestDataTypeInfo())
        .leftOuterJoin(referenceData)
        .where(joinFieldKeyExpression)
        .equalTo("f1")
        .with(new JoinFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Integer, String, String>, Tuple3<Tuple, Integer[], TestIssue[]>>() {
          @Override
          public Tuple3<Tuple, Integer[], TestIssue[]> join(final Tuple3<Tuple, Integer[], TestIssue[]> first, final Tuple3<Integer, String, String> second) {
            if (second == null) {
              first.f2[fieldNumber] = TestIssue.INVALID;
            } else {
              first.f1[fieldNumber] = second.f0;
            }
            return first;
          }
        }).name("CodeValidationJoin#" + fieldNumber).returns(getTestDataTypeInfo());

    final DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> codeJoined
        = partiallyValidated.flatMap(new FlatMapFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Tuple, Integer[], TestIssue[]>>() {
      @Override
      public void flatMap(final Tuple3<Tuple, Integer[], TestIssue[]> value, final Collector<Tuple3<Tuple, Integer[], TestIssue[]>> out) throws Exception {
        if (value.f1[fieldNumber] != null) out.collect(value);
      }
    }).name("FilterCodeJoined#" + fieldNumber).returns(getTestDataTypeInfo());

    final JoinOperator<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Integer, String, String>, Tuple3<Tuple, Integer[], TestIssue[]>> nameJoined
        = partiallyValidated.flatMap(new FlatMapFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Tuple, Integer[], TestIssue[]>>() {
      @Override
      public void flatMap(final Tuple3<Tuple, Integer[], TestIssue[]> value, final Collector<Tuple3<Tuple, Integer[], TestIssue[]>> out) throws Exception {
        if (value.f1[fieldNumber] == null) out.collect(value);
      }
    }).name("FilterInvalid#" + fieldNumber).returns(getTestDataTypeInfo())
        .leftOuterJoin(referenceData)
        .where(joinFieldKeyExpression)
        .equalTo("f2")
        .with(new JoinFunction<Tuple3<Tuple, Integer[], TestIssue[]>, Tuple3<Integer, String, String>, Tuple3<Tuple, Integer[], TestIssue[]>>() {
          @Override
          public Tuple3<Tuple, Integer[], TestIssue[]> join(final Tuple3<Tuple, Integer[], TestIssue[]> first, final Tuple3<Integer, String, String> second) {
            if (second == null) {
              first.f2[fieldNumber] = TestIssue.INVALID;
            } else {
              first.f1[fieldNumber] = second.f0;
            }
            return first;
          }
        }).name("NameValidationJoin#" + fieldNumber).returns(getTestDataTypeInfo());


    //output(joined, "joined" + fieldNumber + ".txt");

    return codeJoined.union(nameJoined).union(nullJoinField);
  }

  private void output(final DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> result, String filePath) {
    result.writeAsFormattedText(filePath, FileSystem.WriteMode.OVERWRITE,
        new TextOutputFormat.TextFormatter<Tuple3<Tuple, Integer[], TestIssue[]>>() {
          @Override
          public String format(final Tuple3<Tuple, Integer[], TestIssue[]> value) {
            final int fieldCount = value.f0.getArity();
            final StringBuilder sb = new StringBuilder(fieldCount * 15);
            for (int i = 0; i < fieldCount; i++) {
              final String str = value.f0.getField(i);
              sb.append(str)
                  .append(',')
                  .append(value.f1[i])
                  .append(',');
            }
            sb.append(Arrays.stream(value.f2).filter(Objects::nonNull).count());
            return sb.toString();
          }
        }).setParallelism(1);
  }

  private TupleTypeInfo<Tuple3<Tuple, Integer[], TestIssue[]>> getTestDataTypeInfo() {
    final TypeInformation[] stringTupleContent = new TypeInformation[joinCount];
    Arrays.fill(stringTupleContent, BasicTypeInfo.STRING_TYPE_INFO);
    final TupleTypeInfo stringDataType = new TupleTypeInfo<>(Tuple.getTupleClass(joinCount), stringTupleContent);
    final ObjectArrayTypeInfo<TestIssue[], TestIssue> issuesTypeInfo
        = ObjectArrayTypeInfo.getInfoFor(EnumTypeInfo.of(TestIssue.class));
    return new TupleTypeInfo<>(stringDataType, BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, issuesTypeInfo);
  }

  private DataSet<Tuple3<Integer, String, String>> getReferenceData() {
    log.info("Generating Reference Data Records: {}", REF_DATA_COUNT);
    return executionEnvironment.fromParallelCollection(new NumberSequenceIterator(1, REF_DATA_COUNT), Long.class)
        .map(new GenRefData(random));
  }

  private DataSet<Tuple3<Tuple, Integer[], TestIssue[]>> getTestData() {
    log.info("Generating Test Data Records: {}", TEST_DATA_COUNT);
    return executionEnvironment.fromParallelCollection(new NumberSequenceIterator(1, TEST_DATA_COUNT), Long.class)
        .map(new GenTestData(joinCount, random))
        .returns(getTestDataTypeInfo());
  }

  private enum TestIssue {
    INVALID,
    NULL;
  }

  private static class GenRefData implements MapFunction<Long, Tuple3<Integer, String, String>> {
    private final Random random;

    GenRefData(final Random random) {
      this.random = random;
    }

    @Override
    public Tuple3<Integer, String, String> map(final Long value) {
      random.setSeed(value);
      final String code = generateRandomString(random, 8);
      final String name = generateRandomString(random, 10);
      return Tuple3.of(value.intValue(), code, name);
    }
  }

  private static class GenTestData implements MapFunction<Long, Tuple3<Tuple, Integer[], TestIssue[]>> {
    private final int joinCount;
    private final Random random;

    GenTestData(final int joinCount, final Random random) {
      this.joinCount = joinCount;
      this.random = random;
    }

    @Override
    public Tuple3<Tuple, Integer[], TestIssue[]> map(final Long value) {
      random.setSeed(value);
      final Tuple keys = Tuple.newInstance(joinCount);
      for (int i = 0; i < keys.getArity(); i++) {
        if (random.nextBoolean()) {
          final int size = random.nextBoolean() ? 8 : 10;
          keys.setField(generateRandomString(random, size), i);
        }
      }
      final Integer[] ids = new Integer[joinCount];
      final TestIssue[] issues = new TestIssue[joinCount];
      return Tuple3.of(keys, ids, issues);
    }
  }

  private static String generateRandomString(Random random, int length) {
    return random.ints((int)'0', (int)'z')
        // Exclude punctuation chars that appear in the middle of the generated range
        .filter(Character::isLetterOrDigit)
        .limit(length)
        .collect(StringBuilder::new, (builder, i) -> builder.append((char) i), StringBuilder::append)
        .toString();
  }

  private static abstract class KeyFlatMapFunction<IN,OUT> extends RichFlatMapFunction<IN,OUT> {
    private final String[] keyFields;
    private final TypeInformation<IN> inputType;

    private transient KeySelector<IN,Tuple> keySelector;

    KeyFlatMapFunction(final String[] keyFields, final TypeInformation<IN> inputType) {
      this.keyFields = keyFields;
      this.inputType = inputType;
    }

    boolean anyKeyFieldsNull(final IN value) throws Exception {
      try {
        final Tuple key = getKeySelector().getKey(value);
        for (int i = 0; i < key.getArity(); i++) {
          if (key.getField(i) == null) return true;
        }
        return false;
      } catch (NullKeyFieldException nkfe) {
        return true;
      }
    }

    private KeySelector<IN,Tuple> getKeySelector() {
      if (keySelector == null) {
        final Keys<IN> keys = new Keys.ExpressionKeys<>(keyFields, inputType);

        final ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
        this.keySelector = KeySelectorUtil.getSelectorForKeys(keys, inputType, executionConfig);
      }
      return keySelector;
    }
  }

}
