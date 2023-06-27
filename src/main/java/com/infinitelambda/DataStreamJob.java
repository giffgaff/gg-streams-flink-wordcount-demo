package com.infinitelambda;

import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import org.apache.flink.util.OutputTag;


public class DataStreamJob {

    private static final OutputTag<String> rejectedInput = new OutputTag<>("rejected") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("word-count-input")
                .setGroupId("flink-wordcount")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenised = text.process(new Tokenizer());

        DataStream<Tuple2<String, Integer>> counts = tokenised
                .keyBy(item -> item.f0)
                .sum(1);

        DataStream<String> rejectedWords =
            tokenised
                .getSideOutput(rejectedInput)
                .map(value -> value, Types.STRING);

        KafkaSink<Tuple2<String, Integer>> sink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("word-count-output")
                        .setValueSerializationSchema(new SerSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<String> rejectedInputSink = KafkaSink.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("word-count-rejected-input")
                .setValueSerializationSchema(new SerSchema2())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        counts.sinkTo(sink);
        rejectedWords.sinkTo(rejectedInputSink);

        env.execute("Kafka WordCount");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     *
     * <p>This rejects words that are longer than 5 characters long.
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
            String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
            Pattern pattern = Pattern.compile("^\\d\\d (loan|outright)");
            if (pattern.matcher(value).find()) {
                out.collect(new Tuple2<>(value, 1));
            } else {
                ctx.output(rejectedInput, value);
                System.out.println("Err: incorrect value in msg: " + value);
            }
        }
    }

    private static final class SerSchema implements SerializationSchema<Tuple2<String, Integer>> {
        @Override
        public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
            return (stringIntegerTuple2.f0 + ":" + stringIntegerTuple2.f1.toString()).getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final class SerSchema2 implements SerializationSchema<String> {
        @Override
        public byte[] serialize(String stringIntegerTuple2) {
            return (stringIntegerTuple2).getBytes(StandardCharsets.UTF_8);
        }
    }
}
