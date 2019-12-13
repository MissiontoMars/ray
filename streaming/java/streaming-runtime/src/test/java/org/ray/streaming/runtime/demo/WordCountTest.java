package org.ray.streaming.runtime.demo;

import com.google.common.collect.ImmutableMap;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.api.function.impl.ReduceFunction;
import org.ray.streaming.api.function.impl.SinkFunction;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.util.Config;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class WordCountTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTest.class);

  // TODO(zhenxuanpan): this test only works in single-process mode, because we put
  //   results in this in-memory map.
  static Map<String, Integer> wordCount = new ConcurrentHashMap<>();

  @Test
  public void testWordCount() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, Object> config = new HashMap<>();
    config.put(Config.STREAMING_BATCH_MAX_COUNT, 1);
    config.put(Config.CHANNEL_TYPE, Config.MEMORY_CHANNEL);
    streamingContext.withConfig(config);
    List<String> text = new ArrayList<>();
    text.add("hello world eagle eagle eagle");
    StreamSource<String> streamSource = StreamSource.buildSource(streamingContext, text);
    streamSource
        .flatMap((FlatMapFunction<String, WordAndCount>) (value, collector) -> {
          String[] records = value.split(" ");
          for (String record : records) {
            collector.collect(new WordAndCount(record, 1));
          }
        })
        .keyBy(pair -> pair.word)
        .reduce((ReduceFunction<WordAndCount>) (oldValue, newValue) ->
            new WordAndCount(oldValue.word, oldValue.count + newValue.count))
        .sink((SinkFunction<WordAndCount>)
            result -> wordCount.put(result.word, result.count));

    streamingContext.execute();

    // Sleep until the count for every word is computed.
    while (wordCount.size() < 3) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.warn("Got an exception while sleeping.", e);
      }
    }
    Assert.assertEquals(wordCount, ImmutableMap.of("eagle", 3, "hello", 1, "world", 1));
    LOGGER.info("TEST DONE=================");
      try {
        Thread.sleep(8000);
      } catch (InterruptedException e) {
        LOGGER.warn("Got an exception while sleeping.", e);
      }
  }

  private static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }

}
