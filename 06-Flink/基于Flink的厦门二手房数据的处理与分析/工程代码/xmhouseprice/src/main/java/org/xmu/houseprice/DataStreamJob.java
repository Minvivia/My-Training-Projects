/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.xmu.houseprice;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.api.java.io.TextInputFormat;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Objects;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Collections;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

public class DataStreamJob {

	private static final double ALPHA = 0.7;
	private static final double BETA = 0.3;

	static Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	private static SingleOutputStreamOperator<Tuple2<String, Float>> AggregateByLocationFunction(DataStream<HouseMessage> filteredStream) {
		// 调用函数来进行聚类统计操作
		SingleOutputStreamOperator<Tuple2<String, Float>> aggregatedStream = filteredStream
				.keyBy(HouseMessage::getLocationInfo)
				.reduce(new ReduceFunction<HouseMessage>() {
					@Override
					public HouseMessage reduce(HouseMessage value1, HouseMessage value2) throws Exception {
						value1.setTotalPrice(value1.getTotalPrice() + value2.getTotalPrice());
						return value1;
					}
				})
				.map(new MapFunction<HouseMessage, Tuple2<String, Float>>() {
					@Override
					public Tuple2<String, Float> map(HouseMessage value) throws Exception {
						return Tuple2.of(value.getLocationInfo(), value.getTotalPrice());
					}
				});

		// Sort the locations by total price within a time window
		aggregatedStream = aggregatedStream
				.keyBy(value -> value.f0) // Key by location_info
				.timeWindow(Time.seconds(5))
				.process(new ProcessWindowFunction<Tuple2<String, Float>, Tuple2<String, Float>, String, TimeWindow>() {
					@Override
					public void process(String key, Context context, Iterable<Tuple2<String, Float>> elements, Collector<Tuple2<String, Float>> out) throws Exception {
						// Initialize variables to keep track of max total price and corresponding location_info
						Float maxTotalPrice = Float.MIN_VALUE;
						String locationWithMaxPrice = null;

						// Find the max total price and corresponding location_info
						for (Tuple2<String, Float> element : elements) {
							if (element.f1 > maxTotalPrice) {
								maxTotalPrice = element.f1;
								locationWithMaxPrice = element.f0;
							}
						}

						// Output only the element with max total price for the location_info
						if (locationWithMaxPrice != null) {
							out.collect(Tuple2.of(locationWithMaxPrice, maxTotalPrice));
						}
					}
				});
		aggregatedStream.map(new MapFunction<Tuple2<String, Float>, Tuple2<String, Float>>() {
			@Override
			public Tuple2<String, Float> map(Tuple2<String, Float> value) throws Exception {
				logger.info("AggregatedStrea Data: {}", value.toString()); // 使用局部的 final logger
				return value;
			}
		});
		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/AggregateByLocation.csv";
		aggregatedStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("AggregateByLocation CSV Sink");
		return aggregatedStream;
	}

	private static SingleOutputStreamOperator<Tuple2<String, Integer>> TotalHouseOfLocationFunction(DataStream<HouseMessage> filteredStream) {
		// 调用函数来进行聚类统计操作
		SingleOutputStreamOperator<Tuple2<String, Integer>> aggregatedStream = filteredStream
				.keyBy(HouseMessage::getLocationInfo)
				.map(new MapFunction<HouseMessage, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(HouseMessage value) throws Exception {
						return Tuple2.of(value.getLocationInfo(), 1); // 每条记录计数为1
					}
				})
				.keyBy(value -> value.f0) // Key by location_info
				.timeWindow(Time.seconds(5))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
						// 同一区域的房源总数相加
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				});
		SingleOutputStreamOperator<Tuple2<String, Integer>> sortedAggregatedStream = aggregatedStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {
						List<Tuple2<String, Integer>> sortedList = new ArrayList<>();
						for (Tuple2<String, Integer> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingInt((Tuple2<String, Integer> tuple) -> tuple.f1).reversed());
						for (Tuple2<String, Integer> element : sortedList) {
							out.collect(element);
						}
					}
				});
		sortedAggregatedStream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
				logger.info("TotalHouse Data: {}", value.toString()); // 使用局部的 final logger
				return value;
			}
		});
		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/TotalHouseOfLocation.csv";
		sortedAggregatedStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("TotalHouseOfLocation CSV Sink");
		return sortedAggregatedStream;
	}

	public static DataStream<Tuple2<String, Double>> calculateAvgUnitPriceByLocation(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple2<String, Double>> avgUnitPriceStream = houseMessageStream
				.keyBy(HouseMessage::getLocationInfo)
				.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<HouseMessage, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
					@Override
					public Tuple3<String, Double, Integer> createAccumulator() {
						return Tuple3.of("", 0.0, 0); // 初始化累计器
					}

					@Override
					public Tuple3<String, Double, Integer> add(HouseMessage value, Tuple3<String, Double, Integer> accumulator) {
						return Tuple3.of(value.getLocationInfo(), accumulator.f1 + value.getUnitPrice(), accumulator.f2 + 1); // 累加 unitPrice 和数量
					}

					@Override
					public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
						return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2); // 返回 location_info 和平均 unitPrice
					}

					@Override
					public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
						return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2); // 合并两个累计器
					}
				});

		// 排序操作
		SingleOutputStreamOperator<Tuple2<String, Double>> sortedStream = avgUnitPriceStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>() {
					@Override
					public void process(ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>.Context context,
										Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) {
						List<Tuple2<String, Double>> sortedList = new ArrayList<>();
						for (Tuple2<String, Double> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingDouble((Tuple2<String, Double> tuple) -> tuple.getField(1)).reversed());
						for (Tuple2<String, Double> element : sortedList) {
							out.collect(element);
						}
					}
				});

		sortedStream.map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
			@Override
			public Tuple2<String, Double> map(Tuple2<String, Double> value) throws Exception {
				logger.info("Sorted AvgUnitPrice Data: {}", value.toString());
				return value;
			}
		});
		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/AvgUnitPriceByLocation.csv";
		sortedStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("AvgUnitPriceByLocation CSV Sink");
		return sortedStream;
	}

	public static DataStream<Tuple2<String, Double>> calculateAvgUnitPriceByLocationSec(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple2<String, Double>> avgUnitPriceStream = houseMessageStream
				// 过滤掉没有 locationInfoSec 的消息
				.filter(houseMessage -> houseMessage.getLocationInfoSec() != null && !houseMessage.getLocationInfoSec().isEmpty())
				.keyBy(HouseMessage::getCompositeKey)
				.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<HouseMessage, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
					@Override
					public Tuple3<String, Double, Integer> createAccumulator() {
						return Tuple3.of("", 0.0, 0); // 初始化累计器
					}

					@Override
					public Tuple3<String, Double, Integer> add(HouseMessage value, Tuple3<String, Double, Integer> accumulator) {
						return Tuple3.of(value.getCompositeKey(), accumulator.f1 + value.getUnitPrice(), accumulator.f2 + 1); // 累加 unitPrice 和数量
					}

					@Override
					public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
						return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2); // 返回组合键 和 平均 unitPrice
					}

					@Override
					public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
						return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2); // 合并两个累计器
					}
				}, new ProcessWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, String, TimeWindow>() {
					@Override
					public void process(String key, ProcessWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) {
						for (Tuple2<String, Double> element : elements) {
							out.collect(element); // 输出每个区域的平均 unitPrice
						}
					}
				});

		// 对结果进行排序
		avgUnitPriceStream = avgUnitPriceStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>() {
					@Override
					public void process(ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>.Context context,
										Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) {
						List<Tuple2<String, Double>> sortedList = new ArrayList<>();
						for (Tuple2<String, Double> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingDouble((Tuple2<String, Double> tuple) -> tuple.getField(1)).reversed());
						for (Tuple2<String, Double> element : sortedList) {
							out.collect(element);
						}
					}
				});

		avgUnitPriceStream.map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
			@Override
			public Tuple2<String, Double> map(Tuple2<String, Double> value) throws Exception {
				logger.info("AvgUnitPrice Data: {}", value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/avgUnitPriceStream.csv";
		avgUnitPriceStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("avgUnitPriceStream CSV Sink");

		return avgUnitPriceStream;
	}

	public static DataStream<Tuple2<Tuple2<String, Boolean>, Double>> calculateAvgPriceByLocationAndElevator(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple2<Tuple2<String, Boolean>, Double>> avgPriceByLocationAndElevatorStream = houseMessageStream
				.keyBy(new HouseMessageKeySelector()) // 使用自定义的 KeySelector
				.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<HouseMessage, Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>>() {
					@Override
					public Tuple3<String, Double, Integer> createAccumulator() {
						return Tuple3.of("", 0.0, 0); // 初始化累计器
					}

					@Override
					public Tuple3<String, Double, Integer> add(HouseMessage value, Tuple3<String, Double, Integer> accumulator) {
						return Tuple3.of(value.getLocationInfo(), accumulator.f1 + value.getUnitPrice(), accumulator.f2 + 1); // 累加 unitPrice 和数量
					}

					@Override
					public Tuple3<String, Double, Integer> getResult(Tuple3<String, Double, Integer> accumulator) {
						return accumulator; // 返回累计器
					}

					@Override
					public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
						return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2); // 合并两个累计器
					}
				}, new ProcessWindowFunction<Tuple3<String, Double, Integer>, Tuple2<Tuple2<String, Boolean>, Double>, Tuple2<String, Boolean>, TimeWindow>() {
					@Override
					public void process(Tuple2<String, Boolean> key, Context context, Iterable<Tuple3<String, Double, Integer>> elements, Collector<Tuple2<Tuple2<String, Boolean>, Double>> out) {
						Tuple3<String, Double, Integer> result = elements.iterator().next();
						out.collect(Tuple2.of(key, result.f1 / result.f2)); // 输出 location_info 和 是否有电梯的平均 unitPrice
					}
				});
		// 对结果进行排序
		avgPriceByLocationAndElevatorStream = avgPriceByLocationAndElevatorStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple2<Tuple2<String, Boolean>, Double>, Tuple2<Tuple2<String, Boolean>, Double>, TimeWindow>() {
					@Override
					public void process(ProcessAllWindowFunction<Tuple2<Tuple2<String, Boolean>, Double>, Tuple2<Tuple2<String, Boolean>, Double>, TimeWindow>.Context context,
										Iterable<Tuple2<Tuple2<String, Boolean>, Double>> elements, Collector<Tuple2<Tuple2<String, Boolean>, Double>> out) {
						List<Tuple2<Tuple2<String, Boolean>, Double>> sortedList = new ArrayList<>();
						for (Tuple2<Tuple2<String, Boolean>, Double> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingDouble((Tuple2<Tuple2<String, Boolean>, Double> tuple) -> tuple.f1).reversed());
						for (Tuple2<Tuple2<String, Boolean>, Double> element : sortedList) {
							out.collect(element);
						}
					}
				});

		avgPriceByLocationAndElevatorStream.map(new MapFunction<Tuple2<Tuple2<String, Boolean>, Double>, Tuple2<Tuple2<String, Boolean>, Double>>() {
			@Override
			public Tuple2<Tuple2<String, Boolean>, Double> map(Tuple2<Tuple2<String, Boolean>, Double> value) throws Exception {
				logger.info("AvgPriceByLocationAndElevator Data: {}", value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/avgPriceByLocationAndElevator.csv";
		avgPriceByLocationAndElevatorStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("avgPriceByLocationAndElevatorStream CSV Sink");

		return avgPriceByLocationAndElevatorStream;
	}

	public static DataStream<Tuple3<Integer, Double, Integer>> calculateAvgElevatorNumByYear(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple3<Integer, Double, Integer>> avgElevatorNumByYearStream = houseMessageStream
				.keyBy(HouseMessage::getBuildingYear)
				.timeWindow(Time.seconds(5))
				.aggregate(new ElevatorNumAggregate(), new ElevatorNumProcessWindowFunction());

		avgElevatorNumByYearStream = avgElevatorNumByYearStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Double, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple3<Integer, Double, Integer>> elements, Collector<Tuple3<Integer, Double, Integer>> out) {
						List<Tuple3<Integer, Double, Integer>> sortedList = new ArrayList<>();
						for (Tuple3<Integer, Double, Integer> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingInt((Tuple3<Integer, Double, Integer> tuple) -> tuple.getField(0)));// Sort by building year
						for (Tuple3<Integer, Double, Integer> element : sortedList) {
							out.collect(element);
						}
					}
				});

		avgElevatorNumByYearStream.map(new MapFunction<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Double, Integer>>() {
			@Override
			public Tuple3<Integer, Double, Integer> map(Tuple3<Integer, Double, Integer> value) throws Exception {
				logger.info("AvgElevatorNumByYear Data: {}", value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/avgElevatorNumByYear.csv";
		avgElevatorNumByYearStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("avgElevatorNumByYearStream CSV Sink");

		return avgElevatorNumByYearStream;
	}

	public static DataStream<Tuple2<String, Integer>> calculateTopWordFrequency(DataStream<HouseMessage> houseMessageStream) {
		// 提取 title 并进行分词
		DataStream<Tuple2<String, Integer>> wordStream = houseMessageStream
				.flatMap(new FlatMapFunction<HouseMessage, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(HouseMessage value, Collector<Tuple2<String, Integer>> out) {
						if (value.getTitle() != null) {
							String[] words = value.getTitle().toLowerCase().split("[，、\\s]+");
							for (String word : words) {
								if (!word.isEmpty()) {
									out.collect(new Tuple2<>(word, 1));
								}
							}
						}
					}
				});

		// 统计词频
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = wordStream
				.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
					@Override
					public String getKey(Tuple2<String, Integer> value) throws Exception {
						return value.f0;
					}
				})
				.timeWindow(Time.seconds(5))
				.sum(1);

		// 收集每个窗口的结果并排序
		SingleOutputStreamOperator<Tuple2<String, Integer>> sortedWordFrequencyStream = wordCounts
				.timeWindowAll(Time.seconds(5))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					@Override
					public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
						List<Tuple2<String, Integer>> wordFrequencyList = new ArrayList<>();
						for (Tuple2<String, Integer> tuple : input) {
							wordFrequencyList.add(tuple);
						}
						// 对词频进行排序
						Collections.sort(wordFrequencyList, new Comparator<Tuple2<String, Integer>>() {
							@Override
							public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
								return Integer.compare(o2.f1, o1.f1); // 降序排序
							}
						});
						// 输出排序后的结果
						for (Tuple2<String, Integer> tuple : wordFrequencyList) {
							out.collect(tuple);
						}
					}
				});

		sortedWordFrequencyStream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
				logger.info("SortedWordFrequency Data: {}", value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/sortedWordFrequency.csv";
		sortedWordFrequencyStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("sortedWordFrequencyStream CSV Sink");

		return sortedWordFrequencyStream;
	}

	public static DataStream<Tuple3<String, Double, Integer>> calculateFollowerAndHouseCount(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple3<String, Double, Integer>> followerAndHouseCountStream = houseMessageStream
				.keyBy(HouseMessage::getCommunityName)
				.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<HouseMessage, Tuple3<String, Integer, Integer>, Tuple3<String, Double, Integer>>() {
					@Override
					public Tuple3<String, Integer, Integer> createAccumulator() {
						return Tuple3.of("", 0, 0);
					}

					@Override
					public Tuple3<String, Integer, Integer> add(HouseMessage value, Tuple3<String, Integer, Integer> accumulator) {
						return Tuple3.of(value.getCommunityName(), accumulator.f1 + value.getFollower(), accumulator.f2 + 1);
					}

					@Override
					public Tuple3<String, Double, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
						double averageFollower = accumulator.f2 == 0 ? 0.0 : (double) accumulator.f1 / accumulator.f2;
						return Tuple3.of(accumulator.f0, averageFollower, accumulator.f2);
					}

					@Override
					public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
						return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
					}
				}, new ProcessWindowFunction<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>, String, TimeWindow>() {
					@Override
					public void process(String key, Context context, Iterable<Tuple3<String, Double, Integer>> elements, Collector<Tuple3<String, Double, Integer>> out) {
						for (Tuple3<String, Double, Integer> element : elements) {
							out.collect(element);
						}
					}
				});

		// 对结果进行排序
		SingleOutputStreamOperator<Tuple3<String, Double, Integer>> sortedFollowerAndHouseCountStream = followerAndHouseCountStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple3<String, Double, Integer>> elements, Collector<Tuple3<String, Double, Integer>> out) {
						List<Tuple3<String, Double, Integer>> sortedList = new ArrayList<>();
						for (Tuple3<String, Double, Integer> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingDouble((Tuple3<String, Double, Integer> tuple) -> tuple.f1).reversed());

						// 输出排序后的结果
						for (Tuple3<String, Double, Integer> element : sortedList) {
							out.collect(element);
						}
					}
				});

		sortedFollowerAndHouseCountStream.map(new MapFunction<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>>() {
			@Override
			public Tuple3<String, Double, Integer> map(Tuple3<String, Double, Integer> value) throws Exception {
				logger.info("SortedCommunityName Data: {}", value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/sortedFollowerAndHouseCount.csv";
		sortedFollowerAndHouseCountStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("sortedFollowerAndHouseCountStream CSV Sink");

		return sortedFollowerAndHouseCountStream;
	}

	public static DataStream<Tuple3<String, Double, Integer>> calculateCommunityPopularity(DataStream<HouseMessage> houseMessageStream) {
		DataStream<Tuple3<String, Integer, Integer>> followerAndHouseCountStream = houseMessageStream
				.keyBy(HouseMessage::getLocationCommunityKey)
				.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<HouseMessage, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
					@Override
					public Tuple3<String, Integer, Integer> createAccumulator() {
						return Tuple3.of("", 0, 0);
					}

					@Override
					public Tuple3<String, Integer, Integer> add(HouseMessage value, Tuple3<String, Integer, Integer> accumulator) {
						return Tuple3.of(value.getLocationCommunityKey(), accumulator.f1 + value.getFollower(), accumulator.f2 + 1);
					}

					@Override
					public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
						return accumulator;
					}

					@Override
					public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
						return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
					}
				}, new ProcessWindowFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>, String, TimeWindow>() {
					@Override
					public void process(String key, Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Tuple3<String, Integer, Integer>> out) {
						Tuple3<String, Integer, Integer> result = elements.iterator().next();
						out.collect(Tuple3.of(key, result.f1, result.f2));
					}
				});

		// 对结果进行排序，计算火爆指数
		SingleOutputStreamOperator<Tuple3<String, Double, Integer>> sortedPopularityStream = followerAndHouseCountStream
				.timeWindowAll(Time.seconds(5))
				.process(new ProcessAllWindowFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Double, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Tuple3<String, Double, Integer>> out) {
						List<Tuple3<String, Integer, Integer>> sortedList = new ArrayList<>();
						for (Tuple3<String, Integer, Integer> element : elements) {
							sortedList.add(element);
						}
						sortedList.sort(Comparator.comparingDouble((Tuple3<String, Integer, Integer> tuple) -> calculatePopularityIndex(tuple.f1, tuple.f2)).reversed());

						// 计算火爆指数
						for (Tuple3<String, Integer, Integer> element : sortedList) {
							String LocationCommunityKey = element.f0;
							int follower = element.f1;
							int houseCount = element.f2;
							double popularityIndex = calculatePopularityIndex(follower, houseCount);
							out.collect(Tuple3.of(LocationCommunityKey, popularityIndex, houseCount));
						}
					}
				});

		// 输出排序后的结果
		sortedPopularityStream.map(new MapFunction<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>>() {
			@Override
			public Tuple3<String, Double, Integer> map(Tuple3<String, Double, Integer> value) throws Exception {
				logger.info("Sorted Community Popularity Data: {}" , value.toString());
				return value;
			}
		});

		String csvFileName = "/home/user/farmerj/bigdata/xmhouseprice/outputData/sortedPopularity.csv";
		sortedPopularityStream.writeAsCsv(csvFileName, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1) // 控制并行度，以确保输出结果的顺序
				.name("sortedPopularityStream CSV Sink");

		return sortedPopularityStream;
	}

	private static double calculatePopularityIndex(int followers, int houses) {
		return ALPHA * followers + BETA * houses;
	}

	private static void ProcessHouseMessage(DataStream<HouseMessage> filteredStream,int totalElementCount) {
		// 计算每个区的房价总价之和
		// SingleOutputStreamOperator<Tuple2<String, Float>> aggregateStream = AggregateByLocationFunction(filteredStream);
		// 计算每个区的房源总数
		// SingleOutputStreamOperator<Tuple2<String, Integer>> locationHouseCount = TotalHouseOfLocationFunction(filteredStream);
		// 计算每个区的平均 unitPrice
		// DataStream<Tuple2<String, Double>> avgUnitPriceStream = calculateAvgUnitPriceByLocation(filteredStream);
		// 计算每个区域(区下属一个级别)的unitPrice
		 DataStream<Tuple2<String, Double>> avgUnitPriceSecStream = calculateAvgUnitPriceByLocationSec(filteredStream);
		// 调用并输出含有电梯和不含电梯的平均房价
		 DataStream<Tuple2<Tuple2<String, Boolean>, Double>> avgPriceByLocationAndElevatorStream = calculateAvgPriceByLocationAndElevator(filteredStream);
		// 每个年份的房源平均有几个电梯的信息
		 DataStream<Tuple3<Integer, Double, Integer>> avgElevatorNumByYearStream = calculateAvgElevatorNumByYear(filteredStream);
		// 宣传语词频统计
		 DataStream<Tuple2<String, Integer>> wordCountsStream = calculateTopWordFrequency(filteredStream);
		// 统计小区和关注人数之间的关系
		 DataStream<Tuple3<String, Double, Integer>> followerAndHouseCountStream = calculateFollowerAndHouseCount(filteredStream);
		// 统计火爆程度
		 DataStream<Tuple3<String, Double, Integer>> communityPopularityStream = calculateCommunityPopularity(filteredStream);


	}

	private static void readHouseMessage(StreamExecutionEnvironment env,String hdfsUri,String filePath) throws Exception {
		String HDFSPath = hdfsUri + filePath;
		TextInputFormat format = new TextInputFormat(new Path(HDFSPath));
		DataStream<String> dataStream = env.readFile(format, HDFSPath).setParallelism(1);
		DataStream<HouseMessage> parsedStream = dataStream.map((MapFunction<String, HouseMessage>) value -> {
			String[] datas = value.split(",");
			if(datas[2].equals("title")) {
				return null;
			}
			HouseMessage Message = new HouseMessage();
			Message.setTitle(datas[2]);
			Message.setURL(datas[3]);
			Message.setTotalPrice(Float.parseFloat(datas[4]));
			Message.setUnitPrice(Float.parseFloat(datas[5]));
			Message.setFollower((int)Float.parseFloat(datas[6]));
			Message.sethuXing(datas[7]);
			Message.setChaoXiang(datas[9]);
			Message.setBuildingArea(Float.parseFloat(datas[11]));
			Message.setBuildingYear(Integer.parseInt(datas[12]));
			Message.setCommunityName(datas[13]);
			Message.setLocationInfo(datas[14]);
			Message.setLocationInfoSec(datas[15]);
			Message.setElevator((int)Float.parseFloat(datas[16]));
			Message.setListingDay(datas[17]);
			Message.setLastTrade(datas[18]);
			Message.setHeightLocation((int)Float.parseFloat(datas[22]));
			Message.setHeight((int)Float.parseFloat(datas[23]));
			Message.setElevatorNum((int)Float.parseFloat(datas[24]));
			Message.setHouseNum((int)Float.parseFloat(datas[25]));
			return Message;
		}).setParallelism(1);

		//删除列的第一行表示字段的空行
		DataStream<HouseMessage> filteredStream = parsedStream.filter((FilterFunction<HouseMessage>) Objects::nonNull);

		int totalElementCount = filteredStream.executeAndCollect(3000).size();

		// 为数据流分配时间戳和水印，并提取
		filteredStream = filteredStream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<HouseMessage>forMonotonousTimestamps()
						.withTimestampAssigner((event, timestamp) -> event.getTimestamp())
		);

		ProcessHouseMessage(filteredStream,totalElementCount);
	}

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.

		// 本地路径
		String localFilePath = "/home/user/farmerj/bigdata/xmhouse_data_clean.csv";
		// HDFS路径
		String remoteFilePath = "/fmj/xmhouseprice/xmhouse_data_clean.csv";

		String hdfsUri ="hdfs://127.0.0.1:9000";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		Configuration hdfsConfig = new Configuration();
		hdfsConfig.setString("fs.defaultFS", hdfsUri);
		hdfsConfig.setString("dfs.client.use.datanode.hostname", "true");

		// 读取房屋数据
		readHouseMessage(env, hdfsUri,remoteFilePath);

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
