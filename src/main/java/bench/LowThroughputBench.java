/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bench;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.DoubleStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LowThroughputBench {
    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        System.out.println("if you want to tweak timer slack, execute:");
        System.out.println("  sudo echo NNN >/proc/" + ProcessHandle.current().pid() + "/timerslack_ns");
        System.out.println();
        System.out.println("Press enter");
        System.in.read();

        benchmarkParkNanos();

        JetConfig config = new JetConfig();
        System.out.println("args: " + Arrays.toString(args));
        System.out.println("available processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("max heap: " + (Runtime.getRuntime().maxMemory() / 1024/1024) + "M");
        JetInstance instance = Jet.newJetInstance(config);

        int[] rates = {
                1000,
                10_000,
                50_000,
                250_000,
                500_000,
                1_000_000
        };

        TreeMap<Integer, Result> results = new TreeMap<>();

        try {
            for (int rate : rates) {
                IMap<Long, Long> actualRates = instance.getMap("actualRates");
                actualRates.clear();

                Pipeline p = Pipeline.create();
                StreamSource<Object> source = Sources.streamFromProcessor("src",
                        ProcessorMetaSupplier.of(() -> new GeneratorP(rate)));
                StreamStage<WindowResult<Long>> stage = p.drawFrom(source)
                                                         .withIngestionTimestamps()
                                                         .map(FunctionEx.identity())
                                                         .map(FunctionEx.identity())
                                                         .map(FunctionEx.identity())
                                                         .map(FunctionEx.identity())
                                                         .map(FunctionEx.identity())
                                                         .map(FunctionEx.identity())
                                                         .window(tumbling(1000))
                                                         .aggregate(counting());
                stage.drainTo(Sinks.logger());
                stage.map(windowResult -> entry(windowResult.end(), windowResult.result())).drainTo(Sinks.map(actualRates));

                Job job = instance.newJob(p);
                System.out.println("running with rate: " + rate);
                System.out.println("warming up...");
                Thread.sleep(10_000);
                System.out.println("sampling cpu usage...");
                double[] cpuUsages = new double[10];
                for (int i = 0; i < 10; i++) {
                    cpuUsages[i] = printUsage();
                    Thread.sleep(1000);
                }
                int averageCpuUsage = (int) Math.round(DoubleStream.of(cpuUsages).average().getAsDouble() * 100);
                System.out.println(String.format("average cpu usage: %d%%", averageCpuUsage));
                int actualRate = (int) actualRates.entrySet().stream()
                                                  .sorted(Comparator.comparing(Entry::getKey))
                                                  .skip(10)
                                                  .mapToLong(Entry::getValue)
                                                  .average()
                                                  .getAsDouble();
                results.put(rate, new Result(rate, actualRate, averageCpuUsage));
                job.cancel();
                try {
                    job.join();
                } catch (CancellationException expected) {
                    System.out.println("job cancelled");
                }
            }
        } finally {
            Jet.shutdownAll();
        }

        System.out.println("Results:");
        results.values().forEach(System.out::println);
    }

    private static void benchmarkParkNanos() {
        System.out.println("benchmarking parkNanos...");
        long startNanos = System.nanoTime();
        long iterationCount = 5000;
        for (long i = 0; i < iterationCount; i++) {
            LockSupport.parkNanos(10_000);
        }
        long durationNanos = System.nanoTime() - startNanos;
        long microsPerIteration = NANOSECONDS.toMicros(durationNanos) / iterationCount;
        System.out.println("parkNanos(10µs) actually took " + microsPerIteration + "µs");
    }

    private static Method cpuUsageMethod;
    private static OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    private static double printUsage() {
        if (cpuUsageMethod == null) {
            try {
                cpuUsageMethod = operatingSystemMXBean.getClass().getDeclaredMethod("getProcessCpuLoad");
                cpuUsageMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        Double value;
        try {
            value = (Double) cpuUsageMethod.invoke(operatingSystemMXBean);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(String.format("cpu usage=%.0f%%", (value * 100)));
        return value;
    }

    private static class Result {
        final int rate;
        final int actualRate;
        final int cpuUsage;

        private Result(int rate, int actualRate, int cpuUsage) {
            this.rate = rate;
            this.actualRate = actualRate;
            this.cpuUsage = cpuUsage;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "rate=" + rate +
                    ", actualRate=" + actualRate +
                    ", cpuUsage=" + cpuUsage +
                    '}';
        }
    }
}
