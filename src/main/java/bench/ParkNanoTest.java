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

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ParkNanoTest {
    public static void main(String[] args) throws IOException {
        benchmarkParkNanos(MICROSECONDS.toNanos(1));
        benchmarkParkNanos(MICROSECONDS.toNanos(10));
        benchmarkParkNanos(MICROSECONDS.toNanos(100));
        benchmarkParkNanos(MICROSECONDS.toNanos(1000));
        benchmarkParkNanos(MICROSECONDS.toNanos(5000));
        System.out.println("enabling hack");
        windowsTimerHack();
        benchmarkParkNanos(MICROSECONDS.toNanos(1));
        benchmarkParkNanos(MICROSECONDS.toNanos(10));
        benchmarkParkNanos(MICROSECONDS.toNanos(100));
        benchmarkParkNanos(MICROSECONDS.toNanos(1000));
        benchmarkParkNanos(MICROSECONDS.toNanos(5000));

        System.in.read();
    }

    private static void benchmarkParkNanos(long sleepNs) {
        long startNanos = System.nanoTime();
        long iterationCount = 1000;
        for (long i = 0; i < iterationCount; i++) {
            LockSupport.parkNanos(sleepNs);
        }
        long durationNanos = System.nanoTime() - startNanos;
        long microsPerIteration = NANOSECONDS.toMicros(durationNanos / iterationCount);
        System.out.println("parkNanos(" + NANOSECONDS.toMicros(sleepNs) + "µs) actually took " + microsPerIteration + "µs");
    }

    public static void windowsTimerHack() {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) { // a delicious interrupt, omm, omm
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
