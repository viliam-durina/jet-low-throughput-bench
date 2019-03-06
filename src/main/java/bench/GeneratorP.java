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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class GeneratorP extends AbstractProcessor {

    private final int totalItemsPerSecond;
    private long nsBetweenItems;
    private long startTime;
    private long emittedCount;
    private long expectedCount;
    private Traverser<?> traverser;

    public GeneratorP(int totalItemsPerSecond) {
        this.totalItemsPerSecond = totalItemsPerSecond;
    }

    @Override
    protected void init(@Nonnull Context context) {
        nsBetweenItems = SECONDS.toNanos(1) * context.totalParallelism() / totalItemsPerSecond;
        if (context.globalProcessorIndex() == 0) {
            double actualRate = (double) SECONDS.toNanos(1) / nsBetweenItems;
            getLogger().info(String.format("Actual per-processor rate will be %.1f, that's %.1f in %d processors",
                    actualRate, actualRate * context.totalParallelism(), context.totalParallelism()));
        }
        startTime = System.nanoTime() + nsBetweenItems * context.globalProcessorIndex() / context.totalParallelism();
        traverser = () -> emittedCount < expectedCount
                ? emittedCount++ * context.totalParallelism() + context.globalProcessorIndex()
                : null;
    }

    @Override
    public boolean complete() {
        expectedCount = (System.nanoTime() - startTime) / nsBetweenItems;
        emitFromTraverser(traverser);
        return false;
    }
}
