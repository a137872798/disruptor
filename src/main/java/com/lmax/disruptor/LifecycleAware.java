/*
 * Copyright 2011 LMAX Ltd.
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
package com.lmax.disruptor;

/**
 * Implement this interface in your {@link EventHandler} to be notified when a thread for the
 * 代表某个对象对生命周期可感知
 * {@link BatchEventProcessor} starts and shuts down.
 */
public interface LifecycleAware
{
    /**
     * Called once on thread start before first event is available.
     * 当线程启动时触发
     */
    void onStart();

    /**
     * <p>Called once just before the thread is shutdown.</p>
     * <p>
     * Sequence event processing will already have stopped before this method is called. No events will
     * be processed after this message.
     * 当线程被关闭时触发
     */
    void onShutdown();
}
