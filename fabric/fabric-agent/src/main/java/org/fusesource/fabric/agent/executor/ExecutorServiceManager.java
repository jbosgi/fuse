package org.fusesource.fabric.agent.executor;


import java.io.Closeable;
import java.util.concurrent.ExecutorService;

public interface ExecutorServiceManager extends Closeable {

    ExecutorService get();
}
