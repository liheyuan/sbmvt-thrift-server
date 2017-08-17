/**
 * @(#)ThriftServerRunnable.java, Jul 31, 2017.
 * <p>
 * Copyright 2017 fenbi.com. All rights reserved.
 * FENBI.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.coder4.sbmvt.thrift.server.configuration;

import com.netflix.discovery.EurekaClient;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author coder4
 */
@Configuration
@ConditionalOnBean(value = TProcessor.class, name = "thriftProcessor")
@EnableEurekaClient()
public class ThriftServerConfiguration implements InitializingBean, DisposableBean, Runnable {

    private static final int THRIFT_PORT = 3000;

    private static final int THRIFT_TIMEOUT = 5000;

    private static final int THRIFT_TCP_BACKLOG = 5000;

    private static final int THRIFT_CORE_THREADS = 128;

    private static final int THRIFT_MAX_THREADS = 256;

    private static final int THRIFT_SELECTOR_THREADS = 16;

    private static final TProtocolFactory THRIFT_PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

    // 16MB
    private static final int THRIFT_MAX_FRAME_SIZE = 16 * 1024 * 1024;

    // 4MB
    private static final int THRIFT_MAX_READ_BUF_SIZE = 4 * 1024 * 1024;

    protected ExecutorService threadPool;

    protected TServer server;

    protected Thread thread;

    @Autowired
    private TProcessor processor;

    @Autowired
    private EurekaClient eurekaClient;

    public TServer build() throws TTransportException {
        TNonblockingServerSocket.NonblockingAbstractServerSocketArgs socketArgs =
                new TNonblockingServerSocket.NonblockingAbstractServerSocketArgs();
        socketArgs.port(THRIFT_PORT);
        socketArgs.clientTimeout(THRIFT_TIMEOUT);
        socketArgs.backlog(THRIFT_TCP_BACKLOG);

        TNonblockingServerTransport transport = new TNonblockingServerSocket(socketArgs);

        threadPool =
                new ThreadPoolExecutor(THRIFT_CORE_THREADS, THRIFT_MAX_THREADS,
                        60L, TimeUnit.SECONDS,
                        new SynchronousQueue<>());

        TTransportFactory transportFactory = new TFramedTransport.Factory(THRIFT_MAX_FRAME_SIZE);
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport)
                .selectorThreads(THRIFT_SELECTOR_THREADS)
                .executorService(threadPool)
                .transportFactory(transportFactory)
                .inputProtocolFactory(THRIFT_PROTOCOL_FACTORY)
                .outputProtocolFactory(THRIFT_PROTOCOL_FACTORY)
                .processor(processor);

        args.maxReadBufferBytes = THRIFT_MAX_READ_BUF_SIZE;

        return new TThreadedSelectorServer(args);
    }

    private boolean isDestroy = false;

    @Override
    public void run() {
        try {
            server = build();
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Start Thrift RPC Server Exception");
        }
    }

    @Override
    public void destroy() throws Exception {
        // Unregister from eureka server & Sleep for 6 seconds
        eurekaClient.shutdown();
        Thread.sleep(TimeUnit.SECONDS.toMillis(6));

        threadPool.shutdown();
        server.stop();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        thread = new Thread(this);
        thread.start();
    }
}