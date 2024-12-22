package com.group15.kvserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Demultiplexer implements AutoCloseable {
    private final TaggedConnection conn;
    private final Map<Integer, BlockingQueue<byte[]>> queues = new ConcurrentHashMap<>();
    private final Thread readerThread;
    private volatile boolean closed = false;

    public Demultiplexer(TaggedConnection conn) {
        this.conn = conn;
        this.readerThread = new Thread(this::reader);
        this.readerThread.start();
    }

    private void reader() {
        try {
            while (!closed) {
                TaggedConnection.Frame frame = conn.receive();
                BlockingQueue<byte[]> queue = queues.computeIfAbsent(frame.tag, k -> new ArrayBlockingQueue<>(1024));
                queue.put(frame.data);
            }
        } catch (IOException | InterruptedException e) {
            if (!closed) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        if (!readerThread.isAlive()) {
            readerThread.start();
        }
    }

    public void send(TaggedConnection.Frame frame) throws IOException {
        conn.send(frame);
    }

    public void send(int tag, byte[] data) throws IOException {
        conn.send(new TaggedConnection.Frame(tag, data));
    }

    public byte[] receive(int tag) throws IOException, InterruptedException {
        BlockingQueue<byte[]> queue = queues.computeIfAbsent(tag, k -> new ArrayBlockingQueue<>(1024));
        return queue.take();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        readerThread.interrupt();
        conn.close();
    }
}