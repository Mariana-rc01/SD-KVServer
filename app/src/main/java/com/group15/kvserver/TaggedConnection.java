package com.group15.kvserver;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaggedConnection implements AutoCloseable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Lock sendLock = new ReentrantLock();
    private final Lock receiveLock = new ReentrantLock();

    public static class Frame {
        public final int tag;
        public final byte[] data;

        public Frame(int tag, byte[] data) {
            this.tag = tag;
            this.data = data;
        }
    }

    public TaggedConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public void send(Frame frame) throws IOException {
        send(frame.tag, frame.data);
    }

    public void send(int tag, byte[] data) throws IOException {
        sendLock.lock();
        try {
            out.writeInt(tag); 
            out.writeInt(data.length); 
            out.write(data); 
            out.flush();
        } finally {
            sendLock.unlock();
        }
    }

    public Frame receive() throws IOException {
        receiveLock.lock();
        try {
            int tag = in.readInt(); 
            int length = in.readInt(); 
            byte[] data = new byte[length];
            in.readFully(data); 
            return new Frame(tag, data);
        } finally {
            receiveLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}