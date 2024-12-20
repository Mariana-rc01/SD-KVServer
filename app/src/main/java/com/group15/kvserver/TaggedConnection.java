package com.group15.kvserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaggedConnection implements AutoCloseable {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private Lock rlock;
    private Lock wlock;

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
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        rlock = new ReentrantLock();
        wlock = new ReentrantLock();
    }

    public void send(Frame frame) throws IOException {
        wlock.lock();
        try {
            out.writeInt(frame.tag);
            out.writeInt(frame.data.length);
            out.write(frame.data);
            out.flush();
        } finally {
            wlock.unlock();
        }
    }

    public void send(int tag, byte[] data) throws IOException {
        send(new Frame(tag, data));
    }

    public Frame receive() throws IOException {
        rlock.lock();
        try {
            int tag = in.readInt();
            int len = in.readInt();
            byte[] data = new byte[len];
            in.readFully(data);
            return new Frame(tag, data);
        } finally {
            rlock.unlock();
        }
    }

    public void close() throws IOException {
        socket.close();
    }
}