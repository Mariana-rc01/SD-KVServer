package com.group15.kvserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ClientLibrary {
    private TaggedConnection taggedConnection;
    private Demultiplexer demultiplexer;
    private final ReentrantLock lock = new ReentrantLock();

    public Map<Integer, Condition> conditionsMap = new HashMap<>();
    public Map<Integer, byte[]> responsesMap = new HashMap<>();

    public ClientLibrary(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        taggedConnection = new TaggedConnection(socket);
        demultiplexer = new Demultiplexer(taggedConnection);
        //new Thread(() -> demultiplexer.reader()).start();
    }

    private byte[] sendWithTag(short requestType, byte[] requestData) throws IOException {
        lock.lock();
        try {
            TaggedConnection.Frame frame = new TaggedConnection.Frame(0, requestType, requestData);
            taggedConnection.send(frame.tag, requestType, requestData);
            
            try {
                return demultiplexer.receive(frame.tag);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for response", e);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean authenticate(String username, String password) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de autenticação com as credenciais
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.AuthRequest.getValue());
                dos.writeUTF(username);
                dos.writeUTF(password);
                requestData = baos.toByteArray();
            }
            System.out.println("Sending authentication request");
            byte[] responseData = sendWithTag(RequestType.AuthRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                return dis.readBoolean();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean register(String username, String password) throws IOException {
        lock.lock();
        try {
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.RegisterRequest.getValue());
                dos.writeUTF(username);
                dos.writeUTF(password);
                requestData = baos.toByteArray();
            }
            byte[] responseData = sendWithTag(RequestType.RegisterRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                return dis.readBoolean();
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de inserção com a chave e o valor
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.PutRequest.getValue());
                dos.writeUTF(key);
                dos.writeInt(value.length);
                dos.write(value);
                requestData = baos.toByteArray();
            }
            System.out.println("Sending put request for key: " + key);
            sendWithTag(RequestType.PutRequest.getValue(), requestData);
        } finally {
            lock.unlock();
        }
    }

    public byte[] get(String key) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de obtenção com a chave
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.GetRequest.getValue());
                dos.writeUTF(key);
                requestData = baos.toByteArray();
            }
            byte[] responseData = sendWithTag(RequestType.GetRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                DataInputStream dis = new DataInputStream(bais)) {
                int length = dis.readInt();
                if (length < 0) return null;
                byte[] data = new byte[length];
                dis.readFully(data);
                return data;
            }
        } finally {
            lock.unlock();
        }
    }

    public void multiPut(Map<String, byte[]> pairs) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de inserção múltipla com os pares chave-valor
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.MultiPutRequest.getValue());
                dos.writeInt(pairs.size());
                for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
                    dos.writeUTF(entry.getKey());
                    dos.writeInt(entry.getValue().length);
                    dos.write(entry.getValue());
                }
                requestData = baos.toByteArray();
            }
            sendWithTag(RequestType.MultiPutRequest.getValue(), requestData);
        } finally {
            lock.unlock();
        }
    }

    public Map<String, byte[]> multiGet(Set<String> keys) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de obtenção múltipla com as chaves
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.MultiGetRequest.getValue());
                dos.writeInt(keys.size());
                for (String key : keys) {
                    dos.writeUTF(key);
                }
                requestData = baos.toByteArray();
            }
            byte[] responseData = sendWithTag(RequestType.MultiGetRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                int n = dis.readInt();
                Map<String, byte[]> result = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    String key = dis.readUTF();
                    int length = dis.readInt();
                    byte[] data = new byte[length];
                    dis.readFully(data);
                    result.put(key, data);
                }
                return result;
            }
        } finally {
            lock.unlock();
        }
    }

    public byte[] getWhen(String key, String keyCond, byte[] valueCond) throws IOException, InterruptedException {
        int tag = key.hashCode() ^ keyCond.hashCode() ^ Arrays.hashCode(valueCond);

        lock.lock();
        try {
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.GetWhenRequest.getValue());
                dos.writeUTF(key);
                dos.writeUTF(keyCond);
                dos.writeInt(valueCond.length);
                dos.write(valueCond);
                requestData = baos.toByteArray();
            }

            Condition condition = lock.newCondition();
            conditionsMap.put(tag, condition);

            try {
                byte[] responseData = sendWithTag(RequestType.GetWhenRequest.getValue(), requestData);
                if (responseData.length == 0) {
                    System.out.println("Waiting for response 1"); // Print para debug (remover após funcionar)
                    while ((responseData = demultiplexer.receive(tag)).length == 0) { // Operação bloqueante
                        System.out.println("Waiting for response 2"); // Print para debug (remover após funcionar)
                    }
                    System.out.println("Received response"); // Print para debug (remover após funcionar)
                }
                conditionsMap.remove(tag);
                return responseData;
            } catch (IOException e) {
                throw new IOException("Error during sendWithTag", e);
            }
        } finally {
            lock.unlock();
        }
    }

    public void addResponse(int tag, byte[] response) {
        lock.lock();
        try {
            responsesMap.put(tag, response);
            if (conditionsMap.containsKey(tag)) {
                conditionsMap.get(tag).signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        lock.lock();
        try {
            sendDisconnectMessage();
            demultiplexer.close();
        } finally {
            lock.unlock();
        }
    }

    public void sendDisconnectMessage() throws IOException {
        lock.lock();
        try {
            byte[] disconnect;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.DisconnectRequest.getValue());
                disconnect = baos.toByteArray();
            }
            sendWithTag(RequestType.DisconnectRequest.getValue(), disconnect);
        } finally {
            lock.unlock();
        }
    }
    
}
