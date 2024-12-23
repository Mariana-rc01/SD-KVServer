package com.group15.kvservertests;

import com.group15.kvserver.ClientLibrary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;


public class Runner {
    private static final String HOST = "localhost";
    private static final int PORT = 12345;

    public static void main(String[] args) {
        try {
            Runner runner = new Runner();
            boolean result = runner.init();

            if (!result) {
                Logger.log("Can't initialize test environment. Server needs to be active!", Logger.LogLevel.ERROR);
                return;
            }

            runner.workload1();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean init() throws IOException {
        try {
            ClientLibrary client = new ClientLibrary(HOST, PORT);
            client.close();
            return true;
        } catch (Exception e) {
            Logger.log("Initialization failed: " + e.getMessage(), Logger.LogLevel.ERROR);
            return false;
        }
    }

    public void workload1() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        
        Logger.log("Populating database.", Logger.LogLevel.INFO);
        for (int i = 1; i <= 1000; i++) {
            put("key" + i, ("value" + i).getBytes());
        }

        for (int i = 1; i <= 1000; i++) {
            final String key = "key" + i;
            final String value = "value" + i;
            executorService.submit(() -> {
                try {
                    long startTime = System.nanoTime();
                    byte[] returnedValue = get(key);
                    long endTime = System.nanoTime();
                    long duration = endTime - startTime;
                    long timestamp = System.currentTimeMillis();

                    // Verify the value
                    if (returnedValue != null) {
                        if (!value.equals(new String(returnedValue))) {
                            Logger.log("Value mismatch for key " + key, Logger.LogLevel.ERROR);
                        }
                    }

                    synchronized (responseTimes) {
                        responseTimes.add(duration);
                        timestamps.add(timestamp);
                    }
                } catch (IOException e) {
                    Logger.log("Get failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
                }
            });
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        generateGraph(responseTimes, timestamps);
    }

    public void put(String key, byte[] value) throws IOException {
        ClientLibrary client = new ClientLibrary(HOST, PORT);
        try {
            client.put(key, value);
            client.close();
        } catch (IOException e) {
            Logger.log("Put failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
        }
    }

    public byte[] get(String key) throws IOException {
        ClientLibrary client = new ClientLibrary(HOST, PORT);
        try {
            byte[] value = client.get(key);
            client.close();
            return value;
        } catch (IOException e) {
            Logger.log("Get failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
        }

        return null;
    }

    public void generateGraph(List<Long> responseTimes, List<Long> timestamps) {
        XYSeries series = new XYSeries("Response Time");
    
        for (int i = 0; i < responseTimes.size(); i++) {
            series.add((Number)(timestamps.get(i) / 1_000_000.0), (Number)(responseTimes.get(i) / 1_000_000.0));
        }
    
        XYSeriesCollection dataset = new XYSeriesCollection(series);
    
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Server Response Time Over Time",
                "Time (ms)",
                "Response Time (ms)",
                dataset
        );
    
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));
        javax.swing.JFrame frame = new javax.swing.JFrame();
        frame.setDefaultCloseOperation(javax.swing.JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(chartPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
