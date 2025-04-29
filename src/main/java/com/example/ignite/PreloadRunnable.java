package com.example.ignite;

import java.util.Random;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;

public class PreloadRunnable implements IgniteRunnable {
    private static final long serialVersionUID = 1L;

    private final int jobId;
    private final Random random = new Random();

    public PreloadRunnable(int jobId) {
        this.jobId = jobId;
    }

    @Override
    public void run() {
        try (IgniteDataStreamer<String, Integer> streamer = Ignition
                .ignite(IgniteServerMain.IGNITE_INSTANCE_NAME)
                .dataStreamer(IgniteServerMain.CACHE_NAME)) {
            
            for (int v = 0; v < 10_000; v++) {
                char randomLetter = (char) ('A' + random.nextInt(26));
                String k = randomLetter + "-" + String.format("%06d", jobId) + "-" + String.format("%06d", v);
                streamer.addData(k, v);
            }
        }
    }
}
