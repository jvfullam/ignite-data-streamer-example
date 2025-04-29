package com.example.ignite;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IgniteServerMain {
    private static final Logger log = LogManager.getLogger();

    public static final String CACHE_NAME = "Cache1";
    public static final String IGNITE_INSTANCE_NAME = "IgniteInstance1";
    private static final String NODE_ID = System.getProperty("NODE_ID");
    private static final int SERVER_NODES = 2;

    public static void main(String[] args) {
        try {
            Ignition.start(getIgniteConfiguration());

            if ("1".equals(NODE_ID)) {
                waitForServerNodesToBeAvailable();
                loadCache();
                idleVerifyLoop();
            }

            while (true) {
                sleep(10_000);
            }
        } catch (Exception e) {
            log.error("{}", e, e);
            System.exit(1);
        }
    }

    private static IgniteConfiguration getIgniteConfiguration() {
        CacheConfiguration<String, Integer> cacheConfig = new CacheConfiguration<String, Integer>()
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setName(CACHE_NAME)
                .setReadThrough(false)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        DataRegionConfiguration defaultDataRegionConfiguration = new DataRegionConfiguration()
                .setName("Default_Region")
                .setMaxSize(1L * 1024 * 1024 * 1024)
                .setInitialSize(1L * 1024 * 1024 * 1024);

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(defaultDataRegionConfiguration);

        TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();

        TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder()
                .setAddresses(List.of("127.0.0.1:47500..47509"));

        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi()
                .setIpFinder(tcpDiscoveryVmIpFinder);

        return new IgniteConfiguration()
                .setCacheConfiguration(cacheConfig)
                .setCommunicationSpi(tcpCommunicationSpi)
                .setDataStorageConfiguration(dataStorageConfiguration)
                .setDiscoverySpi(tcpDiscoverySpi)
                .setIgniteInstanceName(IGNITE_INSTANCE_NAME)
                .setIncludeEventTypes(EventType.EVT_NODE_FAILED);
    }

    private static void waitForServerNodesToBeAvailable() {
        log.info("waiting for server nodes");
        Ignite ignite = Ignition.ignite(IGNITE_INSTANCE_NAME);
        while (ignite.cluster().forServers().nodes().size() < SERVER_NODES) {
            sleep(1);
        }

        log.info("server nodes are available!");
        sleep(100);
    }

    private static void loadCache() {
        Instant start = Instant.now();
        log.info("starting {}", CACHE_NAME);

        Ignite ignite = Ignition.ignite(IGNITE_INSTANCE_NAME);
        IgniteCompute compute = ignite.compute();
        List<IgniteFuture<Void>> jobs = IntStream.range(0, 100)
                .mapToObj(PreloadRunnable::new)
                .map(compute::runAsync)
                .collect(Collectors.toList());

        jobs.forEach(IgniteFuture::get);

        Duration duration = Duration.between(start, Instant.now());
        log.info("finished: duration {}", duration);

        int size = ignite.cache(CACHE_NAME).size();
        log.info("cache {}, size {}", CACHE_NAME, size);
    }

    private static boolean idleVerify() {
        Instant start = Instant.now();

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = mbs.queryMBeans(null, null).stream()
                .filter(objectInstance -> objectInstance.toString().contains("name=IdleVerify"))
                .map(objectInstance -> objectInstance.getObjectName())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("IdleVerify: MBean not found"));

        try {
            String result = (String) mbs.invoke(objectName, "invoke",
                    new Object[] { "", "", "", "", "" },
                    new String[] {});
            log.info("IdleVerify: finished in {}", Duration.between(start, Instant.now()));

            String[] resultSplit = result.split("\\R");
            Stream.of(resultSplit).forEach(s -> log.info("IdleVerify: {}", s));
            return "The check procedure has finished, no conflicts have been found."
                    .equals(resultSplit[resultSplit.length - 1]);
        } catch (Exception e) {
            log.error(e.toString(), e);
            return false;
        }
    }

    private static void idleVerifyLoop() {
        Instant start = Instant.now();

        while (!idleVerify()) {
            if (Duration.between(start, Instant.now()).getSeconds() > 120) {
                log.error("IdleVerifyLoop: exiting, there are still conflicts after 2 minutes of waiting");
                return;
            }
            sleep(10_000);
        }

        Duration duration = Duration.between(start, Instant.now());
        log.info("IdleVerifyLoop: finished in {}", duration);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error(e.toString(), e);
            Thread.interrupted();
            throw new RuntimeException(e);
        }
    }
}
