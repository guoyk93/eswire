package net.guoyk.eswire;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ElasticWire implements Closeable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticWire.class);

    private final TransportClient client;

    private final ElasticWireOptions options;

    public ElasticWire(ElasticWireOptions options) throws UnknownHostException {
        this.options = options;
        Settings settings = Settings.builder()
                .put("client.transport.ignore_cluster_name", true)
                .build();
        this.client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(options.getHost()), 9300));
    }

    public void export(String index, ElasticWireCallback callback) throws ExecutionException, InterruptedException, IOException {
        // open and wait for all active shards
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
        openIndexRequest.waitForActiveShards(ActiveShardCount.ALL);
        this.client.admin().indices().open(openIndexRequest).get();
        LOGGER.info("open index: {}", index);
        // force merge
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index);
        forceMergeRequest.maxNumSegments(1);
        this.client.admin().indices().forceMerge(forceMergeRequest).get();
        LOGGER.info("force merge index: {}", index);
        // transfer
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
        Settings.Builder settingsBuilder = Settings.builder();
        for (Map.Entry<String, String> entry : this.options.getNodeAttrs().entrySet()) {
            settingsBuilder = settingsBuilder.put("index.routing.allocation.require." + entry.getKey(), entry.getValue());
        }
        updateSettingsRequest.settings(settingsBuilder.build());
        this.client.admin().indices().updateSettings(updateSettingsRequest).get();
        LOGGER.info("update index settings: {}", index);
        // wait for recovery
        for (; ; ) {
            //noinspection BusyWait
            Thread.sleep(5000);
            RecoveryRequest recoveryRequest = new RecoveryRequest(index);
            recoveryRequest.activeOnly(true);
            RecoveryResponse recoveryResponse = this.client.admin().indices().recoveries(recoveryRequest).get();
            if (recoveryResponse.hasRecoveries()) {
                if (recoveryResponse.shardRecoveryStates().get(index).isEmpty()) {
                    break;
                }
            } else {
                break;
            }
        }
        LOGGER.info("index recovered: {}", index);
        // get uuid
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(index);
        GetSettingsResponse getSettingsResponse = this.client.admin().indices().getSettings(getSettingsRequest).get();
        String uuid = getSettingsResponse.getSetting(index, "index.uuid");
        LOGGER.info("index uuid: {} = {}", index, uuid);
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(index);
        IndicesStatsResponse indicesStatsResponse = this.client.admin().indices().stats(indicesStatsRequest).get();
        // get segments
        long totalDocs = 0;
        HashMap<Integer, String> shardToSegments = new HashMap<>();
        IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest(index);
        IndicesSegmentResponse indicesSegmentResponse = this.client.admin().indices().segments(indicesSegmentsRequest).get();
        for (Map.Entry<Integer, IndexShardSegments> entry : indicesSegmentResponse.getIndices().get(index).getShards().entrySet()) {
            Integer shardId = entry.getKey();
            IndexShardSegments shards = entry.getValue();
            for (ShardSegments shard : shards) {
                if (shard.getShardRouting().primary()) {
                    for (Segment segment : shard.getSegments()) {
                        if (shardToSegments.get(shardId) != null) {
                            throw new IllegalStateException("more than 1 segment per shard");
                        }
                        shardToSegments.put(shardId, segment.getName());
                        totalDocs += segment.getNumDocs();
                    }
                }
            }
        }
        LOGGER.info("index segments: {}", shardToSegments);
        LOGGER.info("index total: {}", totalDocs);
        if (totalDocs != indicesStatsResponse.getTotal().docs.getCount()) {
            throw new IllegalStateException("segment docs count sum != index docs count");
        }
        // close index
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(index);
        this.client.admin().indices().close(closeIndexRequest).get();
        LOGGER.info("index closed: {}", index);
        // search files
        Map<Integer, String> shardToDirs = new HashMap<>();
        for (String dataDir : this.options.getDataDirs()) {
            shardToSegments.forEach((shardId, segmentName) -> {
                Path path = Paths.get(dataDir, "nodes", "0", "indices", uuid, shardId.toString(), "index");
                if (path.toFile().exists()) {
                    if (shardToDirs.get(shardId) != null) {
                        throw new IllegalStateException("more than 1 dir per shard");
                    }
                    shardToDirs.put(shardId, path.toString());
                }
            });
        }
        if (shardToDirs.size() != shardToSegments.size()) {
            throw new IllegalStateException("missing shard dirs");
        }
        LOGGER.info("index shard directories: {}", shardToDirs);

        for (Map.Entry<Integer, String> entry : shardToDirs.entrySet()) {
            Integer shard = entry.getKey();
            String dir = entry.getValue();
            DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(Paths.get(dir)));
            LOGGER.info("index {} shard {} docs count {}", index, shard, reader.numDocs());
            int limit = 5;
            if (limit > reader.numDocs()) {
                limit = reader.numDocs();
            }
            for (int i = 0; i < limit; i++) {
                Document doc = reader.document(i);
                LOGGER.info("source = {}", doc.get("_source"));
            }
            reader.close();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
        }
    }

}
