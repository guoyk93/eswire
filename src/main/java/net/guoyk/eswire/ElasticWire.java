package net.guoyk.eswire;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ElasticWire implements Closeable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticWire.class);

    private final TransportClient client;

    private final ElasticWireOptions options;

    public ElasticWire(ElasticWireOptions options) throws IOException {
        this.options = options;
        // 创建 Transport 客户端
        Settings settings = Settings.builder()
                .put("client.transport.ignore_cluster_name", true)
                .put("client.transport.sniff", options.isSniff())
                .build();
        this.client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(options.getHost()), options.getPort()));
    }

    public void export(String index, ElasticWireCallback callback) throws ExecutionException, InterruptedException, IOException {
        // 打开，并等待所有分片激活
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
        openIndexRequest.waitForActiveShards(ActiveShardCount.ALL);
        this.client.admin().indices().open(openIndexRequest).get();
        LOGGER.info("[eswire: {}] index opened", index);
        // 强制合并所有分片的段数到 1
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index);
        forceMergeRequest.maxNumSegments(1);
        this.client.admin().indices().forceMerge(forceMergeRequest).get();
        LOGGER.info("[eswire: {}] index force merged", index);
        // 强制设置路由规则，将所有主分片迁移到本机
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
        Settings.Builder settingsBuilder = Settings.builder()
                .put("index.routing.allocation.require." + this.options.getNodeAttrKey(), this.options.getNodeAttrValue());
        updateSettingsRequest.settings(settingsBuilder.build());
        this.client.admin().indices().updateSettings(updateSettingsRequest).get();
        LOGGER.info("[eswire: {}] index routing updated", index);
        // 强制执行集群重新路由
        this.client.admin().cluster().reroute(new ClusterRerouteRequest()).get();
        // 等待分片迁移完成
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
        LOGGER.info("[eswire: {}] index recovered", index);
        // 获取索引 UUID
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(index);
        GetSettingsResponse getSettingsResponse = this.client.admin().indices().getSettings(getSettingsRequest).get();
        String uuid = getSettingsResponse.getSetting(index, "index.uuid");
        LOGGER.info("[eswire: {}] index uuid = {}", index, uuid);
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(index);
        IndicesStatsResponse indicesStatsResponse = this.client.admin().indices().stats(indicesStatsRequest).get();
        // 获取分段信息
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
                            throw new IllegalStateException("got more than 1 segment per shard");
                        }
                        shardToSegments.put(shardId, segment.getName());
                        totalDocs += segment.getNumDocs();
                    }
                }
            }
        }
        LOGGER.info("[eswire: {}] index segments = {}", index, shardToSegments);
        LOGGER.info("[eswire: {}] index segments total docs = {}", index, totalDocs);
        if (totalDocs != indicesStatsResponse.getTotal().docs.getCount()) {
            throw new IllegalStateException("segment docs count sum != index docs count");
        }
        // 完成所有信息收集，关闭索引，防止冲突
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(index);
        this.client.admin().indices().close(closeIndexRequest).get();
        LOGGER.info("[eswire: {}] index closed", index);
        // 从本地路径搜索索引数据目录
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
        LOGGER.info("[eswire: {}] index shard directories = {}", index, shardToDirs);
        if (shardToDirs.size() != shardToSegments.size()) {
            throw new IllegalStateException("missing shard dirs");
        }
        // 使用 Lucene 库读取所有分片的数据目录
        long baseCount = 0;
        for (Map.Entry<Integer, String> entry : shardToDirs.entrySet()) {
            DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(Paths.get(entry.getValue())));
            boolean stop = false;
            // 遍历所有文档
            for (int i = 0; i < reader.numDocs(); i++) {
                BytesRef source = reader.document(i).getField("_source").binaryValue();
                if (source != null) {
                    if (!callback.handleDocumentSource(source.bytes, baseCount + i, totalDocs)) {
                        stop = true;
                    }
                } else {
                    if (!callback.handleDocumentSource(null, baseCount + i, totalDocs)) {
                        stop = true;
                    }
                }
                if (stop) {
                    break;
                }
            }
            // 累加计数基数
            baseCount += reader.numDocs();
            reader.close();
            // 如果取消，则退出不在遍历
            if (stop) {
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
        }
    }

}
