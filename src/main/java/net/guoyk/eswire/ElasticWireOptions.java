package net.guoyk.eswire;

/**
 * 参数对象
 */
public class ElasticWireOptions {

    /**
     * ElasticSearch 本机地址，默认为 127.0.0.1
     */
    private String host;

    /**
     * ElasticSearch Transport 端口号，默认为 9300
     */
    private int port;


    /**
     * 是否启用 ElasticSearch sniff，建议不启用，因为基本没啥用
     */
    private boolean sniff;

    /**
     * ElasticSearch 的数据目录，可以填写多个数据目录，默认值是 /var/lib/elasticsearch
     */
    private String[] dataDirs;

    /**
     * nodeId 用于辨别所有分段是否已经迁移到了当前索引
     */
    private String nodeId;

    /**
     * 当前节点独有的 node.attr 键值对，用于将索引的所有主分片强制迁移到当前主机，默认为 eswire=yup
     */
    private String nodeAttrKey;

    private String nodeAttrValue;

    public ElasticWireOptions() {
        this.host = "127.0.0.1";
        this.nodeId = "";
        this.port = 9300;
        this.sniff = false;
        this.dataDirs = new String[]{"/var/lib/elasticsearch"};
        this.nodeAttrKey = "eswire";
        this.nodeAttrValue = "yup";
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }


    public boolean isSniff() {
        return sniff;
    }

    public void setSniff(boolean sniff) {
        this.sniff = sniff;
    }

    public String[] getDataDirs() {
        return dataDirs;
    }

    public void setDataDirs(String[] dataDirs) {
        this.dataDirs = dataDirs;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeAttrKey() {
        return nodeAttrKey;
    }

    public void setNodeAttrKey(String nodeAttrKey) {
        this.nodeAttrKey = nodeAttrKey;
    }

    public String getNodeAttrValue() {
        return nodeAttrValue;
    }

    public void setNodeAttrValue(String nodeAttrValue) {
        this.nodeAttrValue = nodeAttrValue;
    }

}
