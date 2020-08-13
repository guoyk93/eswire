package net.guoyk.eswire;

import java.util.Map;

public class ElasticWireOptions {

    private String host;

    private String[] dataDirs;

    private Map<String, String> nodeAttrs;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String[] getDataDirs() {
        return dataDirs;
    }

    public void setDataDirs(String[] dataDirs) {
        this.dataDirs = dataDirs;
    }

    public Map<String, String> getNodeAttrs() {
        return nodeAttrs;
    }

    public void setNodeAttrs(Map<String, String> nodeAttrs) {
        this.nodeAttrs = nodeAttrs;
    }

}
