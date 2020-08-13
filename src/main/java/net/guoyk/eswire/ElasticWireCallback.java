package net.guoyk.eswire;

/**
 * 回调接口，用户可以实现该接口，对读取到的文档进行处理
 */
public interface ElasticWireCallback {

    /**
     * 用户实现接口，对读取到的文档进行处理
     *
     * @param bytes 文档内容，基本上就是原始 JSON 字节
     * @param id    当前文档 ID
     * @param total 全部文档数量
     * @return 是否继续扫描文档
     */
    boolean handleDocumentSource(byte[] bytes, long id, long total);

}
