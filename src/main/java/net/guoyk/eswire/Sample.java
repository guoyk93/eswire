package net.guoyk.eswire;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Sample {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Yaml yaml = new Yaml(new Constructor(ElasticWireOptions.class));
        ElasticWireOptions options = yaml.load(Sample.class.getClassLoader().getResourceAsStream("net.guoyk.eswire.sample.yml"));
        ElasticWire elasticWire = new ElasticWire(options);
        elasticWire.export(args[0], null);
    }

}
