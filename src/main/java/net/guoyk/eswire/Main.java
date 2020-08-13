package net.guoyk.eswire;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException {
        Yaml yaml = new Yaml(new Constructor(ElasticWireOptions.class));
        ElasticWireOptions options = yaml.load(Main.class.getClassLoader().getResourceAsStream("net.guoyk.eswire.local.yml"));
        ElasticWire elasticWire = new ElasticWire(options);
        elasticWire.export(args[0], null);
    }

}
