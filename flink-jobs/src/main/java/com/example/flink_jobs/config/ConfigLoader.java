package com.example.flink_jobs.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;

public class ConfigLoader {
    private static final Yaml yaml = new Yaml();

    public static <T> T loadConfig(String path, Class<T> clazz) throws IOException {
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(path)) {
            return yaml.loadAs(in, clazz);
        }
    }
}
