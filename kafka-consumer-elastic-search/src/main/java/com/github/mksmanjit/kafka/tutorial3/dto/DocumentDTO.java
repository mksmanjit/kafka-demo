package com.github.mksmanjit.kafka.tutorial3.dto;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpSerializer;
import jakarta.json.stream.JsonGenerator;

import java.io.Serializable;

public class DocumentDTO implements Serializable {
    public DocumentDTO(String key, String value) {
        this.key = key;
        this.value = value;
    }

    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
