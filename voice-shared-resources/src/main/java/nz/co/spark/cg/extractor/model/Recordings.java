/*
* Recordings.java
 */
package nz.co.spark.cg.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

/**
 * Model class to store recordings returned by Dubber.
 * @author rod
 * @since 2018-12-05
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Recordings {
    private String self;
    private ArrayList<Recording> recordings;

    @JsonProperty("self")
    public String getSelf() {
        return self;
    }

    @JsonProperty("self")
    public void setSelf(String self) {
        this.self = self;
    }

    @JsonProperty(value="meals")
    public ArrayList<Recording> getRecordings() {
        return recordings;
    }

    @JsonProperty(value= "nz/co/spark/cg/extractor/model")
    public void setRecordings(ArrayList<Recording> recordings) {
        this.recordings = recordings;
    }

    @Override
    public String toString() {
        return "Recordings{" +
                "self='" + self + '\'' +
                ", recordings=" + recordings +
                '}';
    }
}
