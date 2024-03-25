/*
* MetaTags.java
 */
package nz.co.spark.cg.extractor.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * Model class to store recording metatags returned by Dubber.
 * @author rod
 * @since 2018-12-05
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaTags {
    @JsonProperty("recording-platform")
    private String recordingPlatform;
    @JsonProperty("original-filename")
    private String originalFileName;
    @JsonProperty("external-tracking-ids")
    private String externalTrakingIds;
    @JsonProperty("recorder-call-id")
    private String recorderCallId;
    @JsonProperty("recorder-identifier")
    private String recorderIdentifier;
    @JsonProperty("external-call-id")
    private String externalCallId;

    @JsonGetter("recording-platform")
    public String getRecordingPlatform() {
        return recordingPlatform;
    }

    @JsonSetter("recording-platform")
    public void setRecordingPlatform(String recordingPlatform) {
        this.recordingPlatform = recordingPlatform;
    }

    @JsonGetter("original-filename")
    public String getOriginalFileName() {
        return originalFileName;
    }

    @JsonSetter("original-filename")
    public void setOriginalFileName(String originalFileName) {
        this.originalFileName = originalFileName;
    }

    @JsonGetter("external-tracking-ids")
    public String getExternalTrakingIds() {
        return externalTrakingIds;
    }

    @JsonSetter("external-tracking-ids")
    public void setExternalTrakingIds(String externalTrakingIds) {
        this.externalTrakingIds = externalTrakingIds;
    }

    @JsonGetter("recorder-call-id")
    public String getRecorderCallId() {
        return recorderCallId;
    }

    @JsonSetter("recorder-call-id")
    public void setRecorderCallId(String recorderCallId) {
        this.recorderCallId = recorderCallId;
    }

    @JsonGetter("recorder-identifier")
    public String getRecorderIdentifier() {
        return recorderIdentifier;
    }

    @JsonSetter("recorder-identifier")
    public void setRecorderIdentifier(String recorderIdentifier) {
        this.recorderIdentifier = recorderIdentifier;
    }

    @JsonGetter("external-call-id")
    public String getExternalCallId() {
        return externalCallId;
    }

    @JsonSetter("external-call-id")
    public void setExternalCallId(String externalCallId) {
        this.externalCallId = externalCallId;
    }

    @Override
    public String toString() {
        return "MetaTags{" +
                "recordingPlatform='" + recordingPlatform + '\'' +
                ", originalFileName='" + originalFileName + '\'' +
                ", externalTrakingIds='" + externalTrakingIds + '\'' +
                ", recorderCallId='" + recorderCallId + '\'' +
                ", recorderIdentifier='" + recorderIdentifier + '\'' +
                ", externalCallId='" + externalCallId + '\'' +
                '}';
    }
}
