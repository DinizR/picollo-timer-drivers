/*
* Recording.java
 */
package nz.co.spark.cg.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Model class to store recording returned by Dubber.
 * @author rod
 * @since 2018-12-05
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Recording {
    @JsonProperty("self")
    private String self;
    @JsonProperty("id")
    private String id;
    @JsonProperty("to")
    private String to;
    @JsonProperty("to_label")
    private String to_label;
    @JsonProperty("from")
    private String from;
    @JsonProperty("from_label")
    private String from_label;
    @JsonProperty("call_type")
    private String call_type;
    @JsonProperty("recording_type")
    private String recording_type;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("start_time")
    private String start_time;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("expiry_time")
    private String expiry_time;
    @JsonProperty("dub_point_id")
    private String dub_point_id;
    private MetaTags metaTags;
    @JsonProperty("date_created")
    private String date_created;
    @JsonProperty("date_updated")
    private String date_updated;
    @JsonProperty("recording_url")
    private String recording_url;

    public String getSelf() {
        return self;
    }

    public void setSelf(String self) {
        this.self = self;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getTo_label() {
        return to_label;
    }

    public void setTo_label(String to_label) {
        this.to_label = to_label;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getFrom_label() {
        return from_label;
    }

    public void setFrom_label(String from_label) {
        this.from_label = from_label;
    }

    public String getCall_type() {
        return call_type;
    }

    public void setCall_type(String call_type) {
        this.call_type = call_type;
    }

    public String getRecording_type() {
        return recording_type;
    }

    public void setRecording_type(String recording_type) {
        this.recording_type = recording_type;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public ZonedDateTime getStart_time() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss Z");
        return start_time == null ? null : ZonedDateTime.parse(start_time,dateTimeFormatter);
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public ZonedDateTime getExpiry_time() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss Z");
        return expiry_time == null ? null : ZonedDateTime.parse(expiry_time,dateTimeFormatter);
    }

    public void setExpiry_time(String expiry_time) {
        this.expiry_time = expiry_time;
    }

    public String getDub_point_id() {
        return dub_point_id;
    }

    public void setDub_point_id(String dub_point_id) {
        this.dub_point_id = dub_point_id;
    }

    public MetaTags getMetaTags() {
        return metaTags;
    }

    public void setMetaTags(MetaTags metaTags) {
        this.metaTags = metaTags;
    }

    public ZonedDateTime getDate_created() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss Z");
        return date_created == null ? null : ZonedDateTime.parse(date_created,dateTimeFormatter);
    }

    public void setDate_created(String date_created) {
        this.date_created = date_created;
    }

    public ZonedDateTime getDate_updated() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss Z");
        return date_updated == null ? null : ZonedDateTime.parse(date_updated,dateTimeFormatter);
    }

    public void setDate_updated(String date_updated) {
        this.date_updated = date_updated;
    }

    public String getRecording_url() {
        return recording_url;
    }

    public void setRecording_url(String recording_url) {
        this.recording_url = recording_url;
    }


    @Override
    public String toString() {
        return "Recording{" +
                "self='" + self + '\'' +
                ", id='" + id + '\'' +
                ", to='" + to + '\'' +
                ", from='" + from + '\'' +
                ", from_label='" + from_label + '\'' +
                ", call_type='" + call_type + '\'' +
                ", recording_type='" + recording_type + '\'' +
                ", channel='" + channel + '\'' +
                ", start_time=" + start_time +
                ", duration=" + duration +
                ", expiry_time=" + expiry_time +
                ", dub_point_id='" + dub_point_id + '\'' +
                ", metaTags=" + metaTags +
                ", date_created=" + date_created +
                ", date_updated=" + date_updated +
                ", recording_url='" + recording_url + '\'' +
                '}';
    }
}