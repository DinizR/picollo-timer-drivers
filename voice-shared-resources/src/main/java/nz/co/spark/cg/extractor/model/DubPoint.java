package nz.co.spark.cg.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Model class to store dub points returned by Dubber.
 * @author rod
 * @since 2019-05-20
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DubPoint {
    @JsonProperty(value="id")
    private String id;
    @JsonProperty(value="type")
    private String type;
    @JsonProperty(value="label")
    private String label;
    @JsonProperty(value="product")
    private String product;
    @JsonProperty(value="number")
    private String number;
    @JsonProperty(value="playback")
    private Boolean playback;
    @JsonProperty(value="external_type")
    private String externalType;
    @JsonProperty(value="service_provider")
    private String serviceProvider;
    @JsonProperty(value="external_group")
    private String external_group;
    @JsonProperty(value="external_identifier")
    private String external_identifier;
    @JsonProperty(value="status")
    private String status;
    @JsonProperty(value="date_created")
    private String date_created;
    @JsonProperty(value="date_updated")
    private String date_updated;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public Boolean getPlayback() {
        return playback;
    }

    public void setPlayback(Boolean playback) {
        this.playback = playback;
    }

    public String getExternalType() {
        return externalType;
    }

    public void setExternalType(String externalType) {
        this.externalType = externalType;
    }

    public String getServiceProvider() {
        return serviceProvider;
    }

    public void setServiceProvider(String serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    public String getExternal_group() {
        return external_group;
    }

    public void setExternal_group(String external_group) {
        this.external_group = external_group;
    }

    public String getExternal_identifier() {
        return external_identifier;
    }

    public void setExternal_identifier(String external_identifier) {
        this.external_identifier = external_identifier;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDate_created() {
        return date_created;
    }

    public void setDate_created(String date_created) {
        this.date_created = date_created;
    }

    public String getDate_updated() {
        return date_updated;
    }

    public void setDate_updated(String date_updated) {
        this.date_updated = date_updated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DubPoint dubPoints = (DubPoint) o;
        return id.equals(dubPoints.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "DubPoint{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", label='" + label + '\'' +
                ", product='" + product + '\'' +
                ", number='" + number + '\'' +
                ", playback=" + playback +
                ", externalType='" + externalType + '\'' +
                ", serviceProvider='" + serviceProvider + '\'' +
                ", external_group='" + external_group + '\'' +
                ", external_identifier='" + external_identifier + '\'' +
                ", status='" + status + '\'' +
                ", date_created='" + date_created + '\'' +
                ", date_updated='" + date_updated + '\'' +
                '}';
    }
}
