/*
* AccessToken.java
 */
package nz.co.spark.cg.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * Model class to store access token returned by Dubber.
 * @author rod
 * @since 2018-12-05
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessToken {
    private String api;
    @JsonProperty("return_type")
    private String return_type;
    @JsonProperty("access_token")
    private String access_token;
    @JsonProperty("token_type")
    private String token_type;
    @JsonProperty("expires_in")
    private Integer expires_in;
    @JsonProperty("refresh_token")
    private String refresh_token;
    @JsonProperty("scope")
    private String scope;
    @JsonProperty("state")
    private String state;
    @JsonProperty("extended")
    private String extended;
    private LocalDateTime retrievalDate;
    private LocalDateTime expirationDate;

    public String getApi() {
        return api;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public String getReturn_type() {
        return return_type;
    }

    public void setReturn_type(String return_type) {
        this.return_type = return_type;
    }

    public String getAccess_token() {
        return access_token;
    }

    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }

    public String getToken_type() {
        return token_type;
    }

    public void setToken_type(String token_type) {
        this.token_type = token_type;
    }

    public Integer getExpires_in() {
        return expires_in;
    }

    public void setExpires_in(Integer expires_in) {
        this.expires_in = expires_in;
    }

    public String getRefresh_token() {
        return refresh_token;
    }

    public void setRefresh_token(String refresh_token) {
        this.refresh_token = refresh_token;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getExtended() {
        return extended;
    }

    public void setExtended(String extended) {
        this.extended = extended;
    }

    public LocalDateTime getRetrievalDate() {
        return retrievalDate;
    }

    public void setRetrievalDate(LocalDateTime retrievalDate) {
        this.retrievalDate = retrievalDate;
    }

    public LocalDateTime getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(LocalDateTime expirationDate) {
        this.expirationDate = expirationDate;
    }

    @Override
    public String toString() {
        return "AccessToken{" +
                "api='" + api + '\'' +
                ", return_type='" + return_type + '\'' +
                ", access_token='" + access_token + '\'' +
                ", token_type='" + token_type + '\'' +
                ", expires_in=" + expires_in +
                ", refresh_token='" + refresh_token + '\'' +
                ", scope='" + scope + '\'' +
                ", state='" + state + '\'' +
                ", extended='" + extended + '\'' +
                ", retrievalDate=" + retrievalDate +
                ", expirationDate=" + expirationDate +
                '}';
    }
}