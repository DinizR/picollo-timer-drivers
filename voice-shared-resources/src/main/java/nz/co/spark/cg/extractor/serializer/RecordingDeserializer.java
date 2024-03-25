package nz.co.spark.cg.extractor.serializer;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import nz.co.spark.cg.extractor.model.MetaTags;
import nz.co.spark.cg.extractor.model.Recording;

import java.io.IOException;

public class RecordingDeserializer extends StdDeserializer<Recording> {
    public RecordingDeserializer() {
        this(null);
    }

    public RecordingDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Recording deserialize(com.fasterxml.jackson.core.JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, com.fasterxml.jackson.core.JsonProcessingException {
        final Recording recording = new Recording();
        final MetaTags metaTags = new MetaTags();
        final JsonNode rn = jsonParser.getCodec().readTree(jsonParser);

        recording.setId(getNodeValue(rn,"id"));
        recording.setTo(getNodeValue(rn,"to"));
        recording.setTo_label(getNodeValue(rn,"to_label"));
        recording.setFrom(getNodeValue(rn,"from"));
        recording.setFrom(getNodeValue(rn,"from_label"));
        recording.setCall_type(getNodeValue(rn,"call_type"));
        recording.setRecording_type(getNodeValue(rn,"recording_type"));
        recording.setRecording_url(getNodeValue(rn,"recording_url"));
        recording.setChannel(getNodeValue(rn,"channel"));
        recording.setStart_time(getNodeValue(rn,"start_time"));
        recording.setDate_created(getNodeValue(rn,"date_created"));
        recording.setDate_updated(getNodeValue(rn,"date_updated"));
        recording.setDub_point_id(getNodeValue(rn,"dub_point_id"));
        recording.setSelf(getNodeValue(rn,"self"));
        recording.setDuration(Integer.valueOf(getNodeValue(rn,"duration")));
        metaTags.setRecordingPlatform(getNodeValue(rn.get("meta_tags"),"recording-platform"));
        metaTags.setOriginalFileName(getNodeValue(rn.get("meta_tags"),"original-filename"));
        metaTags.setExternalTrakingIds(getNodeValue(rn.get("meta_tags"),"external-tracking-ids"));
        metaTags.setRecorderCallId(getNodeValue(rn.get("meta_tags"),"recorder-call-id"));
        metaTags.setRecorderIdentifier(getNodeValue(rn.get("meta_tags"),"recorder-identifier"));
        metaTags.setExternalCallId(getNodeValue(rn.get("meta_tags"),"external-call-id"));

        recording.setMetaTags(metaTags);

        return recording;
    }

    private String getNodeValue(JsonNode jsonNode,String key) {
        return jsonNode == null || jsonNode.get(key) == null ? "" : (jsonNode.get(key).textValue() == null ? jsonNode.get(key).toString() : jsonNode.get(key).textValue());
    }
}