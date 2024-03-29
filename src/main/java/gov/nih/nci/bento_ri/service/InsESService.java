package gov.nih.nci.bento_ri.service;

import com.google.gson.*;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.service.RedisService;
import gov.nih.nci.bento.service.connector.AWSClient;
import gov.nih.nci.bento.service.connector.AbstractClient;
import gov.nih.nci.bento.service.connector.DefaultClient;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.chrono.AssembledChronology.Fields;
import org.opensearch.client.*;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

@Service("InsESService")
public class InsESService extends ESService {
    public static final String SCROLL_ENDPOINT = "/_search/scroll";
    public static final String JSON_OBJECT = "jsonObject";
    public static final String AGGS = "aggs";
    public static final int MAX_ES_SIZE = 10000;

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private static final Logger logger = LogManager.getLogger(RedisService.class);

    @Autowired
    private ConfigurationDAO config;

    private RestClient client;

    private Gson gson = new GsonBuilder().serializeNulls().create();

    private InsESService(ConfigurationDAO config) {
        super(config);
        this.gson = new GsonBuilder().serializeNulls().create();
        logger.info("Initializing Elasticsearch client");
        // Base on host name to use signed request (AWS) or not (local)
        AbstractClient abstractClient = config.isEsSignRequests() ? new AWSClient(config) : new DefaultClient(config);
        client = abstractClient.getLowLevelElasticClient();
    }

    // Base on host name to use signed request (AWS) or not (local)
    // public RestClient searchClient(String serviceName, String region) {
    //     String host = config.getEsHost().trim();
    //     String scheme = config.getEsScheme();
    //     int port = config.getEsPort();
    //     if (config.getEsSignRequests()) {
    //         AWS4Signer signer = new AWS4Signer();
    //         signer.setServiceName(serviceName);
    //         signer.setRegionName(region);
    //         HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
    //         return RestClient.builder(new HttpHost(host, port, scheme)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
    //     } else {
    //         var lowLevelBuilder = RestClient.builder(new HttpHost(host, port, scheme));
    //         return lowLevelBuilder.build();
    //     }
    // }

    // @PostConstruct
    // public void init() {
    //     logger.info("Initializing Elasticsearch client");
    //     client = searchClient("es", "us-east-1");
    // }

    @PreDestroy
    private void close() throws IOException {
        client.close();
    }

    public JsonObject send(Request request) throws IOException{
        Response response = client.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String msg = "Elasticsearch returned code: " + statusCode;
            logger.error(msg);
            throw new IOException(msg);
        }
        return getJSonFromResponse(response);
    }

    public JsonObject getJSonFromResponse(Response response) throws IOException {
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
        return jsonObject;
    }

    // This function build queries with following rules:
    //  - If a list is empty, query will return empty dataset
    //  - If a list has only one element which is empty string, query will return all data available
    //  - If a list is null, query will return all data available
    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams) {
        return buildListQuery(params, excludedParams, false);
    }

    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams, boolean ignoreCase) {
        Map<String, Object> result = new HashMap<>();

        List<Object> filter = new ArrayList<>();
        for (var key: params.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }
            Object obj = params.get(key);

            List<String> valueSet;
            if (obj instanceof List) {
                valueSet = (List<String>) obj;
            } else {
                String value = (String)obj;
                valueSet = List.of(value);
            }

            if (ignoreCase) {
                List<String> lowerCaseValueSet = new ArrayList<>();
                for (String value: valueSet) {
                    lowerCaseValueSet.add(value.toLowerCase());
                }
                valueSet = lowerCaseValueSet;
            }
            // list with only one empty string [""] means return all records
            if (valueSet.size() == 1) {
                if (valueSet.get(0).equals("")) {
                    continue;
                }
            }
            filter.add(Map.of(
                "terms", Map.of( key, valueSet)
            ));
        }

        result.put("query", Map.of("bool", Map.of("filter", filter)));
        return result;
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params) throws IOException {
        return buildFacetFilterQuery(params, Set.of());
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams)  throws IOException {
        return buildFacetFilterQuery(params, rangeParams, Set.of());
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams)  throws IOException {
        return buildFacetFilterQuery(params, rangeParams, excludedParams, Map.of());
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams, Map<String, Object> additionalParams) throws IOException {
        return buildFacetFilterQuery(params, rangeParams, excludedParams, additionalParams, "");
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams, Map<String,Object> additionalParams, String nestedProperty) throws IOException {
        Map<String, Object> result = new HashMap<>();

        List<Object> filter = new ArrayList<>();
        for (var key: params.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }

            if (rangeParams.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Double> bounds = (List<Double>) params.get(key);
                if (bounds.size() >= 2) {
                    Double lower = bounds.get(0);
                    Double higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    Map<String, Double> range = new HashMap<>();
                    if (lower != null) {
                        range.put("gte", lower);
                    }
                    if (higher != null) {
                        range.put("lte", higher);
                    }
                    if (nestedProperty.equals("")) {  // nested queries are on nested property keys
                        filter.add(Map.of(
                            "range", Map.of(key, range)
                        ));
                    } else {
                        filter.add(Map.of(
                            "range", Map.of(nestedProperty+"."+key, range)
                        ));
                    }

                }
            } else {
                // Term parameters (default)
                List<String> valueSet = (List<String>) params.get(key);
                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    if (nestedProperty.equals("")) {  // nested queries are on nested property keys
                        filter.add(Map.of(
                            "terms", Map.of(key, valueSet)
                        ));
                    } else {
                        filter.add(Map.of(
                        "terms", Map.of(nestedProperty+"."+key, valueSet)
                        ));
                    }
                }
            }
        }

        for (var key: additionalParams.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }

            if (rangeParams.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Double> bounds = (List<Double>) additionalParams.get(key);
                if (bounds.size() >= 2) {
                    Double lower = bounds.get(0);
                    Double higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    Map<String, Double> range = new HashMap<>();
                    if (lower != null) {
                        range.put("gte", lower);
                    }
                    if (higher != null) {
                        range.put("lte", higher);
                    }
                    if (nestedProperty.equals("")) {  // nested queries are on nested property keys
                        filter.add(Map.of(
                            "range", Map.of(key, range)
                        ));
                    } else {
                        filter.add(Map.of(
                            "range", Map.of(nestedProperty+"."+key, range)
                        ));
                    }
                }
            } else {
                // it is assumed that if we're adding additional parameters in the backend,
                //   that we know what we're doing and don't require as much validation
                //   as with normal 'params' passed from the user/frontend
                if (nestedProperty.equals("")) {  // nested queries are on nested property keys
                    filter.add(Map.of(
                        "terms", Map.of(key, additionalParams.get(key))
                    ));
                } else {
                    filter.add(Map.of(
                        "terms", Map.of(nestedProperty+"."+key, additionalParams.get(key))
                    ));
                }
            }
        }

        for (var key: additionalParams.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }

            if (rangeParams.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Double> bounds = (List<Double>) additionalParams.get(key);
                if (bounds.size() >= 2) {
                    Double lower = bounds.get(0);
                    Double higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    Map<String, Double> range = new HashMap<>();
                    if (lower != null) {
                        range.put("gte", lower);
                    }
                    if (higher != null) {
                        range.put("lte", higher);
                    }
                    filter.add(Map.of(
                            "range", Map.of(key, range)
                    ));
                }
            } else {
                // it is assumed that if we're adding additional parameters in the backend,
                //   that we know what we're doing and don't require as much validation
                //   as with normal 'params' passed from the user/frontend
                filter.add(Map.of(
                    "terms", Map.of(key, additionalParams.get(key))
                ));
            }
        }

        if (filter.size() == 0) {
            result.put("query", Map.of("match_all", Map.of()));    
        } else if (nestedProperty.equals("")) {  // the nestedParams has to be explicitly set, otherwise the default behavior should be as before
            result.put("query", Map.of("bool", Map.of("filter", filter)));
        } else {
            result.put("query", Map.of("nested", Map.of("path", nestedProperty, "query", Map.of("bool", Map.of("filter", filter)), "inner_hits", Map.of())));
        }
        
        return result;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames) {
        return addAggregations(query, termAggNames, new String(), new String[]{});
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String cardinalityAggName) {
        return addAggregations(query, termAggNames, cardinalityAggName, new String[]{});
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String subCardinalityAggName, String[] rangeAggNames) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);
        // newQuery.put("aggs", getAllAggregations(termAggNames, rangeAggNames));
        // List<Map<String, Object>> fields = new LinkedList<Map<String, Object>>();
        Map<String, Object> fields = new HashMap<String, Object>();
        for (String field: termAggNames) {
            // the "size": 50 is so that we can have more than 10 buckets returned for our aggregations (the default)
            Map<String, Object> subField = Map.of("field", field, "size", 50);
            if (!subCardinalityAggName.isEmpty()) {
                fields.put(field, Map.of("terms", subField, "aggs", addCardinalityHelper(subCardinalityAggName)));
            } else {
                fields.put(field, Map.of("terms", subField));
            }
        }
        newQuery.put("aggs", fields);
        return newQuery;
    }

    public Map<String, Object> addCardinalityAggregation(Map<String, Object> query, String cardinalityAggName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);
        newQuery.put("aggs", addCardinalityHelper(cardinalityAggName));
        return newQuery;
    }

    public Map<String, Object> addCardinalityHelper(String cardinalityAggName) {
        return Map.of("cardinality_count", Map.of("cardinality", Map.of("field", cardinalityAggName)));
    }

    public void addSubAggregations(Map<String, Object> query, String mainAggName, String[] subTermAggNames) {
        addSubAggregations(query, mainAggName, subTermAggNames, new String[]{});
    }

    public void addSubAggregations(Map<String, Object> query, String mainAggName, String[] subTermAggNames, String[] subRangeAggNames) {
        Map<String, Object> mainAgg = (Map<String, Object>) ((Map<String, Object>) query.get("aggregations")).get(mainAggName);
        Map<String, Object> subAggs = getAllAggregations(subTermAggNames, subRangeAggNames);
        mainAgg.put("aggregations", subAggs);
    }

    private Map<String, Object> getAllAggregations(String[]  termAggNames, String[] rangeAggNames) {
        Map<String, Object> aggs = new HashMap<>();
        for (String aggName: termAggNames) {
            // Terms
            aggs.put(aggName, getTermAggregation(aggName));
        }

        for (String aggName: rangeAggNames) {
            // Range
            aggs.put(aggName, getRangeAggregation(aggName));
        }
        return aggs;
    }

    private Map<String, Object> getTermAggregation(String aggName) {
        Map<String, Object> agg = new HashMap<>();
        // agg.put("terms", Map.of("field", aggName, "size", MAX_ES_SIZE));
        agg.put("terms", Map.of("field", aggName));
        return agg;
    }

    private Map<String, Object> getRangeAggregation(String aggName) {
        Map<String, Object> agg = new HashMap<>();
        agg.put("stats", Map.of("field", aggName));
        return agg;
    }

    public Map<String, JsonArray> collectTermAggs(JsonObject jsonObject, String[] termAggNames) {
        Map<String, JsonArray> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        for (String aggName: termAggNames) {
            // Terms buckets
            data.put(aggName, aggs.getAsJsonObject(aggName).getAsJsonArray("buckets"));
        }
        return data;
    }

    public List<String> collectTerms(JsonObject jsonObject, String aggName) {
        List<String> data = new ArrayList<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        JsonArray buckets = aggs.getAsJsonObject(aggName).getAsJsonArray("buckets");
        for (var bucket: buckets) {
            data.add(bucket.getAsJsonObject().get("key").getAsString());
        }
        return data;
    }

    public Map<String, JsonObject> collectRangeAggs(JsonObject jsonObject, String[] rangeAggNames) {
        Map<String, JsonObject> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        for (String aggName: rangeAggNames) {
            // Range/stats
            data.put(aggName, aggs.getAsJsonObject(aggName));
        }
        return data;
    }



    public List<String> collectBucketKeys(JsonArray buckets) {
        List<String> keys = new ArrayList<>();
        for (var bucket: buckets) {
            keys.add(bucket.getAsJsonObject().get("key").getAsString());
        }
        return keys;
    }

    public List<String> collectField(Request request, String fieldName) throws IOException {
        List<String> results = new ArrayList<>();

        if (!request.getParameters().containsKey("scroll"))
            request.addParameter("scroll", "10S");
        JsonObject jsonObject = send(request);
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");

        while (searchHits != null && searchHits.size() > 0) {
            logger.info("Current " + fieldName + " records: " + results.size() + " collecting...");
            for (int i = 0; i < searchHits.size(); i++) {
                String value = searchHits.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(fieldName).getAsString();
                results.add(value);
            }

            Request scrollRequest = new Request("POST", SCROLL_ENDPOINT);
            String scrollId = jsonObject.get("_scroll_id").getAsString();
            Map<String, Object> scrollQuery = Map.of(
                    "scroll", "10S",
                    "scroll_id", scrollId
            );
            scrollRequest.setJsonEntity(gson.toJson(scrollQuery));
            jsonObject = send(scrollRequest);
            searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        }

        String scrollId = jsonObject.get("_scroll_id").getAsString();
        Request clearScrollRequest = new Request("DELETE", SCROLL_ENDPOINT);
        clearScrollRequest.setJsonEntity("{\"scroll_id\":\"" + scrollId +"\"}");
        send(clearScrollRequest);

        return results;
    }

    public List<String> collectFieldForArray(Request request, String fieldName) throws IOException {
        List<String> results = new ArrayList<>();

        if (!request.getParameters().containsKey("scroll"))
            request.addParameter("scroll", "10S");
        JsonObject jsonObject = send(request);
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");

        while (searchHits != null && searchHits.size() > 0) {
            logger.info("Current " + fieldName + " records: " + results.size() + " collecting...");
            for (int i = 0; i < searchHits.size(); i++) {
                JsonArray values = searchHits.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(fieldName).getAsJsonArray();
                for (int j = 0; j < values.size(); j++) {
                    results.add(values.get(j).getAsString());
                }
            }

            Request scrollRequest = new Request("POST", SCROLL_ENDPOINT);
            String scrollId = jsonObject.get("_scroll_id").getAsString();
            Map<String, Object> scrollQuery = Map.of(
                    "scroll", "10S",
                    "scroll_id", scrollId
            );
            scrollRequest.setJsonEntity(gson.toJson(scrollQuery));
            jsonObject = send(scrollRequest);
            searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        }

        String scrollId = jsonObject.get("_scroll_id").getAsString();
        Request clearScrollRequest = new Request("DELETE", SCROLL_ENDPOINT);
        clearScrollRequest.setJsonEntity("{\"scroll_id\":\"" + scrollId +"\"}");
        send(clearScrollRequest);

        return results;
    }

    public int getTotalHits(JsonObject jsonObject) {
        return jsonObject.get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt();
    }

    public List<Map<String, Object>> collectPage(Request request, Map<String, Object> query, String[][] properties, int pageSize, int offset) throws IOException {
        // data over limit of Elasticsearch, have to use roll API
        if (pageSize > MAX_ES_SIZE) {
            throw new IOException("Parameter 'first' must not exceeded " + MAX_ES_SIZE);
        }
        if (pageSize + offset > MAX_ES_SIZE) {
            return collectPageWithScroll(request, query, properties, pageSize, offset);
        }

        // data within limit can use just from/size
        query.put("size", pageSize);
        query.put("from", offset);
        request.setJsonEntity(gson.toJson(query));

        JsonObject jsonObject = send(request);
        return collectPage(jsonObject, properties, pageSize);
    }

    // offset MUST be multiple of pageSize, otherwise the page won't be complete
    private List<Map<String, Object>> collectPageWithScroll(
            Request request, Map<String, Object> query, String[][] properties, int pageSize, int offset) throws IOException {
        final int optimumSize = ( MAX_ES_SIZE / pageSize ) * pageSize;
        if (offset % pageSize != 0) {
            throw new IOException("'offset' must be multiple of 'first'!");
        }
        query.put("size", optimumSize);
        request.setJsonEntity(gson.toJson(query));
        request.addParameter("scroll", "10S");
        JsonObject page = rollToPage(request, offset);
        return collectPage(page, properties, pageSize, offset % optimumSize);
    }

    private JsonObject rollToPage(Request request, int offset) throws IOException {
        int rolledRecords = 0;
        JsonObject jsonObject = send(request);
        String scrollId = jsonObject.get("_scroll_id").getAsString();
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        rolledRecords += searchHits.size();

        while (rolledRecords <= offset && searchHits.size() > 0) {
            // Keep roll until correct page
            logger.info("Current records: " + rolledRecords + " collecting...");
            Request scrollRequest = new Request("POST", SCROLL_ENDPOINT);
            Map<String, Object> scrollQuery = Map.of(
                    "scroll", "10S",
                    "scroll_id", scrollId
            );
            scrollRequest.setJsonEntity(gson.toJson(scrollQuery));
            jsonObject = send(scrollRequest);
            scrollId = jsonObject.get("_scroll_id").getAsString();
            searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
            rolledRecords += searchHits.size();
        }

        // Now return page
        scrollId = jsonObject.get("_scroll_id").getAsString();
        Request clearScrollRequest = new Request("DELETE", SCROLL_ENDPOINT);
        clearScrollRequest.setJsonEntity("{\"scroll_id\":\"" + scrollId +"\"}");
        send(clearScrollRequest);
        return jsonObject;
    }

    // Collect a page of data, result will be of pageSize or less if not enough data remains
    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize) throws IOException {
        return collectPage(jsonObject, properties, pageSize, 0);
    }

    private List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize, int offset) throws IOException {
        return collectPage(jsonObject, properties, null, pageSize, offset);
    }

    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, String[][] highlights, int pageSize, int offset) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();

        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        for (int i = 0; i < searchHits.size(); i++) {
            // skip offset number of documents
            if (i + 1 <= offset) {
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            for (String[] prop: properties) {
                String propName = prop[0];
                String dataField = prop[1];
                JsonElement element = searchHits.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(dataField);
                row.put(propName, getValue(element));
            }
            if (highlights != null) {
                for (String[] highlight: highlights) {
                    String hlName = highlight[0];
                    String hlField = highlight[1];
                    JsonElement element = searchHits.get(i).getAsJsonObject().get("highlight").getAsJsonObject().get(hlField);
                    if (element != null) {
                        row.put(hlName, ((List<String>)getValue(element)).get(0));
                    }
                }
            }
            data.add(row);
            if (data.size() >= pageSize) {
                break;
            }
        }
        return data;
    }

    // Convert JsonElement into Java collections and primitives
    private Object getValue(JsonElement element) {
        Object value = null;
        if (element == null || element.isJsonNull()) {
            return null;
        } else if (element.isJsonObject()) {
            value = new HashMap<String, Object>();
            JsonObject object = element.getAsJsonObject();
            for (String key: object.keySet()) {
                ((Map<String, Object>) value).put(key, getValue(object.get(key)));
            }
        } else if (element.isJsonArray()) {
            value = new ArrayList<>();
            for (JsonElement entry: element.getAsJsonArray()) {
                ((List<Object>)value).add(getValue(entry));
            }
        } else {
            value = element.getAsString();
        }
        return value;
    }
}