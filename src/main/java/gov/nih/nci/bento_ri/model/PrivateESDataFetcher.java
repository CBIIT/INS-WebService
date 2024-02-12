package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.MultipleRequests;
import gov.nih.nci.bento.model.search.filter.DefaultFilter;
import gov.nih.nci.bento.model.search.filter.FilterParam;
import gov.nih.nci.bento.model.search.mapper.TypeMapperImpl;
import gov.nih.nci.bento.model.search.mapper.TypeMapperService;
import gov.nih.nci.bento.model.search.query.QueryParam;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento_ri.service.InsESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.*;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring; 

@Component
public class PrivateESDataFetcher extends AbstractPrivateESDataFetcher {
    private static final Logger logger = LogManager.getLogger(PrivateESDataFetcher.class);
    private final YamlQueryFactory yamlQueryFactory;
    private final TypeMapperService typeMapper = new TypeMapperImpl();
    private InsESService insEsService;
    @Autowired
    private Cache<String, Object> caffeineCache;

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String GRANTS_END_POINT = "/grants/_search";
    final String PROGRAMS_END_POINT = "/programs/_search";
    final String PROJECTS_END_POINT = "/projects/_search";
    final String PUBLICATIONS_END_POINT = "/publications/_search";
    final String HOME_STATS_END_POINT = "/home_stats/_search";

    final String GS_ABOUT_END_POINT = "/about_page/_search";
    final String GS_MODEL_END_POINT = "/data_model/_search";

    final int GS_LIMIT = 10;
    final String GS_END_POINT = "endpoint";
    final String GS_RESULT_FIELD = "result_field";
    final String GS_AGG_FIELD = "agg_field";
    final String GS_AGG_RESULT_FIELD = "agg_result_field";
    final String GS_COUNT_RESULT_FIELD = "count_result_field";
    final String GS_COUNT_FIELD = "count_field";
    final String GS_SEARCH_FIELD = "search_field";
    final String GS_COLLECT_FIELDS = "collect_fields";
    final String GS_SORT_FIELD = "sort_field";
    final String GS_CATEGORY_TYPE = "type";
    final String GS_ABOUT = "about";
    final String GS_HIGHLIGHT_FIELDS = "highlight_fields";
    final String GS_HIGHLIGHT_DELIMITER = "$";
    final Set<String> RANGE_PARAMS = Set.of("age_at_index");

    public PrivateESDataFetcher(InsESService esService) {
        super(esService);
        insEsService = esService;
        yamlQueryFactory = new YamlQueryFactory(esService);
    }

    @Override
    public RuntimeWiring buildRuntimeWiring() throws IOException {
        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("QueryType")
                        .dataFetchers(yamlQueryFactory.createYamlQueries(Const.ES_ACCESS_TYPE.PRIVATE))
                        .dataFetcher("globalSearch", env -> {
                            Map<String, Object> args = env.getArguments();
                            return globalSearch(args);
                        })
                        .dataFetcher("searchParticipants", env -> {
                            Map<String, Object> args = env.getArguments();
                            return searchParticipants(args);
                        })
                        .dataFetcher("numberOfGrants", env -> {
                            return numberOfGrants();
                        })
                        .dataFetcher("numberOfPrograms", env -> {
                            return numberOfPrograms();
                        })
                        .dataFetcher("numberOfProjects", env -> {
                            return numberOfProjects();
                        })
                        .dataFetcher("numberOfPublications", env -> {
                            return numberOfPublications();
                        })
                )
                .build();
    }

    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping) throws IOException {
        return overview(endpoint, params, properties, defaultSort, mapping, "");
    }

    // if the nestedProperty is set, this will filter based upon the params against the nested property for the endpoint's index.
    // otherwise, this will filter based upon the params against the top level properties for the index
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping, String nestedProperty) throws IOException {

        Request request = new Request("GET", endpoint);
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), Map.of(), nestedProperty);
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        List<Map<String, Object>> page = insEsService.collectPage(request, query, properties, pageSize, offset);
        return page;
    }

    private Map<String, String> mapSortOrder(String order_by, String direction, String defaultSort, Map<String, String> mapping) {
        String sortDirection = direction;
        if (!sortDirection.equalsIgnoreCase("asc") && !sortDirection.equalsIgnoreCase("desc")) {
            sortDirection = "asc";
        }

        String sortOrder = defaultSort; // Default sort order
        if (mapping.containsKey(order_by)) {
            sortOrder = mapping.get(order_by);
        } else {
            logger.info("Order: \"" + order_by + "\" not recognized, use default order");
        }
        return Map.of(sortOrder, sortDirection);
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint) throws IOException {
        return subjectCountBy(category, params, endpoint, Map.of());
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), additionalParams);
        return getGroupCount(category, query, endpoint);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint) throws IOException {
        return filterSubjectCountBy(category, params, endpoint, Map.of());
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, category), additionalParams);
        return getGroupCount(category, query, endpoint);
    }

    private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint) throws IOException {
        String[] AGG_NAMES = new String[] {category};
        query = insEsService.addAggregations(query, AGG_NAMES);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);
        Map<String, JsonArray> aggs = insEsService.collectTermAggs(jsonObject, AGG_NAMES);
        JsonArray buckets = aggs.get(category);

        return getGroupCountHelper(buckets);
    }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                    "subjects", group.getAsJsonObject().get("doc_count").getAsInt()
            ));

        }
        return data;
    }

    private List<Map<String, Object>> getGroupCardinalityCountHelper(JsonArray buckets) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                    "subjects", group.getAsJsonObject().getAsJsonObject("cardinality_count").get("value").getAsInt()
            ));

        }
        return data;
    }

    private Map<String, Object> rangeFilterSubjectCountBy(String category, Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS,Set.of(PAGE_SIZE, category));
        return getRangeCount(category, query);
    }

    private Map<String, Object> getRangeCount(String category, Map<String, Object> query) throws IOException {
        String[] AGG_NAMES = new String[] {category};
        query = insEsService.addAggregations(query, new String[]{}, new String(), AGG_NAMES);
        Request request = new Request("GET", SUBJECTS_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);
        Map<String, JsonObject> aggs = insEsService.collectRangeAggs(jsonObject, AGG_NAMES);
        return getRange(aggs.get(category).getAsJsonObject());
    }

    private Map<String, Object> getRange(JsonObject aggs) {
        final String LOWER_BOUND = "lowerBound";
        final String UPPER_BOUND = "upperBound";
        Map<String, Object> range = new HashMap<>();
        range.put("subjects", aggs.get("count").getAsInt());
        JsonElement lowerBound = aggs.get("min");
        if (!lowerBound.isJsonNull()) {
            range.put(LOWER_BOUND, lowerBound.getAsDouble());
        } else {
            range.put(LOWER_BOUND, null);
        }
        JsonElement upperBound = aggs.get("max");
        if (!upperBound.isJsonNull()) {
            range.put("upperBound", aggs.get("max").getAsDouble());
        } else {
            range.put(UPPER_BOUND, null);
        }

        return range;
    }

    private Map<String, Object> globalSearch(Map<String, Object> params) throws IOException {
        Map<String, Object> result = new HashMap<>();
        String input = (String) params.get("input");
        int size = (int) params.get("first");
        int offset = (int) params.get("offset");
        List<Map<String, Object>> searchCategories = new ArrayList<>();
        searchCategories.add(Map.of(
                GS_END_POINT, PROJECTS_END_POINT,
                GS_COUNT_RESULT_FIELD, "project_count",
                GS_COUNT_FIELD, "project_id", // returned results to count (unique) toward total result count
                GS_AGG_RESULT_FIELD, "project_titles",
                GS_AGG_FIELD, "project_title",
                GS_RESULT_FIELD, "projects",
                GS_SEARCH_FIELD, List.of("activity_code.search", "serial_number.search", "project_id.search", "application_id.search",
                                        "project_title.search", "abstract_text.search", "queried_project_id.search",
                                        "keywords.search", "org_name.search", "org_city.search", "org_state.search", "docs.search", "principal_investigators.search",
                                        "program_officers.search", "full_foa.search", "programs.search"),
                GS_SORT_FIELD, "project_id",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"project_id", "project_id"},
                        new String[]{"queried_project_id", "queried_project_id"},
                        new String[]{"application_id", "application_id"},
                        new String[]{"project_title", "project_title"},
                        new String[]{"abstract_text", "abstract_text"},
                        new String[]{"keywords", "keywords"},
                        new String[]{"org_name", "org_name"},
                        new String[]{"org_city", "org_city"},
                        new String[]{"org_state", "org_state"},
                        new String[]{"lead_doc", "docs"},
                        new String[]{"principal_investigators", "principal_investigators"},
                        new String[]{"program_officers", "program_officers"},
                        new String[]{"full_foa", "full_foa"},
                        new String[]{"program", "programs"},
                        new String[]{"type", "type"}
                },
                // GS_HIGHLIGHT_FIELDS, new String[][] {
                //         new String[]{"highlight", "project_id.search"}
                // },
                GS_CATEGORY_TYPE, "project"
        ));

        for (Map<String, Object> category: searchCategories) {
            String resultFieldName = (String) category.get(GS_RESULT_FIELD);
            String resultCountFieldName = (String) category.get(GS_COUNT_FIELD);
            String resultAggFieldName = (String) category.get(GS_AGG_FIELD);
            String[][] properties = (String[][]) category.get(GS_COLLECT_FIELDS);
            String[][] highlights = (String[][]) category.get(GS_HIGHLIGHT_FIELDS);
            Map<String, Object> query = getGlobalSearchQuery(input, category, resultCountFieldName, resultAggFieldName);

            // Get results
            Request request = new Request("GET", (String)category.get(GS_END_POINT));
            String sortFieldName = (String)category.get(GS_SORT_FIELD);
            query.put("sort", Map.of(sortFieldName, "asc"));
            // query = addHighlight(query, category);

            // size and offset
            query.put("size", size);
            query.put("from", offset);
            
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = insEsService.send(request);
            List<Map<String, Object>> objects = insEsService.collectPage(jsonObject, properties, highlights, (int)query.get("size"), 0);
            
            // Get count
            Integer countResultFieldName = jsonObject.get("aggregations").getAsJsonObject().get("field_count").getAsJsonObject().get("value").getAsInt();
            result.put((String)category.get(GS_COUNT_RESULT_FIELD), countResultFieldName);

            // Get aggregation field
            JsonArray aggResultFieldName = jsonObject.get("aggregations").getAsJsonObject().get("agg_field").getAsJsonObject().get("buckets").getAsJsonArray();
            List<Map<String, Object>> aggResults = new ArrayList<Map<String, Object>>();
            for (JsonElement bucket: aggResultFieldName) {
                aggResults.add(Map.of("title", bucket.getAsJsonObject().get("key").getAsString()));
            }
            result.put((String)category.get(GS_AGG_RESULT_FIELD), aggResults);

            
            for (var object: objects) {
                object.put(GS_CATEGORY_TYPE, category.get(GS_CATEGORY_TYPE));
            }

            List<Map<String, Object>> existingObjects = (List<Map<String, Object>>)result.getOrDefault(resultFieldName, null);
            if (existingObjects != null) {
                existingObjects.addAll(objects);
                result.put(resultFieldName, existingObjects);
            } else {
                result.put(resultFieldName, objects);
            }

        }

        List<Map<String, String>> about_results = searchAboutPage(input);
        int about_count = about_results.size();
        result.put("about_count", about_count);
        result.put("about_page", paginate(about_results, size, offset));

        return result;
    }

    private List paginate(List org, int pageSize, int offset) {
        List<Object> result = new ArrayList<>();
        int size = org.size();
        if (offset <= size -1) {
            int end_index = offset + pageSize;
            if (end_index > size) {
                end_index = size;
            }
            result = org.subList(offset, end_index);
        }
        return result;
    }

    private List<Map<String, String>> searchAboutPage(String input) throws IOException {
        final String ABOUT_CONTENT = "content.paragraph";
        Map<String, Object> query = Map.of(
                "query", Map.of("match", Map.of(ABOUT_CONTENT, input)),
                "highlight", Map.of(
                        "fields", Map.of(ABOUT_CONTENT, Map.of()),
                        "pre_tags", GS_HIGHLIGHT_DELIMITER,
                        "post_tags", GS_HIGHLIGHT_DELIMITER
                    ),
                "size", InsESService.MAX_ES_SIZE
        );
        
        Request request = new Request("GET", GS_ABOUT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);

        List<Map<String, String>> result = new ArrayList<>();

        for (JsonElement hit: jsonObject.get("hits").getAsJsonObject().get("hits").getAsJsonArray()) {
            for (JsonElement highlight: hit.getAsJsonObject().get("highlight").getAsJsonObject().get(ABOUT_CONTENT).getAsJsonArray()) {
                String page = hit.getAsJsonObject().get("_source").getAsJsonObject().get("page").getAsString();
                String title = hit.getAsJsonObject().get("_source").getAsJsonObject().get("title").getAsString();
                result.add(Map.of(
                        GS_CATEGORY_TYPE, GS_ABOUT,
                        "page", page,
                        "title", title,
                        "text", highlight.getAsString()
                ));
            }
        }

        return result;
    }

    private Map<String, Object> getGlobalSearchQuery(String input, Map<String, Object> category, String resultCountFieldName, String resultAggFieldName) {
        List<String> searchFields = (List<String>)category.get(GS_SEARCH_FIELD);
        List<Object> searchClauses = new ArrayList<>();
        for (String searchFieldName: searchFields) {
            searchClauses.add(Map.of("match_phrase_prefix", Map.of(searchFieldName, input)));
        }
        Map<String, Object> query = new HashMap<>();
        query.put("query", Map.of("bool", Map.of("should", searchClauses)));
        
        // get an accurate count of results, not just number of documents returned -- those may be different depending upon
        //   the index schema
        Map<String, Object> aggs = new HashMap<String, Object>();
        if (resultCountFieldName != null) {
            aggs.put("field_count", Map.of("cardinality", Map.of("field", resultCountFieldName)));
        }
        if (resultAggFieldName != null) {
            aggs.put("agg_field", Map.of("terms", Map.of("field", resultAggFieldName)));
        }
        query.put("aggs", aggs);

        return query;
    }

    private Map<String, Object> addHighlight(Map<String, Object> query, Map<String, Object> category) {
        Map<String, Object> result = new HashMap<>(query);
        List<String> searchFields = (List<String>)category.get(GS_SEARCH_FIELD);
        Map<String, Object> highlightClauses = new HashMap<>();
        for (String searchFieldName: searchFields) {
            highlightClauses.put(searchFieldName, Map.of());
        }

        result.put("highlight", Map.of(
                "fields", highlightClauses,
                "pre_tags", "",
                "post_tags", "",
                "fragment_size", 1
                )
        );
        return result;
    }

    private Map<String, Object> searchParticipants(Map<String, Object> params) throws IOException {
        String cacheKey = generateCacheKey(params);
        Map<String, Object> data = (Map<String, Object>)caffeineCache.asMap().get(cacheKey);
    }

    private Integer numberOfGrants() throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = insEsService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_grants").getAsInt();

        return count;
    }

    private Integer numberOfPrograms() throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = insEsService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_programs").getAsInt();

        return count;
    }

    private Integer numberOfProjects() throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = insEsService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_projects").getAsInt();

        return count;
    }

    private Integer numberOfPublications() throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = insEsService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_publications").getAsInt();

        return count;
    }

    private final static class BENTO_INDEX {
        private static final String SAMPLES = "samples";
    }

    private final static class BENTO_FIELDS {
        private static final String SAMPLE_NESTED_FILE_INFO = "file_info";
        private static final String FILES = "files";
    }

    private String generateCacheKey(Map<String, Object> params) throws IOException {
        List<String> keys = new ArrayList<>();
        for (String key: params.keySet()) {
            if (RANGE_PARAMS.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Integer> bounds = (List<Integer>) params.get(key);
                if (bounds.size() >= 2) {
                    Integer lower = bounds.get(0);
                    Integer higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    keys.add(key.concat(lower.toString()).concat(higher.toString()));
                }
            } else {
                List<String> valueSet = (List<String>) params.get(key);
                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    keys.add(key.concat(valueSet.toString()));
                }
            }
        }
        if (keys.size() == 0){
            return "all";
        } else {
            return keys.toString();
        }
    }
}
