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
// import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento_ri.service.InsESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.springframework.stereotype.Component;

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

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String PROJECTS_END_POINT = "/projects/_search";
    final String PROGRAMS_END_POINT = "/programs/_search";
    final String PROGRAMS_COUNT_END_POINT = "/programs/_count";
    final String PROJECTS_COUNT_END_POINT = "/projects/_count";
    final String PUBLICATIONS_END_POINT = "/publications/_search";
    final String PUBLICATIONS_COUNT_END_POINT = "/publications/_count";
    final String DATASETS_END_POINT = "/datasets/_search";
    final String DATASETS_COUNT_END_POINT = "/datasets/_count";
    final String CLINICAL_TRIALS_END_POINT = "/clinical_trials/_search";
    final String CLINICAL_TRIALS_COUNT_END_POINT = "/clinical_trials/_count";
    final String PATENTS_END_POINT = "/patents/_search";
    final String PATENTS_COUNT_END_POINT = "/patents/_count";

    final String STUDIES_END_POINT = "/studies/_search";
    final String STUDIES_COUNT_END_POINT = "/studies/_count";

    final String SUBJECTS_END_POINT = "/subjects/_search";
    final String SUBJECTS_COUNT_END_POINT = "/subjects/_count";
    final String SUBJECT_IDS_END_POINT = "/subject_ids/_search";
    final String SAMPLES_END_POINT = "/samples/_search";
    final String SAMPLES_COUNT_END_POINT = "/samples/_count";
    final String FILES_END_POINT = "/files/_search";
    final String FILES_COUNT_END_POINT = "/files/_count";
    final String NODES_END_POINT = "/model_nodes/_search";
    final String NODES_COUNT_END_POINT = "/model_nodes/_count";
    final String PROPERTIES_END_POINT = "/model_properties/_search";
    final String PROPERTIES_COUNT_END_POINT = "/model_properties/_count";
    final String VALUES_END_POINT = "/model_values/_search";
    final String VALUES_COUNT_END_POINT = "/model_values/_count";
    final String GS_ABOUT_END_POINT = "/about_page/_search";
    final String GS_MODEL_END_POINT = "/data_model/_search";
    final String SEARCH_PROJECTS_ES_END_POINT = "/filter_ids/_search";
    final String SEARCH_PROJECTS_ES_COUNT_END_POINT = "/filter_ids/_count";

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
                        .dataFetcher("searchProjects", env -> {
                            Map<String, Object> args = env.getArguments();
                            return searchProjects(args);
                        })
                        .dataFetcher("patentOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return patentOverView(args);
                        })
                        .dataFetcher("patentOverViewByProject", env -> {
                            Map<String, Object> args = env.getArguments();
                            return patentOverView(args);
                        })
                        .dataFetcher("clinicalTrialOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return clinicalTrialOverView(args);
                        })
                        .dataFetcher("clinicalTrialOverViewByProject", env -> {
                            Map<String, Object> args = env.getArguments();
                            return clinicalTrialOverView(args);
                        })
                        .dataFetcher("datasetOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return datasetOverView(args);
                        })
                        .dataFetcher("datasetOverViewByProject", env -> {
                            Map<String, Object> args = env.getArguments();
                            return datasetOverView(args);
                        })
                        .dataFetcher("publicationOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return publicationOverview(args);
                         })
                         .dataFetcher("publicationOverViewByProject", env -> {
                            Map<String, Object> args = env.getArguments();
                            return publicationOverview(args);
                         })
                        .dataFetcher("projectOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectOverView(args);
                        })
                        .dataFetcher("projectOverViewByProject", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectOverView(args);
                        })
                        .dataFetcher("numberOfPrograms", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfPrograms(args);
                        })
                        .dataFetcher("numberOfProjects", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfProjects(args);
                        }).
                        dataFetcher("numberOfCoreProjects", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfCoreProjects(args);
                        })
                        .dataFetcher("numberOfPublications", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfPublications(args);
                        })
                        .dataFetcher("numberOfGEOs", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfGEOs(args);
                        })
                        .dataFetcher("numberOfSRAs", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfSRAs(args);
                        })
                        .dataFetcher("numberOfDBGaps", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfDBGaps(args);
                        })
                        .dataFetcher("numberOfDatasets", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfDatasets(args);
                        })
                        .dataFetcher("numberOfClinicalTrials", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfClinicalTrials(args);
                        })
                        .dataFetcher("numberOfPatents", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfPatents(args);
                        })
                        .dataFetcher("programPublicationCount", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programPublicationCount(args);
                        })
                        .dataFetcher("programDatasetCount", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programDatasetCount(args);
                        })
                        .dataFetcher("programClinicalTrialCount", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programClinicalTrialCount(args);
                        })
                        .dataFetcher("programPatentCount", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programPatentCount(args);
                        })
                        .dataFetcher("programInfo", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programInfo(args);
                        })
                        .dataFetcher("projectDetail", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectDetail(args);
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

    private List<Map<String, Object>> findSubjectIdsInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"subject_id", "subject_id"},
                new String[]{"program_id", "program_id"}
        };

        Map<String, Object> query = insEsService.buildListQuery(params, Set.of(), true);
        Request request = new Request("GET", SUBJECT_IDS_END_POINT);

        return insEsService.collectPage(request, query, properties, InsESService.MAX_ES_SIZE, 0);
    }

    private List<Map<String, Object>> filesInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"study_code", "study_acronym"},
                new String[]{"subject_id", "subject_ids"},
                new String[]{"file_name", "file_names"},
                new String[]{"file_type", "file_type"},
                new String[]{"association", "association"},
                new String[]{"file_description", "file_description"},
                new String[]{"file_format", "file_format"},
                new String[]{"file_size", "file_size"},
                new String[]{"file_id", "file_ids"},
                new String[]{"md5sum", "md5sum"}
        };

        String defaultSort = "file_name"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("study_code", "study_acronym"),
                Map.entry("subject_id", "subject_id_num"),
                Map.entry("file_name", "file_names"),
                Map.entry("file_type", "file_type"),
                Map.entry("association", "association"),
                Map.entry("file_description", "file_description"),
                Map.entry("file_format", "file_format"),
                Map.entry("file_size", "file_size"),
                Map.entry("file_id", "file_id_num"),
                Map.entry("md5sum", "md5sum")
        );

        Map<String, Object> query = insEsService.buildListQuery(params, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION));
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        Request request = new Request("GET", FILES_END_POINT);

        return insEsService.collectPage(request, query, properties, pageSize, offset);
    }

    private List<String> fileIDsFromList(Map<String, Object> params) throws IOException {
        return collectFieldFromList(params, "file_ids", FILES_END_POINT);
    }

    // This function search values in parameters and return a given collectField's unique values in a list
    private List<String> collectFieldFromList(Map<String, Object> params, String collectField, String endpoint) throws IOException {
        String[] idFieldArray = new String[]{collectField};
        Map<String, Object> query = insEsService.buildListQuery(params, Set.of());
        query = insEsService.addAggregations(query, idFieldArray);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);
        return insEsService.collectTerms(jsonObject, collectField);
    }

    private Map<String, Object> searchProjects(Map<String, Object> params) throws IOException {
        final String AGG_NAME = "agg_name";
        final String AGG_ENDPOINT = "agg_endpoint";
        final String WIDGET_QUERY = "widgetQueryName";
        final String FILTER_COUNT_QUERY = "filterCountQueryName";
        // Query related values
        final List<Map<String, String>> PROJECT_TERM_AGGS = new ArrayList<>();
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "programs",
                WIDGET_QUERY, "projectCountByProgram",
                FILTER_COUNT_QUERY, "filterProjectCountByProgram",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "docs",
                WIDGET_QUERY, "projectCountByDOC",
                FILTER_COUNT_QUERY, "filterProjectCountByDOC",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "fiscal_years",
                WIDGET_QUERY, "projectCountByFiscalYear",
                FILTER_COUNT_QUERY, "filterProjectCountByFiscalYear",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "award_amounts",
                WIDGET_QUERY,"projectCountByAwardAmount",
                FILTER_COUNT_QUERY, "filterProjectCountByAwardAmount",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        List<String> project_agg_names = new ArrayList<>();
        for (var agg: PROJECT_TERM_AGGS) {
            project_agg_names.add(agg.get(AGG_NAME));
        }
        final String[] PROJECTS_TERM_AGG_NAMES = project_agg_names.toArray(new String[PROJECT_TERM_AGGS.size()]);

        final List<Map<String, String>> PUBLICATION_TERM_AGGS = new ArrayList<>();
        PUBLICATION_TERM_AGGS.add(Map.of(
            AGG_NAME, "citation_count_category",
            WIDGET_QUERY, "publicationCountByCitation",
            FILTER_COUNT_QUERY, "filterPublicationCountByCitation",
            AGG_ENDPOINT, PUBLICATIONS_END_POINT
        ));
        PUBLICATION_TERM_AGGS.add(Map.of(
            AGG_NAME, "rcr_range",
            WIDGET_QUERY, "publicationCountByRCR",
            FILTER_COUNT_QUERY, "filterPublicationCountByRCR",
            AGG_ENDPOINT, PUBLICATIONS_END_POINT
        ));
        PUBLICATION_TERM_AGGS.add(Map.of(
            AGG_NAME, "year",  // needs to be a keyword as well?
            WIDGET_QUERY, "publicationCountByYear",
            FILTER_COUNT_QUERY, "filterPublicationCountByYear",
            AGG_ENDPOINT, PUBLICATIONS_END_POINT
        ));

        List<String> publication_agg_names = new ArrayList<>();
        for (var agg: PUBLICATION_TERM_AGGS) {
            publication_agg_names.add(agg.get(AGG_NAME));
        }
        final String[] PUBLICATIONS_TERM_AGG_NAMES = publication_agg_names.toArray(new String[PUBLICATION_TERM_AGGS.size()]);

        Map<String, Object> query = insEsService.buildFacetFilterQuery(params);
        Request projectsCountRequest = new Request("GET", PROJECTS_COUNT_END_POINT);
        projectsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject projectsCountResult = insEsService.send(projectsCountRequest);
        int numberOfProjects = projectsCountResult.get("count").getAsInt();

        // special case because it's counting core projects
        Map<String, Object> coreProjectsCountQuery = insEsService.buildFacetFilterQuery(params, Set.of(), Set.of(), Map.of("representative", List.of(true))); // insEsService.addCardinalityAggregation(query, "queried_project_id");
        Request coreProjectsCountRequest = new Request("GET", PROJECTS_COUNT_END_POINT); // PROJECTS_END_POINT);
        coreProjectsCountRequest.setJsonEntity(gson.toJson(coreProjectsCountQuery));
        JsonObject coreProjectsCountResult = insEsService.send(coreProjectsCountRequest);
        int numberOfCoreProjects = coreProjectsCountResult.get("count").getAsInt(); // coreProjectsCountResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt(); // coreProjectsCountResult.getAsJsonObject("aggregations").getAsJsonObject("cardinality_count").get("value").getAsInt();

        // count anything with 'nested_projects'
        Map<String, Object> nestedQuery = insEsService.buildFacetFilterQuery(params, Set.of(), Set.of(), Map.of(), "nested_projects");
        Request programsCountRequest = new Request("GET", PROGRAMS_COUNT_END_POINT);
        programsCountRequest.setJsonEntity(gson.toJson(nestedQuery));
        JsonObject programsCountResult = insEsService.send(programsCountRequest);
        int numberOfPrograms = programsCountResult.get("count").getAsInt();

        Request publicationsCountRequest = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        publicationsCountRequest.setJsonEntity(gson.toJson(nestedQuery));
        JsonObject publicationsCountResult = insEsService.send(publicationsCountRequest);
        int numberOfPublications = publicationsCountResult.get("count").getAsInt();

        Request datasetsCountRequest = new Request("GET", DATASETS_COUNT_END_POINT);
        datasetsCountRequest.setJsonEntity(gson.toJson(nestedQuery));
        JsonObject datasetsCountResult = insEsService.send(datasetsCountRequest);
        int numberOfDatasets = datasetsCountResult.get("count").getAsInt();

        Request clinicalTrialsCountRequest = new Request("GET", CLINICAL_TRIALS_COUNT_END_POINT);
        clinicalTrialsCountRequest.setJsonEntity(gson.toJson(nestedQuery));
        JsonObject clinicalTrialsCountResult = insEsService.send(clinicalTrialsCountRequest);
        int numberOfClinicalTrials = clinicalTrialsCountResult.get("count").getAsInt();

        Request patentsCountRequest = new Request("GET", PATENTS_COUNT_END_POINT);
        patentsCountRequest.setJsonEntity(gson.toJson(nestedQuery));
        JsonObject patentsCountResult = insEsService.send(patentsCountRequest);
        int numberOfPatents = patentsCountResult.get("count").getAsInt();

        Map<String, Object> data = new HashMap<>();

        data.put("numberOfPrograms", numberOfPrograms);
        data.put("numberOfProjects", numberOfProjects);
        data.put("numberOfCoreProjects", numberOfCoreProjects);
        data.put("numberOfPublications", numberOfPublications);
        data.put("numberOfDatasets", numberOfDatasets);
        data.put("numberOfClinicalTrials", numberOfClinicalTrials);
        data.put("numberOfPatents", numberOfPatents);

        // widgets data and facet filter counts for projects
        for (var agg: PROJECT_TERM_AGGS) {
            String field = agg.get(AGG_NAME);
            String widgetQueryName = agg.get(WIDGET_QUERY);
            String filterCountQueryName = agg.get(FILTER_COUNT_QUERY);
            String endpoint = agg.get(AGG_ENDPOINT);
            // subjectCountByXXXX
            List<Map<String, Object>> widgetData;
            widgetData = subjectCountBy(field, params, endpoint, Map.of("representative", List.of(true)));
            data.put(widgetQueryName, widgetData);
            // filterSubjectCountByXXXX
            if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
                List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint, Map.of("representative", List.of(true)));
                data.put(filterCountQueryName, filterCount);
            } else {
                data.put(filterCountQueryName, widgetData);
            }
        }

        // widgets data and facet filter counts for publications
        for (var agg: PUBLICATION_TERM_AGGS) {
            String field = agg.get(AGG_NAME);
            String widgetQueryName = agg.get(WIDGET_QUERY);
            String filterCountQueryName = agg.get(FILTER_COUNT_QUERY);
            String endpoint = agg.get(AGG_ENDPOINT);
            // subjectCountByXXXX
            List<Map<String, Object>> widgetData;
            widgetData = subjectCountBy(field, params, endpoint);;
            data.put(widgetQueryName, widgetData);
            // filterSubjectCountByXXXX
            if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
                List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint);;
                data.put(filterCountQueryName, filterCount);
            } else {
                data.put(filterCountQueryName, widgetData);
            }
        }


        // Map<String, JsonObject> rangeAggs = insEsService.collectRangeAggs(subjectResult, RANGE_AGG_NAMES);

        // for (String field: RANGE_AGG_NAMES) {
        //     String filterCountQueryName = RANGE_AGGS.get(field);
        //     if (params.containsKey(field) && ((List<Double>)params.get(field)).size() >= 2) {
        //         Map<String, Object> filterCount = rangeFilterSubjectCountBy(field, params);;
        //         data.put(filterCountQueryName, filterCount);
        //     } else {
        //         data.put(filterCountQueryName, getRange(rangeAggs.get(field)));
        //     }
        // }

        return data;
    }

    private List<Map<String, Object>> patentOverView(Map<String, Object> params) throws IOException {
        // Following String array of arrays should be in form of "GraphQL_field_name", "ES_field_name"
        final String[][] PROPERTIES = new String[][]{
                new String[]{"patent_id", "patent_id"},
                new String[]{"fulfilled_date", "fulfilled_date"},
                new String[]{"queried_project_ids", "queried_project_ids"}
        };

        String defaultSort = "patent_id"; // Default sort order
        String nestedProperty = "nested_projects";  // Filter on the nested property for the patents index

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("patent_id", "patent_id"),
                Map.entry("fulfilled_date", "fulfilled_date"),
                Map.entry("queried_project_ids", "queried_project_ids")
        );

        return overview(PATENTS_END_POINT, params, PROPERTIES, defaultSort, mapping, nestedProperty);
    }
    
    private List<Map<String, Object>> clinicalTrialOverView(Map<String, Object> params) throws IOException {
        // Following String array of arrays should be in form of "GraphQL_field_name", "ES_field_name"
        final String[][] PROPERTIES = new String[][]{
                new String[]{"clinical_trial_id", "clinical_trial_id"},
                new String[]{"title", "title"},
                new String[]{"last_update_posted", "last_update_posted"},
                new String[]{"recruitment_status", "recruitment_status"},
                new String[]{"queried_project_ids", "queried_project_ids"}
        };

        String defaultSort = "clinical_trial_id"; // Default sort order
        String nestedProperty = "nested_projects";  // Filter on the nested property for the clinical_trials index

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("clinical_trial_id", "clinical_trial_id"),
                Map.entry("title", "title"),
                Map.entry("last_update_posted", "last_update_posted"),
                Map.entry("recruitment_status", "recruitment_status"),
                Map.entry("queried_project_ids", "queried_project_ids")
        );

        return overview(CLINICAL_TRIALS_END_POINT, params, PROPERTIES, defaultSort, mapping, nestedProperty);
    }
    
    private List<Map<String, Object>> datasetOverView(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"type", "transformed_type"},
                new String[]{"accession", "accession"},
                new String[]{"title", "title"},
                new String[]{"release_date", "release_date"},
                new String[]{"registration_date", "registration_date"},
                new String[]{"bioproject_accession", "bioproject_accession"},
                new String[]{"status", "status"},
                new String[]{"submission_date", "submission_date"},
                new String[]{"last_update_date", "last_update_date"},
                new String[]{"queried_project_ids", "queried_project_ids"},
                new String[]{"link", "link"},
                new String[]{"transformed_type", "transformed_type"},
        };

        String defaultSort = "accession"; // Default sort order
        String nestedProperty = "nested_projects";  // Filter on the nested property for the datasets index

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("type", "transformed_type"),
                Map.entry("accession", "accession"),
                Map.entry("title", "title"),
                Map.entry("release_date", "release_date"),
                Map.entry("registration_date", "registration_date"),
                Map.entry("bioproject_accession", "bioproject_accession"),
                Map.entry("status", "status"),
                Map.entry("submission_date", "submission_date"),
                Map.entry("last_update_date", "last_update_date"),
                Map.entry("queried_project_ids", "queried_project_ids"),
                Map.entry("link", "link"),
                Map.entry("transformed_type", "transformed_type")
        );

        return overview(DATASETS_END_POINT, params, PROPERTIES, defaultSort, mapping, nestedProperty);
    }

    private List<Map<String, Object>> publicationOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"publication_id", "publication_id"},
                new String[]{"pmc_id", "pmc_id"},
                new String[]{"year", "year"},
                new String[]{"journal", "journal"},
                new String[]{"title", "title"},
                new String[]{"authors", "authors"},
                new String[]{"publish_date", "publish_date"},
                new String[]{"citation_count", "citation_count"},
                new String[]{"relative_citation_ratio", "relative_citation_ratio"},
                new String[]{"tumor_grade", "tumor_grades"},
                new String[]{"nih_percentile", "nih_percentile"},
                new String[]{"doi", "doi"},
                new String[]{"queried_project_ids", "queried_project_ids"},
        };

        String defaultSort = "publication_id"; // Default sort order
        String nestedProperty = "nested_projects";  // Filter on the nested property for the publications index

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("publication_id", "publication_id"),
                Map.entry("pmc_id", "pmc_id"),
                Map.entry("year", "year"),
                Map.entry("journal", "journal"),
                Map.entry("title", "title"),
                Map.entry("authors", "authors"),
                Map.entry("publish_date", "publish_date"),
                Map.entry("citation_count", "citation_count"),
                Map.entry("relative_citation_ratio", "relative_citation_ratio"),
                Map.entry("nih_percentile", "nih_percentile"),
                Map.entry("doi", "doi"),
                Map.entry("queried_project_ids", "queried_project_ids")
        );

        return overview(PUBLICATIONS_END_POINT, params, PROPERTIES, defaultSort, mapping, nestedProperty);
    }
    
    private List<Map<String, Object>> projectOverView(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"program", "programs"},
            new String[]{"activity_code", "activity_code"},
            new String[]{"project_id", "project_id"},
            new String[]{"application_id", "application_id"},
            new String[]{"fiscal_year", "fiscal_years"},
            new String[]{"project_title", "project_title"},
            new String[]{"project_type", "project_type"},
            new String[]{"abstract_text", "abstract_text"},
            new String[]{"keywords", "keywords"},
            new String[]{"org_name", "org_name"},
            new String[]{"org_city", "org_citie"},
            new String[]{"org_state", "org_state"},
            new String[]{"org_country", "org_country"},
            new String[]{"principal_investigators", "principal_investigators"},
            new String[]{"lead_doc", "docs"},
            new String[]{"program_officers", "program_officers"},
            new String[]{"award_amount", "award_amount"},
            new String[]{"nci_funded_amount", "nci_funded_amount"},
            new String[]{"award_notice_date", "award_notice_date"},
            new String[]{"project_start_date", "project_start_date"},
            new String[]{"project_end_date", "project_end_date"},
            new String[]{"full_foa", "full_foa"},
            new String[]{"queried_project_id", "queried_project_id"},
        };

        String defaultSort = "project_id.sort"; // Default sort order

        // the indexes in ES are named after what is passed as filter params, the sorting params have different names for the same index properties
        // we need to translate 'fiscal_year'(sort param) to 'fiscal_years.raw'(index property) for sorting
        // we need to translate 'lead_doc'(sort param) to 'docs.sort'(index property) for sorting
        // we need to translate 'program'(sort param) to 'programs.sort'(index property) for sorting
        // we can leave 'award_amount'(sort param) alone for now, until we change how we handle the 'award_amount/award_amounts' index
        // Additionally we change 'project_title' to 'project_title.sort' and 'project_id' to 'project_id.sort' and 
        //   'activity_code' to 'activity_code.sort' to preserve
        //   the pattern of having a 'normalizer: lowercase' sort field for textual columns that doesn't affect (make lowercase)
        //   the query results for any other queries against those properties
        //   For example, if the top-level 'programs' property had 'normalizer: lowercase' then queries against 'programs' would
        //   return lowercase results, which caused problems for the Filter feature queries, hence a separate 'sort' subfield
        //   with the 'normalizer: lowercase' encapsulated
        Map<String, String> mapping = Map.ofEntries(
                Map.entry("program", "programs.sort"),
                Map.entry("lead_doc", "docs.sort"),
                Map.entry("fiscal_year", "fiscal_years.raw"),
                Map.entry("award_amount", "award_amount"),
                Map.entry("activity_code", "activity_code.sort"),
                Map.entry("project_id", "project_id.sort"),
                Map.entry("project_title", "project_title.sort"),
                Map.entry("principal_investigators", "principal_investigators.sort"),
                Map.entry("program_officers", "program_officers.sort"),
                Map.entry("project_end_date", "project_end_date"),
                Map.entry("queried_project_id", "queried_project_id")
        );

        return overview(PROJECTS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }

    private Integer numberOfPrograms(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());

        Request request = new Request("GET", PROGRAMS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfProjects(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());

        Request request = new Request("GET", PROJECTS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfCoreProjects(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of(), Set.of(), Map.of("representative", List.of(true)));

        Request request = new Request("GET", PROJECTS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfPublications(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());

        Request request = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfGEOs(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("transformed_type", List.of("GEO")), Set.of());  // RANGE_PARAMS

        Request request = new Request("GET", DATASETS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfSRAs(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("transformed_type", List.of("SRA")), Set.of());  // RANGE_PARAMS

        Request request = new Request("GET", DATASETS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfDBGaps(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("transformed_type", List.of("dbGaP")), Set.of());  // RANGE_PARAMS

        Request request = new Request("GET", DATASETS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfDatasets(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());  // RANGE_PARAMS

        Request request = new Request("GET", DATASETS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfClinicalTrials(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());

        Request request = new Request("GET", CLINICAL_TRIALS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer numberOfPatents(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of(), Set.of());

        Request request = new Request("GET", PATENTS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer programPublicationCount(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("programs", List.of(params.get("program_id"))), Set.of());

        Request request = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer programDatasetCount(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("programs", List.of(params.get("program_id"))), Set.of());

        Request request = new Request("GET", DATASETS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer programClinicalTrialCount(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("programs", List.of(params.get("program_id"))), Set.of());

        Request request = new Request("GET", CLINICAL_TRIALS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private Integer programPatentCount(Map<String, Object> params) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(Map.of("programs", List.of(params.get("program_id"))), Set.of());

        Request request = new Request("GET", PATENTS_COUNT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject result = insEsService.send(request);
        int number = result.get("count").getAsInt();
        return number;
    }

    private List<Map<String, Object>> programInfo(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"program_id", "programs"},
            new String[]{"program_name", "program_name"},
            new String[]{"program_website", "program_website"},
            new String[]{"num_projects", "num_projects"},
            new String[]{"num_publications", "num_publications"},
        };

        String defaultSort = "programs"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("program_id", "programs"),
                Map.entry("program_name", "program_name"),
                Map.entry("program_website", "program_website"),
                Map.entry("num_projects", "num_projects"),
                Map.entry("num_publications", "num_publications")
        );

        return overview(PROGRAMS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }

    private Map<String, Object> projectDetail(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"program", "programs"},
            new String[]{"activity_code", "activity_code"},
            new String[]{"project_id", "project_id"},
            new String[]{"queried_project_id", "queried_project_id"},
            new String[]{"application_id", "application_id"},
            new String[]{"fiscal_year", "fiscal_years"},
            new String[]{"project_title", "project_title"},
            new String[]{"project_type", "project_type"},
            new String[]{"abstract_text", "abstract_text"},
            new String[]{"keywords", "keywords"},
            new String[]{"org_name", "org_name"},
            new String[]{"org_city", "org_citie"},
            new String[]{"org_state", "org_state"},
            new String[]{"org_country", "org_country"},
            new String[]{"principal_investigators", "principal_investigators"},
            new String[]{"lead_doc", "docs"},
            new String[]{"program_officers", "program_officers"},
            // new String[]{"award_amount", "award_amount"},
            // new String[]{"nci_funded_amount", "nci_funded_amount"},
            new String[]{"award_notice_date", "award_notice_date"},
            new String[]{"project_start_date", "project_start_date"},
            new String[]{"project_end_date", "project_end_date"},
            new String[]{"full_foa", "full_foa"},
        };

        final String[][] CUMULATIVE_PROPERTIES = new String[][]{
            new String[]{"nci_funded_amount", "nci_funded_amount"},
            new String[]{"award_amount", "award_amount"},
        };

        // get the data
        Map<String,Object> query = insEsService.buildFacetFilterQuery(Map.of("queried_project_id", List.of(params.get("project_id"))), Set.of(), Set.of(), Map.of("representative", List.of(true)));
        Request request = new Request("GET", PROJECTS_END_POINT);
        Map<String,Object> result = insEsService.collectPage(request, query, PROPERTIES, 1, 0).get(0);

        // adeforge 03/16/2023: This could probably be a new type of aggregate query. As it stands, this is a basic query for all documents
        //   in the 'projects' index with a given 'queried_project_id'. We loop over them and sum the relevant 'CUMULATIVE_PROPERTIES'.
        //   This was not implemented as a new type of 'addAggregations' in the ESService.java module because this is a one-off that
        //   was simple to implement as such and the number of documents (grants in a core project) is low -- there wouldn't be a noticeable
        //   performance hit over and above the fact that it's an endpoint hit. If we were to implement a new aggregate query, that
        //   would be an endpoint hit as well and the difference between that and this would probably be negligible, but would cost more development time.
        //   If we want to formalize this with a new type of OpenSearch query and associated modifications to ESService.java, we can do that.
        //   The above are the reasons for implementing it this way for now.
        // get the cumulative data
        query = insEsService.buildFacetFilterQuery(Map.of("queried_project_id", List.of(params.get("project_id"))));
        query.put("size", InsESService.MAX_ES_SIZE);
        query.put("from", 0);
        request = new Request("GET", PROJECTS_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonArray cumulativeResult = insEsService.send(request).getAsJsonObject("hits").getAsJsonArray("hits");
        Integer cumulativeResultSize = cumulativeResult.size();
        // the cumulative properties return arrays of values to be summed
        for (var cumulativeProperty : CUMULATIVE_PROPERTIES) {
            String propName = cumulativeProperty[0];
            Integer sum = 0;
            for (int i = 0; i < cumulativeResultSize; i++) {
                sum += cumulativeResult.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(propName).getAsInt();
                result.put(propName, sum);
            }
        }

        // get the project count
        query = insEsService.buildFacetFilterQuery(Map.of("queried_project_id", List.of(params.get("project_id"))));
        Request projectsCountRequest = new Request("GET", PROJECTS_COUNT_END_POINT);
        projectsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject projectsCountResult = insEsService.send(projectsCountRequest);
        int numberOfProjects = projectsCountResult.get("count").getAsInt();

        // get the counts
        query = insEsService.buildFacetFilterQuery(Map.of("queried_project_ids", List.of(params.get("project_id"))));
        
        Request publicationsCountRequest = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        publicationsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject publicationsCountResult = insEsService.send(publicationsCountRequest);
        int numberOfPublications = publicationsCountResult.get("count").getAsInt();

        Request datasetsCountRequest = new Request("GET", DATASETS_COUNT_END_POINT);
        datasetsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject datasetsCountResult = insEsService.send(datasetsCountRequest);
        int numberOfDatasets = datasetsCountResult.get("count").getAsInt();

        Request clinicalTrialsCountRequest = new Request("GET", CLINICAL_TRIALS_COUNT_END_POINT);
        clinicalTrialsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject clinicalTrialsCountResult = insEsService.send(clinicalTrialsCountRequest);
        int numberOfClinicalTrials = clinicalTrialsCountResult.get("count").getAsInt();

        Request patentsCountRequest = new Request("GET", PATENTS_COUNT_END_POINT);
        patentsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject patentsCountResult = insEsService.send(patentsCountRequest);
        int numberOfPatents = patentsCountResult.get("count").getAsInt();

        Map<String, Object> data = new HashMap<>();
        data.put("num_projects", numberOfProjects);
        data.put("num_publications", numberOfPublications);
        data.put("num_datasets", numberOfDatasets);
        data.put("num_clinical_trials", numberOfClinicalTrials);
        data.put("num_patents", numberOfPatents);

        // combine counts and project data
        result.putAll(data);

        return result;
    }

    private final static class BENTO_INDEX {
        private static final String SAMPLES = "samples";
    }

    private final static class BENTO_FIELDS {
        private static final String SAMPLE_NESTED_FILE_INFO = "file_info";
        private static final String FILES = "files";
    }
}
