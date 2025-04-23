package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.mapper.TypeMapperImpl;
import gov.nih.nci.bento.model.search.mapper.TypeMapperService;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento_ri.service.InsESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private InsESService insEsService;

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String DATASETS_END_POINT = "/datasets/_search";
    final String GRANTS_END_POINT = "/grants/_search";
    final String PROGRAMS_END_POINT = "/programs/_search";
    final String PROJECTS_END_POINT = "/projects/_search";
    final String FACETED_PROJECTS_END_POINT = "/faceted_projects/_search";
    final String PUBLICATIONS_END_POINT = "/publications/_search";
    final String HOME_STATS_END_POINT = "/home_stats/_search";

    final String GRANTS_COUNT_END_POINT = "/grants/_count";
    final String PROGRAMS_COUNT_END_POINT = "/programs/_count";
    final String PROJECTS_COUNT_END_POINT = "/projects/_count";
    final String FACETED_PROJECTS_COUNT_END_POINT = "/faceted_projects/_count";
    final String PUBLICATIONS_COUNT_END_POINT = "/publications/_count";

    // For slider fields
    final Set<String> RANGE_PARAMS = Set.of(
        "relative_citation_ratio"
    );

    final Set<String> BOOLEAN_PARAMS = Set.of();

    final Set<String> ARRAY_PARAMS = Set.of();

    // For multiple selection from a list
    final Set<String> INCLUDE_PARAMS  = Set.of(
        // Programs
        "focus_area", "cancer_type"
    );

    // For general use, like facet filter
    final Set<String> REGULAR_PARAMS = Set.of(
        // Programs
        "focus_area",
        "cancer_type",

        // Projects
        "project_id"
    );

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
                        .dataFetcher("stats", env -> {
                            Map<String, Object> args = env.getArguments();
                            return stats(args);
                        })
                        .dataFetcher("idsLists", env -> idsLists())
                        .dataFetcher("searchProjects", env -> {
                            Map<String, Object> args = env.getArguments();
                            return searchProjects(args);
                        })
                        .dataFetcher("grantsOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return grantsOverview(args);
                        })
                        .dataFetcher("programsOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programsOverview(args);
                        })
                        .dataFetcher("projectsOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectsOverview(args);
                        })
                        .dataFetcher("publicationsOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return publicationsOverview(args);
                        })
                        .dataFetcher("numberOfDatasets", env -> {
                            return numberOfDatasets();
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
                        .dataFetcher("datasetDetails", env -> {
                            Map<String, Object> args = env.getArguments();
                            return datasetDetails(args);
                        })
                        .dataFetcher("programDetails", env -> {
                            Map<String, Object> args = env.getArguments();
                            return programDetails(args);
                        })
                        .dataFetcher("projectDetails", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectDetails(args);
                        })
                        .dataFetcher("findProgramIdsInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return findProgramIdsInList(args);
                        })
                )
                .build();
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), REGULAR_PARAMS, "nested_filters", indexType);
        List<String> only_includes;
        List<String> valueSet = INCLUDE_PARAMS.contains(category) ? (List<String>)params.get(category) : List.of();
        if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))){
            only_includes = valueSet;
        } else {
            only_includes = List.of();
        }
        return getGroupCount(category, query, endpoint, cardinalityAggName, only_includes);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountByRange(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), REGULAR_PARAMS, "nested_filters", indexType);
        return getGroupCountByRange(category, query, endpoint, cardinalityAggName);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return filterSubjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, category), REGULAR_PARAMS, "nested_filters", indexType);
        return getGroupCount(category, query, endpoint, cardinalityAggName, List.of());
    }

    private JsonArray getNodeCount(String category, Map<String, Object> query, String endpoint) throws IOException {
        query = insEsService.addNodeCountAggregations(query, category);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);
        Map<String, JsonArray> aggs = insEsService.collectNodeCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return buckets;
    }

    private List<Map<String, Object>> getGroupCountByRange(String category, Map<String, Object> query, String endpoint, String cardinalityAggName) throws IOException {
        query = insEsService.addRangeCountAggregations(query, category, cardinalityAggName);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = insEsService.send(request);
        Map<String, JsonArray> aggs = insEsService.collectRangCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return getGroupCountHelper(buckets, cardinalityAggName);
    }

    private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint, String cardinalityAggName, List<String> only_includes) throws IOException {
        if (RANGE_PARAMS.contains(category)) {
            query = insEsService.addRangeAggregations(query, category, only_includes);
            Request request = new Request("GET", endpoint);
            String jsonizedRequest = gson.toJson(query);
            request.setJsonEntity(jsonizedRequest);
            JsonObject jsonObject = insEsService.send(request);
            Map<String, JsonObject> aggs = insEsService.collectRangAggs(jsonObject, category);
            JsonObject ranges = aggs.get(category);

            return getRangeGroupCountHelper(ranges);
        } else {
            String[] AGG_NAMES = new String[] {category};
            query = insEsService.addAggregations(query, AGG_NAMES, cardinalityAggName, only_includes);
            String queryJson = gson.toJson(query);
            Request request = new Request("GET", endpoint);
            request.setJsonEntity(queryJson);
            JsonObject jsonObject = insEsService.send(request);
            Map<String, JsonArray> aggs = insEsService.collectTermAggs(jsonObject, AGG_NAMES);
            JsonArray buckets = aggs.get(category);

            return getGroupCountHelper(buckets, cardinalityAggName);
        }
        
    }

    private List<Map<String, Object>> getRangeGroupCountHelper(JsonObject ranges) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        if (ranges.get("count").getAsInt() == 0) {
            data.add(Map.of("lowerBound", 0,
                    "subjects", 0,
                    "upperBound", 0
            ));
        } else {
            data.add(Map.of("lowerBound", ranges.get("min").getAsInt(),
                    "subjects", ranges.get("count").getAsInt(),
                    "upperBound", ranges.get("max").getAsInt()
            ));
        }
        return data;
    }

    private List<Map<String, Object>> getBooleanGroupCountHelper(JsonObject filters) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map.Entry<String, JsonElement> group: filters.entrySet()) {
            int count = group.getValue().getAsJsonObject().get("parent").getAsJsonObject().get("doc_count").getAsInt();
            if (count > 0) {
                data.add(Map.of("group", group.getKey(),
                    "subjects", count
                ));
            }
        }
        return data;
    }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets, String cardinalityAggName) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                    "subjects", cardinalityAggName != null ? group.getAsJsonObject().get("cardinality_count").getAsJsonObject().get("value").getAsInt() : group.getAsJsonObject().get("doc_count").getAsInt()
            ));

        }
        return data;
    }

    /**
     * Get counts of filtered results
     * @param params The filters applied
     * @return Counts
     * @throws IOException
     */
    private Map<String, Object> stats(Map<String, Object> params) throws IOException {
        Map<String, Object> data = new HashMap<>();

        // Get Grant counts for Explore page stats bar
        Map<String, Object> grantsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "grants");
        String grantsQueryJson = gson.toJson(grantsQuery);
        Request grantsCountRequest = new Request("GET", GRANTS_COUNT_END_POINT);
        grantsCountRequest.setJsonEntity(grantsQueryJson);
        JsonObject grantsCountResult = insEsService.send(grantsCountRequest);
        int numberOfGrants = grantsCountResult.get("count").getAsInt();

        // Get Project counts for Explore page stats bar, and piggyback to get the Program counts
        Map<String, Object> projectsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "projects");
        projectsQuery.put("size", 0);
        projectsQuery.put("aggs", Map.ofEntries(
            Map.entry("programs.program_id", Map.ofEntries(
                Map.entry("nested", Map.ofEntries(
                    Map.entry("path", "programs")
                )),
                Map.entry("aggs", Map.ofEntries(
                    Map.entry("num_programs", Map.ofEntries(
                        Map.entry("cardinality", Map.ofEntries(
                            Map.entry("field", "programs.program_id")
                        ))
                    ))
                ))
            ))
        ));
        String projectsQueryJson = gson.toJson(projectsQuery);
        Request projectsCountRequest = new Request("GET", PROJECTS_END_POINT);
        projectsCountRequest.setJsonEntity(projectsQueryJson);
        JsonObject projectsCountResult = insEsService.send(projectsCountRequest);
        int numberOfProjects = projectsCountResult.getAsJsonObject("hits")
            .getAsJsonObject("total")
            .get("value").getAsInt();
        int numberOfPrograms = projectsCountResult.getAsJsonObject("aggregations")
            .getAsJsonObject("programs.program_id")
            .getAsJsonObject("num_programs")
            .get("value").getAsInt();
        
        // Get Publication counts for Explore page stats bar
        Map<String, Object> publicationsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "publications");
        String publicationsQueryJson = gson.toJson(publicationsQuery);
        Request publicationsCountRequest = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        publicationsCountRequest.setJsonEntity(publicationsQueryJson);
        JsonObject publicationsCountResult = insEsService.send(publicationsCountRequest);
        int numberOfPublications = publicationsCountResult.get("count").getAsInt();

        data.put("numberOfGrants", numberOfGrants);
        data.put("numberOfPrograms", numberOfPrograms);
        data.put("numberOfProjects", numberOfProjects);
        data.put("numberOfPublications", numberOfPublications);

        return data;
    }

    private List<Map<String, Object>> idsLists() throws IOException {
        Map<String, String[][]> indexProperties = Map.of(
            PROGRAMS_END_POINT, new String[][]{
                new String[]{"program_id", "program_id"},
                new String[]{"program_acronym", "program_acronym"},
                new String[]{"program_name", "program_name"}
            }
        );
        //Generic Query
        Map<String, Object> query = esService.buildListQuery();
        //Results Map
        List<Map<String, Object>> results = new ArrayList<>();
        //Iterate through each index properties map and make a request to each endpoint then format the results as
        // String arrays
        for (String endpoint: indexProperties.keySet()){
            Request request = new Request("GET", endpoint);
            String[][] properties = indexProperties.get(endpoint);
            List<String> fields = new ArrayList<>();
            for (String[] prop: properties) {
                fields.add(prop[1]);
            }
            query.put("_source", fields);
            
            List<Map<String, Object>> result = esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE,0);
            results.addAll(result);
        }

        return results;
    }

    private Map<String, Object> searchProjects(Map<String, Object> params) throws IOException {
        Map<String, Object> data = new HashMap<>();

        final String CARDINALITY_AGG_NAME = "cardinality_agg_name";
        final String AGG_NAME = "agg_name";
        final String AGG_ENDPOINT = "agg_endpoint";
        final String WIDGET_QUERY = "widgetQueryName";
        final String FILTER_COUNT_QUERY = "filterCountQueryName";
        // Query related values
        final List<Map<String, String>> PROJECT_TERM_AGGS = new ArrayList<>();
        PROJECT_TERM_AGGS.add(Map.of(
            WIDGET_QUERY, "programCountByProgramDoc",
            AGG_NAME, "program_doc",
            AGG_ENDPOINT, PROGRAMS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
            WIDGET_QUERY, "programCountByFocusArea",
            AGG_NAME, "focus_area",
            AGG_ENDPOINT, PROGRAMS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
            WIDGET_QUERY, "publicationCountByRelativeCitationRatio",
            AGG_NAME, "relative_citation_ratio",
            AGG_ENDPOINT, PUBLICATIONS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
            CARDINALITY_AGG_NAME, "project_id",
            AGG_NAME, "focus_area",
            FILTER_COUNT_QUERY, "filterProjectCountByFocusArea",
            AGG_ENDPOINT, FACETED_PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
            CARDINALITY_AGG_NAME, "project_id",
            AGG_NAME, "cancer_type",
            FILTER_COUNT_QUERY, "filterProjectCountByCancerType",
            AGG_ENDPOINT, FACETED_PROJECTS_END_POINT
        ));

        // Get Grant counts for Explore page stats bar
        Map<String, Object> grantsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "grants");
        String grantsQueryJson = gson.toJson(grantsQuery);
        Request grantsCountRequest = new Request("GET", GRANTS_COUNT_END_POINT);
        grantsCountRequest.setJsonEntity(grantsQueryJson);
        JsonObject grantsCountResult = insEsService.send(grantsCountRequest);
        int numberOfGrants = grantsCountResult.get("count").getAsInt();

        // Get Program counts for Explore page stats bar
        Map<String, Object> programsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "programs");
        String programsQueryJson = gson.toJson(programsQuery);
        Request programsCountRequest = new Request("GET", PROGRAMS_COUNT_END_POINT);
        programsCountRequest.setJsonEntity(programsQueryJson);
        JsonObject programsCountResult = insEsService.send(programsCountRequest);
        int numberOfPrograms = programsCountResult.get("count").getAsInt();

        // Get Project counts for Explore page stats bar
        Map<String, Object> projectsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "projects");
        String projectsQueryJson = gson.toJson(projectsQuery);
        Request projectsCountRequest = new Request("GET", PROJECTS_COUNT_END_POINT);
        projectsCountRequest.setJsonEntity(projectsQueryJson);
        JsonObject projectsCountResult = insEsService.send(projectsCountRequest);
        int numberOfProjects = projectsCountResult.get("count").getAsInt();
        
        // Get Publication counts for Explore page stats bar
        Map<String, Object> publicationsQuery = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "publications");
        String publicationsQueryJson = gson.toJson(publicationsQuery);
        Request publicationsCountRequest = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        publicationsCountRequest.setJsonEntity(publicationsQueryJson);
        JsonObject publicationsCountResult = insEsService.send(publicationsCountRequest);
        int numberOfPublications = publicationsCountResult.get("count").getAsInt();

        data.put("numberOfGrants", numberOfGrants);
        data.put("numberOfPrograms", numberOfPrograms);
        data.put("numberOfProjects", numberOfProjects);
        data.put("numberOfPublications", numberOfPublications);
        
        // widgets data and facet filter counts for projects
        for (var agg: PROJECT_TERM_AGGS) {
            String field = agg.get(AGG_NAME);
            String widgetQueryName = agg.get(WIDGET_QUERY);
            String filterCountQueryName = agg.get(FILTER_COUNT_QUERY);
            String endpoint = agg.get(AGG_ENDPOINT);
            String indexType = endpoint.replace("/", "").replace("_search", "");
            String cardinalityAggName = agg.get(CARDINALITY_AGG_NAME);
            List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint, cardinalityAggName, indexType);
            if(RANGE_PARAMS.contains(field)) {
                data.put(filterCountQueryName, filterCount.get(0));
            } else {
                data.put(filterCountQueryName, filterCount);
            }

            // Nothing more to do if this aggregator isn't for a widget
            if (widgetQueryName == null) {
                continue;
            }
            
            if (RANGE_PARAMS.contains(field)) {
                List<Map<String, Object>> subjectCount = subjectCountByRange(field, params, endpoint, cardinalityAggName, indexType);
                data.put(widgetQueryName, subjectCount);
            } else if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
                List<Map<String, Object>> subjectCount = subjectCountBy(field, params, endpoint, cardinalityAggName, indexType);
                data.put(widgetQueryName, subjectCount);
            } else {
                data.put(widgetQueryName, filterCount);
            }
        }

        return data;
    }

    private List<Map<String, Object>> grantsOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Grants
            new String[]{"fiscal_year", "fiscal_year"},
            new String[]{"grant_id", "grant_id"},
            new String[]{"grant_title", "grant_title"},
            new String[]{"principal_investigators", "principal_investigators"},
            new String[]{"program_officers", "program_officers"},
            new String[]{"grant_end_date", "grant_end_date"},

            // Projects
            new String[]{"project_id", "project_id"},
        };

        String defaultSort = "grant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Grants
            Map.entry("fiscal_year", "fiscal_year"),
            Map.entry("grant_id", "grant_id_sort"),
            Map.entry("grant_title", "grant_title_sort"),
            Map.entry("principal_investigators", "principal_investigators_sort"),
            Map.entry("program_officers", "program_officers_sort"),
            Map.entry("grant_end_date", "grant_end_date"),

            // Projects
            Map.entry("project_id", "project_id_sort")
        );

        return overview(GRANTS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "grants");
    }

    private List<Map<String, Object>> programsOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Programs
            new String[]{"cancer_type_str", "cancer_type_str"},
            new String[]{"data_link", "data_link"},
            new String[]{"data_link_and_program_acronym", "data_link_and_program_acronym"},
            new String[]{"focus_area_str", "focus_area_str"},
            new String[]{"program_id", "program_id"},
            new String[]{"program_acronym", "program_acronym"},
            new String[]{"program_link", "program_link"},
            new String[]{"program_name", "program_name"},

            // Additional fields for download
            // Stub
        };

        String defaultSort = "program_name"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("cancer_type_str", "cancer_type_sort"),
            Map.entry("data_link", "data_link_sort"),
            Map.entry("focus_area_str", "focus_area_sort"),
            Map.entry("program_id", "program_id_sort"),
            Map.entry("program_acronym", "program_acronym_sort"),
            Map.entry("data_link_and_program_acronym", "data_link_and_program_acronym_sort"),
            Map.entry("program_link", "program_link_sort"),
            Map.entry("program_name", "program_name_sort")
        );

        return overview(PROGRAMS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "programs");
    }

    private List<Map<String, Object>> projectsOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Projects
            new String[]{"project_org_name", "project_org_name"},
            new String[]{"project_end_date", "project_end_date"},
            new String[]{"project_id", "project_id"},
            new String[]{"project_start_date", "project_start_date"},
            new String[]{"project_title", "project_title"},

            // Programs
            new String[]{"program_ids", "program_ids"},
            new String[]{"program_names", "program_names"},

            // Additional fields for download
            // Stub
        };

        String defaultSort = "project_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Projects
            Map.entry("project_org_name", "project_org_name_sort"),
            Map.entry("project_end_date", "project_end_date"),
            Map.entry("project_id", "project_id_sort"),
            Map.entry("project_start_date", "project_start_date"),
            Map.entry("project_title", "project_title_sort"),

            // Programs
            Map.entry("program_names", "program_names_sort")

            // Additional fields for download
            // Stub
        );

        return overview(PROJECTS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "projects");
    }

    private List<Map<String, Object>> publicationsOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Publications
            new String[]{"authors", "authors"},
            new String[]{"cited_by", "cited_by"},
            new String[]{"pmid", "pmid"},
            new String[]{"publication_date", "publication_date"},
            new String[]{"relative_citation_ratio", "relative_citation_ratio"},
            new String[]{"publication_title", "publication_title"},

            // Projects
            new String[]{"project_ids", "project_ids"},

            // Additional fields for download
            // Stub
        };

        String defaultSort = "pmid"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Publications
            Map.entry("authors", "authors_sort"),
            Map.entry("cited_by", "cited_by"),
            Map.entry("pmid", "pmid_sort"),
            Map.entry("publication_date", "publication_date"),
            Map.entry("relative_citation_ratio", "relative_citation_ratio"),
            Map.entry("publication_title", "publication_title_sort"),

            // Projects
            Map.entry("project_ids", "project_ids_sort")

            // Additional fields for download
            // Stub
        );

        return overview(PUBLICATIONS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "publications");
    }

    // if the nestedProperty is set, this will filter based upon the params against the nested property for the endpoint's index.
    // otherwise, this will filter based upon the params against the top level properties for the index
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping, Set<String> regular_fields, String nestedProperty, String overviewType) throws IOException {
        Request request = new Request("GET", endpoint);
        Map<String, Object> query = insEsService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), regular_fields, nestedProperty, overviewType);
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        List<Map<String, Object>> page = insEsService.collectPage(request, query, properties, pageSize, offset);
        return page;
    }

    private List<Map<String, Object>> findProgramIdsInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"program_id", "program_id"},
                new String[]{"program_name", "program_name"}
        };

        Map<String, Object> query = esService.buildListQuery(params, Set.of(), false);
        Request request = new Request("GET", PROGRAMS_END_POINT);

        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private Map<String, Map<String, String>> mapSortOrder(String order_by, String direction, String defaultSort, Map<String, String> mapping) {
        String sortDirection = direction;
        String sortOrder = defaultSort; // Default sort order
        Map<String, String> missingDirection = Map.ofEntries(
            Map.entry("asc", "_first"),
            Map.entry("desc", "_last")
        );

        // Invalid sort direction defaults to ascending
        if (!(sortDirection.equalsIgnoreCase("asc") || sortDirection.equalsIgnoreCase("desc"))) {
            sortDirection = "asc";
        }

        // Handle sort order
        if (mapping.containsKey(order_by)) {
            sortOrder = mapping.get(order_by);
        } else {
            logger.info("Order: \"" + order_by + "\" not recognized, use default order");
        }

        return Map.ofEntries(
            Map.entry(sortOrder, Map.ofEntries(
                Map.entry("order", sortDirection),
                Map.entry("missing", missingDirection.get(sortDirection))
            ))
        );
    }

    /**
     * Queries Opensearch for the total Datasets count
     * @return
     * @throws Exception
     */
    private Integer numberOfDatasets() throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = insEsService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_datasets").getAsInt();

        return count;
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

    /**
     * Gets the details for a single Dataset record
     *
     * @param dataset_source_id The ID of the Dataset
     * @return A map of the Dataset record's properties
     * @throws IOException
     */
    private Map<String, Object> datasetDetails(Map<String, Object> params) throws IOException {
        Map<String, Object> dataset;
        String datasetId = (String) params.get("dataset_source_id");
        List<Map<String, Object>> datasets;

        final String[][] PROPERTIES = new String[][]{
            new String[]{"dataset_title", "dataset_title"},
            new String[]{"description", "description"},
            new String[]{"dataset_source_id", "dataset_source_id"},
            new String[]{"dataset_source_url", "dataset_source_url"},
            new String[]{"dataset_doc", "dataset_doc"},
            new String[]{"release_date", "release_date"},
            new String[]{"PI_name", "PI_name"},
            new String[]{"funding_source", "funding_source"},
            new String[]{"dataset_pmid", "dataset_pmid"},
            new String[]{"study_type", "study_type"},
            new String[]{"limitations_for_reuse", "limitations_for_reuse"},
            new String[]{"assay_method", "assay_method"},
            new String[]{"participant_count", "participant_count"},
            new String[]{"sample_count", "sample_count"},
            new String[]{"primary_disease", "primary_disease"},
            new String[]{"related_genes", "related_genes"},
            new String[]{"related_diseases", "related_diseases"},
            new String[]{"related_terms", "related_terms"},
            new String[]{"study_links", "study_links"},
        };

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("dataset_title", "dataset_title"),
            Map.entry("description", "description"),
            Map.entry("dataset_source_id", "dataset_source_id"),
            Map.entry("dataset_source_url", "dataset_source_url"),
            Map.entry("dataset_doc", "dataset_doc"),
            Map.entry("release_date", "release_date"),
            Map.entry("PI_name", "PI_name"),
            Map.entry("funding_source", "funding_source"),
            Map.entry("dataset_pmid", "dataset_pmid"),
            Map.entry("study_type", "study_type"),
            Map.entry("limitations_for_reuse", "limitations_for_reuse"),
            Map.entry("assay_method", "assay_method"),
            Map.entry("participant_count", "participant_count"),
            Map.entry("sample_count", "sample_count"),
            Map.entry("primary_disease", "primary_disease"),
            Map.entry("related_genes", "related_genes"),
            Map.entry("related_diseases", "related_diseases"),
            Map.entry("related_terms", "related_terms"),
            Map.entry("study_links", "study_links")
        );

        Map<String, Object> dataset_params = Map.ofEntries(
            Map.entry("dataset_source_id", List.of(datasetId)),
            Map.entry(ORDER_BY, "dataset_source_id"),
            Map.entry(SORT_DIRECTION, "ASC"),
            Map.entry(PAGE_SIZE, 1),
            Map.entry(OFFSET, 0)
        );

        datasets = overview(DATASETS_END_POINT, dataset_params, PROPERTIES, "dataset_source_id", mapping, REGULAR_PARAMS, "nested_filters", "datasets");

        try {
            dataset = datasets.get(0);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }

        return dataset;
    }

    /**
     * Gets the details for a single Program record
     *
     * @param programId The ID of the Program
     * @return A map of the Program record's properties
     * @throws IOException
     */
    private Map<String, Object> programDetails(Map<String, Object> params) throws IOException {
        Map<String, Object> program;
        String programId = (String) params.get("program_id");
        List<Map<String, Object>> programs;

        final String[][] PROPERTIES = new String[][]{
            new String[]{"cancer_type", "cancer_type"},
            new String[]{"contact_nih", "contact_nih"},
            new String[]{"contact_pi", "contact_pi"},
            new String[]{"program_doc", "program_doc"},
            new String[]{"focus_area", "focus_area"},
            new String[]{"nofo", "nofo"},
            new String[]{"program_acronym", "program_acronym"},
            new String[]{"program_link", "program_link"},
            new String[]{"program_name", "program_name"},
        };

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("cancer_type", "cancer_type"),
            Map.entry("contact_nih", "contact_nih"),
            Map.entry("contact_pi", "contact_pi"),
            Map.entry("program_doc", "program_doc"),
            Map.entry("focus_area", "focus_area"),
            Map.entry("nofo", "nofo"),
            Map.entry("program_acronym", "program_acronym"),
            Map.entry("program_link", "program_link"),
            Map.entry("program_name", "program_name")
        );

        Map<String, Object> program_params = Map.ofEntries(
            Map.entry("program_id", List.of(programId)),
            Map.entry(ORDER_BY, "program_id"),
            Map.entry(SORT_DIRECTION, "ASC"),
            Map.entry(PAGE_SIZE, 1),
            Map.entry(OFFSET, 0)
        );

        programs = overview(PROGRAMS_END_POINT, program_params, PROPERTIES, "program_id", mapping, REGULAR_PARAMS, "nested_filters", "programs");

        try {
            program = programs.get(0);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }

        return program;
    }

    /**
     * Gets the details for a single Project record
     *
     * @param projectId The ID of the Project
     * @return A map of the Project record's properties
     * @throws IOException
     */
    private Map<String, Object> projectDetails(Map<String, Object> params) throws IOException {
        Map<String, Object> project;
        String projectId = (String) params.get("project_id");
        List<Map<String, Object>> projects;

        final String[][] PROPERTIES = new String[][]{
            new String[]{"project_abstract_text", "project_abstract_text"},
            new String[]{"project_opportunity_number", "project_opportunity_number"},
            new String[]{"project_org_name", "project_org_name"},
            new String[]{"program_acronyms", "program_acronyms"},
            new String[]{"program_ids", "program_ids"},
            new String[]{"project_end_date", "project_end_date"},
            new String[]{"project_id", "project_id"},
            new String[]{"project_start_date", "project_start_date"},
            new String[]{"project_title", "project_title"},
        };

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("project_abstract_text", "project_abstract_text"),
            Map.entry("project_opportunity_number", "project_opportunity_number"),
            Map.entry("project_org_name", "project_org_name"),
            Map.entry("program_acronyms", "program_acronyms"),
            Map.entry("program_ids", "program_ids"),
            Map.entry("project_end_date", "project_end_date"),
            Map.entry("project_id", "project_id"),
            Map.entry("project_start_date", "project_start_date"),
            Map.entry("project_title", "project_title")
        );

        Map<String, Object> project_params = Map.ofEntries(
            Map.entry("project_id", List.of(projectId)),
            Map.entry(ORDER_BY, "project_id"),
            Map.entry(SORT_DIRECTION, "ASC"),
            Map.entry(PAGE_SIZE, 1),
            Map.entry(OFFSET, 0)
        );

        projects = overview(PROJECTS_END_POINT, project_params, PROPERTIES, "project_id", mapping, REGULAR_PARAMS, "nested_filters", "projects");

        try {
            project = projects.get(0);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }

        return project;
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
