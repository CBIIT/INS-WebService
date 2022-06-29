package gov.nih.nci.bento.model;

import com.google.gson.*;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

public class BentoEsFilter implements DataFetcher {
    private static final Logger logger = LogManager.getLogger(BentoEsFilter.class);

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String PROGRAMS_END_POINT = "/programs/_search";
    final String PROGRAMS_COUNT_END_POINT = "/programs/_count";
    final String PROJECTS_END_POINT = "/projects/_search";
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
    final String GS_COUNT_ENDPOINT = "count_endpoint";
    final String GS_RESULT_FIELD = "result_field";
    final String GS_COUNT_RESULT_FIELD = "count_result_field";
    final String GS_SEARCH_FIELD = "search_field";
    final String GS_COLLECT_FIELDS = "collect_fields";
    final String GS_SORT_FIELD = "sort_field";
    final String GS_CATEGORY_TYPE = "type";
    final String GS_ABOUT = "about";
    final String GS_HIGHLIGHT_FIELDS = "highlight_fields";
    final String GS_HIGHLIGHT_DELIMITER = "$";
    final Set<String> RANGE_PARAMS = Set.of("age_at_index");


    @Autowired
    ESService esService;

    private Gson gson = new GsonBuilder().serializeNulls().create();

    @Override
    public RuntimeWiring buildRuntimeWiring() {
        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("QueryType")
                        // .dataFetcher("searchSubjects", env -> {
                        //     Map<String, Object> args = env.getArguments();
                        //     return searchSubjects(args);
                        // })
                        .dataFetcher("subjectOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return subjectOverview(args);
                        })
                        .dataFetcher("sampleOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return sampleOverview(args);
                        })
                        .dataFetcher("fileOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return fileOverview(args);
                        })
                        .dataFetcher("globalSearch", env -> {
                            Map<String, Object> args = env.getArguments();
                            return globalSearch(args);
                        })
                        .dataFetcher("fileIDsFromList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return fileIDsFromList(args);
                        })
                        .dataFetcher("filesInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return filesInList(args);
                        })
                        .dataFetcher("findSubjectIdsInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return findSubjectIdsInList(args);
                        })
                        .dataFetcher("searchProjects", env -> {
                            Map<String, Object> args = env.getArguments();
                            return searchProjects(args);
                        })
                        .dataFetcher("patentOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return patentOverView(args);
                        })
                        .dataFetcher("clinicalTrialOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return clinicalTrialOverView(args);
                        })
                        .dataFetcher("datasetOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return datasetOverView(args);
                          })
                        .dataFetcher("publicationOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return publicationOverview(args);
                         })
                        .dataFetcher("projectOverView", env -> {
                            Map<String, Object> args = env.getArguments();
                            return projectOverView(args);
                        })
                )
                .build();
    }

    // private Map<String, Object> searchSubjects(Map<String, Object> params) throws IOException {
    //     final String AGG_NAME = "agg_name";
    //     final String AGG_ENDPOINT = "agg_endpoint";
    //     final String WIDGET_QUERY = "widgetQueryName";
    //     final String FILTER_COUNT_QUERY = "filterCountQueryName";
    //     // Query related values
    //     final List<Map<String, String>> TERM_AGGS = new ArrayList<>();
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "programs",
    //             WIDGET_QUERY, "subjectCountByProgram",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByProgram",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "studies",
    //             WIDGET_QUERY, "subjectCountByStudy",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByStudy",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "diagnoses",
    //             WIDGET_QUERY, "subjectCountByDiagnoses",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByDiagnoses",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "rc_scores",
    //             WIDGET_QUERY,"subjectCountByRecurrenceScore",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByRecurrenceScore",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "tumor_sizes",
    //             WIDGET_QUERY, "subjectCountByTumorSize",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByTumorSize",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "tumor_grades",
    //             WIDGET_QUERY, "subjectCountByTumorGrade",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByTumorGrade",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "er_status",
    //             WIDGET_QUERY, "subjectCountByErStatus",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByErStatus",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "pr_status",
    //             WIDGET_QUERY, "subjectCountByPrStatus",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByPrStatus",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "chemo_regimen",
    //             WIDGET_QUERY, "subjectCountByChemotherapyRegimen",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByChemotherapyRegimen",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "endo_therapies",
    //             WIDGET_QUERY, "subjectCountByEndocrineTherapy",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByEndocrineTherapy",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "meno_status",
    //             WIDGET_QUERY, "subjectCountByMenopauseStatus",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByMenopauseStatus",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "tissue_type",
    //             WIDGET_QUERY, "subjectCountByTissueType",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByTissueType",
    //             AGG_ENDPOINT, SAMPLES_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "composition",
    //             WIDGET_QUERY, "subjectCountByTissueComposition",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByTissueComposition",
    //             AGG_ENDPOINT, SAMPLES_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "association",
    //             WIDGET_QUERY, "subjectCountByFileAssociation",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByFileAssociation",
    //             AGG_ENDPOINT, FILES_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "file_type",
    //             WIDGET_QUERY, "subjectCountByFileType",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByFileType",
    //             AGG_ENDPOINT, FILES_END_POINT
    //     ));
    //     TERM_AGGS.add(Map.of(
    //             AGG_NAME, "lab_procedures",
    //             WIDGET_QUERY, "subjectCountByLabProcedures",
    //             FILTER_COUNT_QUERY, "filterSubjectCountByLabProcedures",
    //             AGG_ENDPOINT, SUBJECTS_END_POINT
    //     ));

    //     List<String> agg_names = new ArrayList<>();
    //     for (var agg: TERM_AGGS) {
    //         agg_names.add(agg.get(AGG_NAME));
    //     }
    //     final String[] TERM_AGG_NAMES = agg_names.toArray(new String[TERM_AGGS.size()]);

    //     final Map<String, String> RANGE_AGGS = new HashMap<>();
    //     RANGE_AGGS.put("age_at_index",  "filterSubjectCountByAge");
    //     final String[] RANGE_AGG_NAMES = RANGE_AGGS.keySet().toArray(new String[0]);

    //     Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS);
    //     Request sampleCountRequest = new Request("GET", SAMPLES_COUNT_END_POINT);
    //     sampleCountRequest.setJsonEntity(gson.toJson(query));
    //     JsonObject sampleCountResult = esService.send(sampleCountRequest);
    //     int numberOfSamples = sampleCountResult.get("count").getAsInt();

    //     Request fileCountRequest = new Request("GET", FILES_COUNT_END_POINT);
    //     fileCountRequest.setJsonEntity(gson.toJson(query));
    //     JsonObject fileCountResult = esService.send(fileCountRequest);
    //     int numberOfFiles = fileCountResult.get("count").getAsInt();

    //     Request subjectCountRequest = new Request("GET", SUBJECTS_COUNT_END_POINT);
    //     subjectCountRequest.setJsonEntity(gson.toJson(query));
    //     JsonObject subjectCountResult = esService.send(subjectCountRequest);
    //     int numberOfSubjects = subjectCountResult.get("count").getAsInt();


    //     // Get aggregations
    //     Map<String, Object> aggQuery = esService.addAggregations(query, TERM_AGG_NAMES, RANGE_AGG_NAMES);
    //     Request subjectRequest = new Request("GET", SUBJECTS_END_POINT);
    //     subjectRequest.setJsonEntity(gson.toJson(aggQuery));
    //     JsonObject subjectResult = esService.send(subjectRequest);
    //     Map<String, JsonArray> aggs = esService.collectTermAggs(subjectResult, TERM_AGG_NAMES);

    //     Map<String, Object> data = new HashMap<>();
    //     data.put("numberOfPrograms", aggs.get("programs").size());
    //     data.put("numberOfStudies", aggs.get("studies").size());
    //     data.put("numberOfLabProcedures", aggs.get("lab_procedures").size());
    //     data.put("numberOfSubjects", numberOfSubjects);
    //     data.put("numberOfSamples", numberOfSamples);
    //     data.put("numberOfFiles", numberOfFiles);

    //     data.put("armsByPrograms", armsByPrograms(params));
    //     // widgets data and facet filter counts
    //     for (var agg: TERM_AGGS) {
    //         String field = agg.get(AGG_NAME);
    //         String widgetQueryName = agg.get(WIDGET_QUERY);
    //         String filterCountQueryName = agg.get(FILTER_COUNT_QUERY);
    //         String endpoint = agg.get(AGG_ENDPOINT);
    //         // subjectCountByXXXX
    //         List<Map<String, Object>> widgetData;
    //         if (endpoint.equals(SUBJECTS_END_POINT)) {
    //             widgetData = getGroupCountHelper(aggs.get(field));
    //             data.put(widgetQueryName, widgetData);
    //         } else {
    //             widgetData = subjectCountBy(field, params, endpoint);;
    //             data.put(widgetQueryName, widgetData);
    //         }
    //         // filterSubjectCountByXXXX
    //         if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
    //             List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint);;
    //             data.put(filterCountQueryName, filterCount);
    //         } else {
    //             data.put(filterCountQueryName, widgetData);
    //         }
    //     }

    //     Map<String, JsonObject> rangeAggs = esService.collectRangeAggs(subjectResult, RANGE_AGG_NAMES);

    //     for (String field: RANGE_AGG_NAMES) {
    //         String filterCountQueryName = RANGE_AGGS.get(field);
    //         if (params.containsKey(field) && ((List<Double>)params.get(field)).size() >= 2) {
    //             Map<String, Object> filterCount = rangeFilterSubjectCountBy(field, params);;
    //             data.put(filterCountQueryName, filterCount);
    //         } else {
    //             data.put(filterCountQueryName, getRange(rangeAggs.get(field)));
    //         }
    //     }

    //     return data;
    // }

    private List<Map<String, Object>> subjectOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"subject_id", "subject_ids"},
                new String[]{"program", "programs"},
                new String[]{"program_id", "program_id"},
                new String[]{"study_acronym", "study_acronym"},
                new String[]{"study_short_description", "study_short_description"},
                new String[]{"study_info", "studies"},
                new String[]{"diagnosis", "diagnoses"},
                new String[]{"recurrence_score", "rc_scores"},
                new String[]{"tumor_size", "tumor_sizes"},
                new String[]{"tumor_grade", "tumor_grades"},
                new String[]{"er_status", "er_status"},
                new String[]{"pr_status", "pr_status"},
                new String[]{"chemotherapy", "chemo_regimen"},
                new String[]{"endocrine_therapy", "endo_therapies"},
                new String[]{"menopause_status", "meno_status"},
                new String[]{"age_at_index", "age_at_index"},
                new String[]{"survival_time", "survival_time"},
                new String[]{"survival_time_unit", "survival_time_unit"},
                new String[]{"files", "files"},
                new String[]{"samples", "samples"},
                new String[]{"lab_procedures", "lab_procedures"},
        };

        String defaultSort = "subject_id_num"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("subject_id", "subject_id_num"),
                Map.entry("program", "programs"),
                Map.entry("program_id", "program_id"),
                Map.entry("study_acronym", "study_acronym"),
                Map.entry("study_short_description", "study_short_description"),
                Map.entry("study_info", "studies"),
                Map.entry("diagnosis", "diagnoses"),
                Map.entry("recurrence_score", "rc_scores"),
                Map.entry("tumor_size", "tumor_sizes"),
                Map.entry("tumor_grade", "tumor_grades"),
                Map.entry("er_status", "er_status"),
                Map.entry("pr_status", "pr_status"),
                Map.entry("chemotherapy", "chemo_regimen"),
                Map.entry("endocrine_therapy", "endo_therapies"),
                Map.entry("menopause_status", "meno_status"),
                Map.entry("age_at_index", "age_at_index"),
                Map.entry("survival_time", "survival_time")
        );

        return overview(SUBJECTS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }

    private List<Map<String, Object>> sampleOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"program", "programs"},
                new String[]{"program_id", "program_id"},
                new String[]{"arm", "study_acronym"},
                new String[]{"subject_id", "subject_ids"},
                new String[]{"sample_id", "sample_ids"},
                new String[]{"diagnosis", "diagnoses"},
                new String[]{"tissue_type", "tissue_type"},
                new String[]{"tissue_composition", "composition"},
                new String[]{"sample_anatomic_site", "sample_anatomic_site"},
                new String[]{"sample_procurement_method", "sample_procurement_method"},
                new String[]{"platform", "platform"},
                new String[]{"files", "files"}
        };

        String defaultSort = "sample_id_num"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("program", "programs"),
                Map.entry("arm", "study_acronym"),
                Map.entry("subject_id", "subject_id_num"),
                Map.entry("sample_id", "sample_id_num"),
                Map.entry("diagnosis", "diagnoses"),
                Map.entry("tissue_type", "tissue_type"),
                Map.entry("tissue_composition", "composition"),
                Map.entry("sample_anatomic_site", "sample_anatomic_site"),
                Map.entry("sample_procurement_method", "sample_procurement_method"),
                Map.entry("platform", "platform")
        );

        return overview(SAMPLES_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }

    private List<Map<String, Object>> fileOverview(Map<String, Object> params) throws IOException {
        // Following String array of arrays should be in form of "GraphQL_field_name", "ES_field_name"
        final String[][] PROPERTIES = new String[][]{
                new String[]{"program", "programs"},
                new String[]{"program_id", "program_id"},
                new String[]{"arm", "study_acronym"},
                new String[]{"subject_id", "subject_ids"},
                new String[]{"sample_id", "sample_ids"},
                new String[]{"file_id", "file_ids"},
                new String[]{"file_name", "file_names"},
                new String[]{"association", "association"},
                new String[]{"file_description", "file_description"},
                new String[]{"file_format", "file_format"},
                new String[]{"file_size", "file_size"},
                new String[]{"diagnosis", "diagnoses"}
        };

        String defaultSort = "file_name"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("program", "programs"),
                Map.entry("arm", "study_acronym"),
                Map.entry("subject_id", "subject_id_num"),
                Map.entry("sample_id", "sample_id_num"),
                Map.entry("file_id", "file_id_num"),
                Map.entry("file_name", "file_names"),
                Map.entry("association", "association"),
                Map.entry("file_description", "file_description"),
                Map.entry("file_format", "file_format"),
                Map.entry("file_size", "file_size"),
                Map.entry("diagnosis", "diagnoses")
        );

        return overview(FILES_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }

    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping) throws IOException {

        Request request = new Request("GET", endpoint);
        Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION));
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        List<Map<String, Object>> page = esService.collectPage(request, query, properties, pageSize, offset);
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

    // private List<Map<String, Object>> armsByPrograms(Map<String, Object> params) throws IOException {
    //     final String category = "programs";
    //     final String subCategory = "study_acronym";

    //     String[] subCategories = new String[] { subCategory };
    //     Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE));
    //     String[] AGG_NAMES = new String[] {category};
    //     query = esService.addAggregations(query, AGG_NAMES);
    //     esService.addSubAggregations(query, category, subCategories);
    //     Request request = new Request("GET", SUBJECTS_END_POINT);
    //     request.setJsonEntity(gson.toJson(query));
    //     JsonObject jsonObject = esService.send(request);
    //     Map<String, JsonArray> aggs = esService.collectTermAggs(jsonObject, AGG_NAMES);
    //     JsonArray buckets = aggs.get(category);

    //     List<Map<String, Object>> data = new ArrayList<>();
    //     for (JsonElement group: buckets) {
    //         List<Map<String, Object>> studies = new ArrayList<>();

    //         for (JsonElement studyElement: group.getAsJsonObject().get(subCategory).getAsJsonObject().get("buckets").getAsJsonArray()) {
    //             JsonObject study = studyElement.getAsJsonObject();
    //             int size = study.get("doc_count").getAsInt();
    //             studies.add(Map.of(
    //                     "arm", study.get("key").getAsString(),
    //                     "caseSize", size,
    //                     "size", size
    //             ));
    //         }
    //         data.add(Map.of("program", group.getAsJsonObject().get("key").getAsString(),
    //                 "caseSize", group.getAsJsonObject().get("doc_count").getAsInt(),
    //                 "children", studies
    //         ));

    //     }
    //     return data;
    // }

    // private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint) throws IOException {
    //     Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE));
    //     return getGroupCount(category, query, endpoint);
    // }

    // private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint) throws IOException {
    //     Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, category));
    //     return getGroupCount(category, query, endpoint);
    // }

    // private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint) throws IOException {
    //     String[] AGG_NAMES = new String[] {category};
    //     query = esService.addAggregations(query, AGG_NAMES);
    //     Request request = new Request("GET", endpoint);
    //     request.setJsonEntity(gson.toJson(query));
    //     JsonObject jsonObject = esService.send(request);
    //     Map<String, JsonArray> aggs = esService.collectTermAggs(jsonObject, AGG_NAMES);
    //     JsonArray buckets = aggs.get(category);

    //     return getGroupCountHelper(buckets);
    // }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                    "subjects", group.getAsJsonObject().get("doc_count").getAsInt()
            ));

        }
        return data;
    }

    private Map<String, Object> rangeFilterSubjectCountBy(String category, Map<String, Object> params) throws IOException {
        Map<String, Object> query = esService.buildFacetFilterQuery(params, RANGE_PARAMS,Set.of(PAGE_SIZE, category));
        return getRangeCount(category, query);
    }

    private Map<String, Object> getRangeCount(String category, Map<String, Object> query) throws IOException {
        String[] AGG_NAMES = new String[] {category};
        query = esService.addAggregations(query, new String[]{}, AGG_NAMES);
        Request request = new Request("GET", SUBJECTS_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = esService.send(request);
        Map<String, JsonObject> aggs = esService.collectRangeAggs(jsonObject, AGG_NAMES);
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
                GS_END_POINT, PROGRAMS_END_POINT,
                GS_COUNT_ENDPOINT, PROGRAMS_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "program_count",
                GS_RESULT_FIELD, "programs",
                GS_SEARCH_FIELD, List.of("program_id", "program_code", "program_name"),
                GS_SORT_FIELD, "program_id_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"program_code", "program_code"},
                        new String[]{"program_id", "program_id"},
                        new String[]{"program_name", "program_name"}
                },
                GS_CATEGORY_TYPE, "program"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, STUDIES_END_POINT,
                GS_COUNT_ENDPOINT, STUDIES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "study_count",
                GS_RESULT_FIELD, "studies",
                GS_SEARCH_FIELD, List.of("study_id", "study_name", "study_type"),
                GS_SORT_FIELD, "study_id_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"program_id", "program_id"},
                        new String[]{"study_id", "study_id"},
                        new String[]{"study_type", "study_type"},
                        new String[]{"study_code", "study_code"},
                        new String[]{"study_name", "study_name"}
                },
                GS_CATEGORY_TYPE, "study"

        ));
        searchCategories.add(Map.of(
                GS_END_POINT, SUBJECTS_END_POINT,
                GS_COUNT_ENDPOINT, SUBJECTS_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "subject_count",
                GS_RESULT_FIELD, "subjects",
                GS_SEARCH_FIELD, List.of("subject_id_gs", "diagnosis_gs", "age_at_index_gs"),
                GS_SORT_FIELD, "subject_id_num",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"program_id", "program_id"},
                        new String[]{"subject_id", "subject_id_gs"},
                        new String[]{"program_code", "programs"},
                        new String[]{"study", "study_acronym"},
                        new String[]{"diagnosis", "diagnoses"},
                        new String[]{"age", "age_at_index"}
                },
                GS_CATEGORY_TYPE, "subject"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, SAMPLES_END_POINT,
                GS_COUNT_ENDPOINT, SAMPLES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "sample_count",
                GS_RESULT_FIELD, "samples",
                GS_SEARCH_FIELD, List.of("sample_id_gs", "sample_anatomic_site_gs", "tissue_type_gs"),
                GS_SORT_FIELD, "sample_id_num",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"program_id", "program_id"},
                        new String[]{"subject_id", "subject_ids"},
                        new String[]{"sample_id", "sample_ids"},
                        new String[]{"diagnosis", "diagnoses"},
                        new String[]{"sample_anatomic_site", "sample_anatomic_site"},
                        new String[]{"tissue_type", "tissue_type"}
                },
                GS_CATEGORY_TYPE, "sample"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, FILES_END_POINT,
                GS_COUNT_ENDPOINT, FILES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "file_count",
                GS_RESULT_FIELD, "files",
                GS_SEARCH_FIELD, List.of("file_id_gs", "file_name_gs", "file_format_gs"),
                GS_SORT_FIELD, "file_id_num",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"program_id", "program_id"},
                        new String[]{"subject_id", "subject_ids"},
                        new String[]{"sample_id", "sample_ids"},
                        new String[]{"file_name", "file_names"},
                        new String[]{"file_format", "file_format"},
                        new String[]{"file_id", "file_ids"}
                },
                GS_CATEGORY_TYPE, "file"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, NODES_END_POINT,
                GS_COUNT_ENDPOINT, NODES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("node"),
                GS_SORT_FIELD, "node_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node_name", "node"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "node"}
                },
                GS_CATEGORY_TYPE, "node"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, PROPERTIES_END_POINT,
                GS_COUNT_ENDPOINT, PROPERTIES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("property", "property_description", "property_type", "property_required"),
                GS_SORT_FIELD, "property_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node_name", "node"},
                        new String[]{"property_name", "property"},
                        new String[]{"property_type", "property_type"},
                        new String[]{"property_required", "property_required"},
                        new String[]{"property_description", "property_description"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "property"},
                        new String[]{"highlight", "property_description"},
                        new String[]{"highlight", "property_type"},
                        new String[]{"highlight", "property_required"}
                },
                GS_CATEGORY_TYPE, "property"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, VALUES_END_POINT,
                GS_COUNT_ENDPOINT, VALUES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("value"),
                GS_SORT_FIELD, "value_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node_name", "node"},
                        new String[]{"property_name", "property"},
                        new String[]{"property_type", "property_type"},
                        new String[]{"property_required", "property_required"},
                        new String[]{"property_description", "property_description"},
                        new String[]{"value", "value"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "value"}
                },
                GS_CATEGORY_TYPE, "value"
        ));

        Set<String> combinedCategories = Set.of("model") ;

        for (Map<String, Object> category: searchCategories) {
            String countResultFieldName = (String) category.get(GS_COUNT_RESULT_FIELD);
            String resultFieldName = (String) category.get(GS_RESULT_FIELD);
            String[][] properties = (String[][]) category.get(GS_COLLECT_FIELDS);
            String[][] highlights = (String[][]) category.get(GS_HIGHLIGHT_FIELDS);
            Map<String, Object> query = getGlobalSearchQuery(input, category);

            // Get count
            Request countRequest = new Request("GET", (String) category.get(GS_COUNT_ENDPOINT));
            countRequest.setJsonEntity(gson.toJson(query));
            JsonObject countResult = esService.send(countRequest);
            int oldCount = (int)result.getOrDefault(countResultFieldName, 0);
            result.put(countResultFieldName, countResult.get("count").getAsInt() + oldCount);

            // Get results
            Request request = new Request("GET", (String)category.get(GS_END_POINT));
            String sortFieldName = (String)category.get(GS_SORT_FIELD);
            query.put("sort", Map.of(sortFieldName, "asc"));
            query = addHighlight(query, category);

            if (combinedCategories.contains(resultFieldName)) {
                query.put("size", ESService.MAX_ES_SIZE);
                query.put("from", 0);
            } else {
                query.put("size", size);
                query.put("from", offset);
            }
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = esService.send(request);
            List<Map<String, Object>> objects = esService.collectPage(jsonObject, properties, highlights, (int)query.get("size"), 0);

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

        for (String category: combinedCategories) {
            List<Object> pagedCategory = paginate((List)result.get(category), size, offset);
            result.put(category, pagedCategory);
        }

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
                "size", ESService.MAX_ES_SIZE
        );
        Request request = new Request("GET", GS_ABOUT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = esService.send(request);

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

    private Map<String, Object> getGlobalSearchQuery(String input, Map<String, Object> category) {
        List<String> searchFields = (List<String>)category.get(GS_SEARCH_FIELD);
        List<Object> searchClauses = new ArrayList<>();
        for (String searchFieldName: searchFields) {
            searchClauses.add(Map.of("match_phrase_prefix", Map.of(searchFieldName, input)));
        }
        Map<String, Object> query = new HashMap<>();
        query.put("query", Map.of("bool", Map.of("should", searchClauses)));
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

        Map<String, Object> query = esService.buildListQuery(params, Set.of(), true);
        Request request = new Request("GET", SUBJECT_IDS_END_POINT);

        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
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

        Map<String, Object> query = esService.buildListQuery(params, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION));
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        Request request = new Request("GET", FILES_END_POINT);

        return esService.collectPage(request, query, properties, pageSize, offset);
    }

    private List<String> fileIDsFromList(Map<String, Object> params) throws IOException {
        return collectFieldFromList(params, "file_ids", FILES_END_POINT);
    }

    // This function search values in parameters and return a given collectField's unique values in a list
    private List<String> collectFieldFromList(Map<String, Object> params, String collectField, String endpoint) throws IOException {
        String[] idFieldArray = new String[]{collectField};
        Map<String, Object> query = esService.buildListQuery(params, Set.of());
        query = esService.addAggregations(query, idFieldArray);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = esService.send(request);
        return esService.collectTerms(jsonObject, collectField);
    }

    // private Map<String, Object> searchProjects(Map<String, Object> params) throws IOException {
    //     Map<String, Object> query = esService.buildListQuery(params, Set.of());
    //     Request request = new Request("GET", SEARCH_PROJECTS_ES_END_POINT);
    //     request.setJsonEntity(gson.toJson(query));

    //     Map<String, Object> result = new HashMap<String, Object>();
    //     result.put(GS_CATEGORY_TYPE, SEARCH_PROJECTS_ES_END_POINT);

    //     List<String> programs = esService.collectField(request, "programs").stream().distinct().collect(Collectors.toList());
    //     List<String> projectIds = esService.collectFieldForArray(request, "project_ids").stream().distinct().collect(Collectors.toList());
    //     List<String> publicationIds = esService.collectFieldForArray(request, "publication_ids").stream().distinct().collect(Collectors.toList());
    //     List<String> accessions = esService.collectFieldForArray(request, "accessions").stream().distinct().collect(Collectors.toList());
    //     List<String> clinicalTrialIds = esService.collectFieldForArray(request, "clinical_trial_ids").stream().distinct().collect(Collectors.toList());
    //     List<String> patentIds = esService.collectFieldForArray(request, "patent_ids").stream().distinct().collect(Collectors.toList());

    //     result.put("projectIds", projectIds);
    //     result.put("publicationIds", publicationIds);
    //     result.put("accessions", accessions);
    //     result.put("clinicalTrialIds", clinicalTrialIds);
    //     result.put("patentIds", patentIds);
        
    //     Integer numberOfPrograms = programs.size();
    //     result.put("numberOfPrograms", numberOfPrograms);
    //     Integer numberOfProjects = projectIds.size();
    //     result.put("numberOfProjects", numberOfProjects);
    //     Integer numberOfPublications = publicationIds.size();
    //     result.put("numberOfPublications", numberOfPublications);
    //     Integer numberOfAccessions = accessions.size();
    //     result.put("numberOfAccessions", numberOfAccessions);
    //     Integer numberOfClinicalTrials = clinicalTrialIds.size();
    //     result.put("numberOfClinicalTrials", numberOfClinicalTrials);
    //     Integer numberOfPatents = patentIds.size();
    //     result.put("numberOfPatents", numberOfPatents);

    //     return result;
    // }

    private Map<String, Object> searchProjects(Map<String, Object> params) throws IOException {
        final String AGG_NAME = "agg_name";
        final String AGG_ENDPOINT = "agg_endpoint";
        final String WIDGET_QUERY = "widgetQueryName";
        final String FILTER_COUNT_QUERY = "filterCountQueryName";
        // Query related values
        final List<Map<String, String>> PROJECT_TERM_AGGS = new ArrayList<>();
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "program_id",
                WIDGET_QUERY, "projectCountByProgram",
                FILTER_COUNT_QUERY, "filterProjectCountByProgram",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "lead_doc",
                WIDGET_QUERY, "projectCountByDoc",
                FILTER_COUNT_QUERY, "filterProjectCountByDoc",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "fiscal_year",
                WIDGET_QUERY, "projectCountByFiscalYear",
                FILTER_COUNT_QUERY, "filterProjectCountByFiscalYear",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PROJECT_TERM_AGGS.add(Map.of(
                AGG_NAME, "award_amount_category",
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
                WIDGET_QUERY, "projectCountByProgram",
                FILTER_COUNT_QUERY, "filterProjectCountByProgram",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PUBLICATION_TERM_AGGS.add(Map.of(
                AGG_NAME, "rcr_range",
                WIDGET_QUERY, "projectCountByDoc",
                FILTER_COUNT_QUERY, "filterProjectCountByDoc",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        PUBLICATION_TERM_AGGS.add(Map.of(
                AGG_NAME, "year",
                WIDGET_QUERY, "projectCountByFiscalYear",
                FILTER_COUNT_QUERY, "filterProjectCountByFiscalYear",
                AGG_ENDPOINT, PROJECTS_END_POINT
        ));
        List<String> publication_agg_names = new ArrayList<>();
        for (var agg: PUBLICATION_TERM_AGGS) {
            publication_agg_names.add(agg.get(AGG_NAME));
        }
        final String[] PUBLICATIONS_TERM_AGG_NAMES = publication_agg_names.toArray(new String[PUBLICATION_TERM_AGGS.size()]);

        Map<String, Object> query = esService.buildFacetFilterQuery(params, Set.of());  // RANGE_PARAMS

        Request programsCountRequest = new Request("GET", PROGRAMS_COUNT_END_POINT);
        programsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject programsCountResult = esService.send(programsCountRequest);
        int numberOfPrograms = programsCountResult.get("count").getAsInt();

        Request projectsCountRequest = new Request("GET", PROJECTS_COUNT_END_POINT);
        projectsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject projectsCountResult = esService.send(projectsCountRequest);
        int numberOfProjects = projectsCountResult.get("count").getAsInt();

        Request publicationsCountRequest = new Request("GET", PUBLICATIONS_COUNT_END_POINT);
        publicationsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject publicationsCountResult = esService.send(publicationsCountRequest);
        int numberOfPublications = publicationsCountResult.get("count").getAsInt();

        Request datasetsCountRequest = new Request("GET", DATASETS_COUNT_END_POINT);
        datasetsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject datasetsCountResult = esService.send(datasetsCountRequest);
        int numberOfDatasets = datasetsCountResult.get("count").getAsInt();

        Request clinicalTrialsCountRequest = new Request("GET", CLINICAL_TRIALS_COUNT_END_POINT);
        clinicalTrialsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject clinicalTrialsCountResult = esService.send(clinicalTrialsCountRequest);
        int numberOfClinicalTrials = clinicalTrialsCountResult.get("count").getAsInt();

        Request patentsCountRequest = new Request("GET", PATENTS_COUNT_END_POINT);
        patentsCountRequest.setJsonEntity(gson.toJson(query));
        JsonObject patentsCountResult = esService.send(patentsCountRequest);
        int numberOfPatents = patentsCountResult.get("count").getAsInt();

        // Get aggregations
        Map<String, Object> projectAggQuery = esService.addAggregations(query, PROJECTS_TERM_AGG_NAMES, new String[]{});
        Request projectRequest = new Request("GET", PROJECTS_END_POINT);
        projectRequest.setJsonEntity(gson.toJson(projectAggQuery));
        JsonObject projectResult = esService.send(projectRequest);
        Map<String, List<Map<String, Object>>> projectAggs = esService.collectTermAggs(projectResult, PROJECTS_TERM_AGG_NAMES);

        Map<String, Object> publicationAggQuery = esService.addAggregations(query, PUBLICATIONS_TERM_AGG_NAMES, new String[]{});
        Request publicationRequest = new Request("GET", PUBLICATIONS_END_POINT);
        publicationRequest.setJsonEntity(gson.toJson(publicationAggQuery));
        JsonObject publicationResult = esService.send(publicationRequest);
        Map<String, List<Map<String, Object>>> publicationAggs = esService.collectTermAggs(publicationResult, PUBLICATIONS_TERM_AGG_NAMES);

        Map<String, Object> data = new HashMap<>();
        data.put("filterProjectCountByDOC", projectAggs.get("lead_doc"));
        data.put("filterProjectCountByFiscalYear", projectAggs.get("fiscal_year"));
        data.put("filterProjectCountByAwardAmount", projectAggs.get("award_amount_category"));
        data.put("filterProjectCountByProgram", projectAggs.get("program_id"));
        data.put("numberOfPrograms", numberOfPrograms);
        data.put("numberOfProjects", numberOfProjects);
        data.put("numberOfPublications", numberOfPublications);
        data.put("numberOfDatasets", numberOfDatasets);
        data.put("numberOfClinicalTrials", numberOfClinicalTrials);
        data.put("numberOfPatents", numberOfPatents);
        data.put("projectCountByDOC", projectAggs.get("lead_doc"));
        data.put("projectCountByFiscalYear", projectAggs.get("fiscal_year"));
        data.put("projectCountByAwardAmount", projectAggs.get("award_amount_category"));
        data.put("projectCountByProgram", projectAggs.get("program_id"));
        data.put("publicationCountByCitation", publicationAggs.get("citation_count_category"));
        data.put("publicationCountByRCR", publicationAggs.get("rcr_range"));
        data.put("publicationCountByYear", publicationAggs.get("year"));

        // data.put("armsByPrograms", armsByPrograms(params));
        // widgets data and facet filter counts
        // for (var agg: TERM_AGGS) {
        //     String field = agg.get(AGG_NAME);
        //     String widgetQueryName = agg.get(WIDGET_QUERY);
        //     String filterCountQueryName = agg.get(FILTER_COUNT_QUERY);
        //     String endpoint = agg.get(AGG_ENDPOINT);
        //     // subjectCountByXXXX
        //     List<Map<String, Object>> widgetData;
        //     if (endpoint.equals(SUBJECTS_END_POINT)) {
        //         widgetData = getGroupCountHelper(aggs.get(field));
        //         data.put(widgetQueryName, widgetData);
        //     } else {
        //         widgetData = subjectCountBy(field, params, endpoint);;
        //         data.put(widgetQueryName, widgetData);
        //     }
        //     // filterSubjectCountByXXXX
        //     if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
        //         List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint);;
        //         data.put(filterCountQueryName, filterCount);
        //     } else {
        //         data.put(filterCountQueryName, widgetData);
        //     }
        // }

        // Map<String, JsonObject> rangeAggs = esService.collectRangeAggs(subjectResult, RANGE_AGG_NAMES);

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

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("patent_id", "patent_id"),
                Map.entry("fulfilled_date", "fulfilled_date"),
                Map.entry("queried_project_ids", "queried_project_ids")
        );

        return overview(PATENTS_END_POINT, params, PROPERTIES, defaultSort, mapping);
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

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("clinical_trial_id", "clinical_trial_id"),
                Map.entry("title", "title"),
                Map.entry("last_update_posted", "last_update_posted"),
                Map.entry("recruitment_status", "recruitment_status"),
                Map.entry("queried_project_ids", "queried_project_ids")
        );

        return overview(CLINICAL_TRIALS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }
    
    private List<Map<String, Object>> datasetOverView(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"type", "type"},
                new String[]{"accession", "accession"},
                new String[]{"title", "title"},
                new String[]{"release_date", "registration_date"},
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

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("type", "type"),
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

        return overview(DATASETS_END_POINT, params, PROPERTIES, defaultSort, mapping);
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

        return overview(PUBLICATIONS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }
    
    private List<Map<String, Object>> projectOverView(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"program", "program"},
            new String[]{"project_id", "project_id"},
            new String[]{"application_id", "application_id"},
            new String[]{"fiscal_year", "fiscal_year"},
            new String[]{"project_title", "project_title"},
            new String[]{"project_type", "project_type"},
            new String[]{"abstract_text", "abstract_text"},
            new String[]{"keywords", "keywords"},
            new String[]{"org_name", "org_name"},
            new String[]{"org_city", "org_citie"},
            new String[]{"org_state", "org_state"},
            new String[]{"org_country", "org_country"},
            new String[]{"principal_investigators", "principal_investigators"},
            new String[]{"lead_doc", "lead_doc"},
            new String[]{"program_officers", "program_officers"},
            new String[]{"award_amount", "award_amount"},
            new String[]{"nci_funded_amount", "nci_funded_amount"},
            new String[]{"award_notice_date", "award_notice_date"},
            new String[]{"project_start_date", "project_start_date"},
            new String[]{"project_end_date", "project_end_date"},
            new String[]{"full_foa", "full_foa"},
        };

        String defaultSort = "project_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("program", "program"),        
                Map.entry("project_id", "project_id"),
                Map.entry("application_id", "application_id"),
                Map.entry("fiscal_year", "fiscal_year"),
                Map.entry("project_title", "project_title"),
                Map.entry("project_type", "project_type"),
                Map.entry("abstract_text", "abstract_text"),
                Map.entry("keywords", "keywords"),
                Map.entry("org_name", "org_name"),
                Map.entry("org_city", "org_city"),
                Map.entry("org_state", "org_state"),
                Map.entry("org_country", "org_country"),
                Map.entry("principal_investigators", "principal_investigators"),
                Map.entry("lead_doc", "lead_doc"),
                Map.entry("program_officers", "program_officers"),
                Map.entry("award_amount", "award_amount"),
                Map.entry("nci_funded_amount", "nci_funded_amount"),
                Map.entry("award_notice_date", "award_notice_date"),
                Map.entry("project_start_date", "project_start_date"),
                Map.entry("project_end_date", "project_end_date"),
                Map.entry("full_foa", "full_foa")
        );

        return overview(PROJECTS_END_POINT, params, PROPERTIES, defaultSort, mapping);
    }
}
