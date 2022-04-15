package com.wong.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wong.elastic.pojo.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ElasticApplicationTests {

	@Autowired
	public RestHighLevelClient restHighLevelClient;

	ObjectMapper mapper = new ObjectMapper();

	// Index
	// GET user_index/
	@Test
	public void testCreateIndex() throws IOException {
		CreateIndexRequest request = new CreateIndexRequest("user_index");
		CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
		System.out.println(response.isAcknowledged());
		System.out.println(response);
		restHighLevelClient.close();
	}

	@Test
	public void testIndexIsExists() throws IOException {
		GetIndexRequest request = new GetIndexRequest("index");
		boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
		restHighLevelClient.close();
	}

	@Test
	public void testDeleteIndex() throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("user_index");
		AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(response.isAcknowledged());
		restHighLevelClient.close();
	}

	// document
	// GET user_index/
	// GET user_index/_doc/1
	@Test
	public void testAddDocument() throws IOException {
		User user = new User("tina", 18);
		IndexRequest request = new IndexRequest("user_index");
		// PUT /user_index/_doc/1
		request.id("1");
		request.timeout(TimeValue.timeValueMillis(1000));// request.timeout("1s")
		String jsonString = mapper.writeValueAsString(user);
		request.source(jsonString, XContentType.JSON);
		IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
		System.out.println(response.status());// get index status :  CREATED
		System.out.println(response);
	}

	@Test
	public void testGetDocument() throws IOException {
		GetRequest request = new GetRequest("user_index","1");
		GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
		System.out.println(response.getSourceAsString());
		System.out.println(request);
		restHighLevelClient.close();
	}

	@Test
	public void testDocumentIsExists() throws IOException {
		GetRequest request = new GetRequest("user_index", "1");
		// do not get _source from return data
		request.fetchSourceContext(new FetchSourceContext(false));
		request.storedFields("_none_");
		boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);

		System.out.println("123456".substring(0,0));
		System.out.println("123456".substring(1));
	}

//	PUT user_index/_doc/1/_update
//	{
//		"name": "tina",
//			"age" : "33"
//	}
	@Test
	public void testUpdateDocument() throws IOException {
		UpdateRequest request = new UpdateRequest("user_index","1");
		User user = new User("atom",11);
		String jsonString = mapper.writeValueAsString(user);
		request.doc(jsonString, XContentType.JSON);
//		Map<String, Object> map = new HashMap<>();
//		map.put("name", user.getName());
//		map.put("age", user.getAge());
//		request.doc(jsonString, XContentType.JSON);
		UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
		System.out.println(response.status()); // OK
		restHighLevelClient.close();
	}

	//	DELETE user_index/_doc/1
	@Test
	public void testDeleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("user_index", "1");
		request.timeout("1s");
		DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
		System.out.println(response.status());// OK
	}


// SearchRequest
// SearchSourceBuilder requirement builder
// HighlightBuilder
// TermQueryBuilder pricise search
// MatchAllQueryBuilder
// xxxQueryBuilder ...
	@Test
	public void testSearch() throws IOException {

		SearchRequest searchRequest = new SearchRequest();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "tina");

		// MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

		// highlight keyword
		searchSourceBuilder.highlighter(new HighlightBuilder());
		// page searching
		// searchSourceBuilder.from();
		// searchSourceBuilder.size();
		searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

		searchSourceBuilder.query(termQueryBuilder);

		// add search requirement to request
		searchRequest.source(searchSourceBuilder);

		// client search
		SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

		// return result
		SearchHits hits = search.getHits();
		System.out.println(mapper.writeValueAsString(hits));
		System.out.println("=======================");
		for (SearchHit documentFields : hits.getHits()) {
			System.out.println(documentFields.getSourceAsMap());
		}
	}

	@Test
	public void test() throws IOException {
		IndexRequest request = new IndexRequest("bulk");// random id will auto-create if id is not specified
		request.source(mapper.writeValueAsString(new User("lion",1)),XContentType.JSON);
		request.source(mapper.writeValueAsString(new User("lisa",2)),XContentType.JSON);
		request.source(mapper.writeValueAsString(new User("tonny",3)),XContentType.JSON); // only last is created
。	IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
		System.out.println(index.status());// created
	}

/*
	GET /bulk/_search
	{
		"query": {
			"match": {
				"age": "3"
			}
		}
	}
*/
	@Test
	public void testBulk() throws IOException {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");

		ArrayList<User> users = new ArrayList<>();
		users.add(new User("tina-1",1));
		users.add(new User("tina-2",2));
		users.add(new User("tina-3",3));
		users.add(new User("tina-4",4));
		users.add(new User("tina-5",5));
		users.add(new User("tina-6",6));

		// batch request
		for (int i = 0; i < users.size(); i++) {
			bulkRequest.add(
					new IndexRequest("bulk")
							.id(""+(i + 1)) // random id will auto-create if id is not specified
							.source(mapper.writeValueAsString(users.get(i)),XContentType.JSON)
			);
		}

		BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulk.status());// ok
	}
}
