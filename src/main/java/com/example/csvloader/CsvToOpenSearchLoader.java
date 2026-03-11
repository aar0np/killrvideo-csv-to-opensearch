package com.example.csvloader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;

import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvToOpenSearchLoader {

    private final String opensearchHost;
    private final int opensearchPort;
    private final String opensearchScheme;
    private final String username;
    private final String password;
    private final String indexName;

    public CsvToOpenSearchLoader(String indexName) {
        this.opensearchHost = System.getenv("OPENSEARCH_HOST");
        this.opensearchScheme = "https";

        this.username = System.getenv("OPENSEARCH_USERNAME");
        this.password = System.getenv("OPENSEARCH_PASSWORD");

        this.indexName = indexName;

        if (System.getenv("OPENSEARCH_PORT") != null) {
        	this.opensearchPort = Integer.parseInt(System.getenv("OPENSEARCH_PORT"));
        } else {
        	this.opensearchPort = 9200;
        }
    }

    private OpenSearchClient createClient() throws Exception {

    	final HttpHost host = new HttpHost(opensearchScheme, opensearchHost, opensearchPort);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final var sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build();
        
        credentialsProvider.setCredentials(
            new AuthScope(host),
            new UsernamePasswordCredentials(username, password.toCharArray())
        );

        final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
        		.builder(host)
        		.setMapper(new JacksonJsonpMapper())
                .setHttpClientConfigCallback(httpClientBuilder -> {

                    // Disable SSL/TLS verification as our local testing clusters use self-signed certificates
                    final var tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setSslContext(sslContext)
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .build();

                    final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();

                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                })
        		.build();

        return new OpenSearchClient(transport);
    }
    
    /**
     * Loads CSV file into OpenSearch.
     * 
     * @param csvFilePath Path to the CSV file
     * @throws IOException If file reading or indexing fails
     */
    public void loadCsvToOpenSearch(String csvFilePath) throws IOException {
        System.out.printf("Starting CSV load from: %s\n", csvFilePath);
        int recordCount = 0;
        
        try  {
        	OpenSearchClient client = createClient();
            Reader reader = new FileReader(csvFilePath);
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                 .setHeader()
                 .setSkipHeaderRecord(true)
                 .build();
            
            CSVParser csvParser = csvFormat.parse(reader);
            List<String> uniqueList = new ArrayList<>();
            
            
            for (CSVRecord csvRecord : csvParser) {
                Map<String, Object> document = new HashMap<>();
                
                // Convert CSV record to a map
                csvRecord.toMap().forEach(document::put);
                
                if (document.containsKey("name")) {

                	String name = document.get("name").toString();
                	
                	// don't add the same video twice
                	if (!uniqueList.contains(name)) {
		                // Create index request
		                IndexRequest indexRequest = new IndexRequest
		                		.Builder()
		                		.index(indexName)
		                		.document(document)
		                		.build();
		                
		                client.index(indexRequest);
		                
		                uniqueList.add(name);
		                recordCount++;
                	}
                }
            }
                        
            System.out.printf("Successfully indexed %d total records into index '%s'\n", recordCount, indexName);
            
        } catch (Exception e) {
            System.out.printf("Error loading CSV to OpenSearch: %s\n", e);
            throw new IOException("Failed to load CSV to OpenSearch", e);
        }
    }
    
    /**
     * Main entry point.
     * 
     * Usage: java -jar csv-to-opensearch.jar <csv-file-name>
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -jar csv-to-opensearch.jar <csv-file-path> [config-file]");
            System.err.println("  csv-file-path: Path to the CSV file to load");
            System.exit(1);
        }

        String csvFile = args[0];
        int dotPos = csvFile.indexOf('.');
        String indexName = csvFile.substring(0, dotPos);

        try {
            CsvToOpenSearchLoader loader = new CsvToOpenSearchLoader(indexName);
            loader.loadCsvToOpenSearch("data/" + csvFile);

            System.out.println("CSV load completed successfully!");

        } catch (Exception e) {
        	System.out.printf("Failed to load CSV: %s\n", e);
            System.exit(1);
        }
    }
}
