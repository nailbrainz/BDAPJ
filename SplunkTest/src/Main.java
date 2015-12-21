import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.splunk.*; 


public class Main {
	public static void main(String[] args) throws Exception {

        // Create a map of arguments and add login parameters
		
		HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);

        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername("admin");
        loginArgs.setPassword("bigdata");
        loginArgs.setHost("163.180.116.150");
        loginArgs.setPort(8089);
        
        // Create a Service instance and log in with the argument map
        Service service = Service.connect(loginArgs);
        IndexCollectionArgs indexcollArgs = new IndexCollectionArgs();
        indexcollArgs.setSortKey("totalEventCount");
        indexcollArgs.setSortDirection(IndexCollectionArgs.SortDirection.DESC);
        IndexCollection myIndexes = service.getIndexes(indexcollArgs);

        // List the indexes and their event counts
        System.out.println("There are " + myIndexes.size() + " indexes:\n");
        for (Index entity: myIndexes.values()) {
            System.out.println("  " + entity.getName() + " (events: " 
                    + entity.getTotalEventCount() + ")");
        }
        
        
        /*
        Index myIndex = service.getIndexes().get("test_index");
        
        TcpInput myInput = (TcpInput)service.getInputs().get("12121");
        myInput.setIndex("test_index");
        // Open a socket
        Socket socket = myInput.attach();
        try {
            OutputStream ostream = socket.getOutputStream();
            Writer out = new OutputStreamWriter(ostream, "UTF8");

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
            String date = dateFormat.format(new Date());

            // Send events to the socket then close it
            out.write("X=1\tY=2\tZ=3");
            out.flush();
            System.out.println("What");
       } finally {
            socket.close();
       }*/
        
        String searchQuery_blocking = "search index=\"test_index\" | stats avg(X)"; // Return the first 100 events
        JobArgs jobargs = new JobArgs();
        jobargs.setExecutionMode(JobArgs.ExecutionMode.BLOCKING);

        // A blocking search returns the job when the search is done
        System.out.println("Wait for the search to finish...");
        Job job = service.getJobs().create(searchQuery_blocking, jobargs);
        System.out.println("...done!\n");

        // Get properties of the job
        System.out.println("Search job properties:\n---------------------");
        System.out.println("Search job ID:         " + job.getSid());
        System.out.println("The number of events:  " + job.getEventCount());
        System.out.println("The number of results: " + job.getResultCount());
        System.out.println("Search duration:       " + job.getRunDuration() + " seconds");
        System.out.println("This job expires in:   " + job.getTtl() + " seconds");        
        System.out.println("This job expires in:   " + job.getResults());      
        StringBuffer sb = new StringBuffer();
        InputStream inputStream = job.getResults();
        byte[] b = new byte[4096];
        for (int n; (n = inputStream.read(b)) != -1;) {
            sb.append(new String(b, 0, n));
        }
        
     System.out.println(sb.toString());;
        /*
        UserCollection users = service.getUsers();
        String lastUser = null;
        for (User user : users.values()) {
            lastUser = user.getName();
            System.out.println(lastUser);
        }
        
        
        SavedSearch savedSearch = service.getSavedSearches().get("Test Search");
        System.out.println("Run the '" + savedSearch.getName() + "' search ("
                + savedSearch.getSearch() + ")\n");
        Job jobSavedSearch = null;

        // Run the saved search
        try {
            jobSavedSearch = savedSearch.dispatch();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        System.out.println("Waiting for the job to finish...\n");

        // Wait for the job to finish
        while (!jobSavedSearch.isDone()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        JobResultsArgs resultsArgs = new JobResultsArgs();
        resultsArgs.setOutputMode(JobResultsArgs.OutputMode.JSON);

        // Display results in JSON using ResultsReaderJson
        InputStream results = jobSavedSearch.getResults(resultsArgs);
        ResultsReaderJson resultsReader = new ResultsReaderJson(results);
        HashMap<String, String> event;
        System.out.println("\nFormatted results from the search job as JSON\n");
        while ((event = resultsReader.getNextEvent()) != null) {
            for (String key: event.keySet())
                System.out.println("   " + key + ":  " + event.get(key));
        }
        resultsReader.close();
        
        */
	}
}
