package com.walmart.qarth.kafka.file;


import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.client.ResponseHandler;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Created by vgodbol on 4/25/16.
 */
public class HttpGetResource {
    private static final Logger log = LoggerFactory.getLogger(HttpGetResource.class);

    public static void main(String args[]) {

        String resp = new HttpGetResource().http("http://10.187.239.194:8000", "" );
        System.out.println(resp);
    }


    public String http(String url, String body) {
        //try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
        try {
            HttpClient client = new DefaultHttpClient();
            //HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url);
            try {
                HttpResponse response = client.execute(request);
                System.out.println(response.getStatusLine());
                ResponseHandler<String> handler = new BasicResponseHandler();
                String body1 = handler.handleResponse(response);
                return body1;
            } catch (Exception e) {
                e.printStackTrace();

            }


            /*HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url);

            //HttpPost request = new HttpPost(url);
            //HttpGet request = new HttpGet(url);
            //StringEntity params = new StringEntity(body);
            request.addHeader("content-type", "application/json");
            //request.setEntity(params);
            HttpResponse result = client.execute(request);

            return result;*/

            /*String json = EntityUtils.toString(result.getEntity(), "UTF-8");
            try {
                JSONParser parser = new JSONParser();
                Object resultObject = parser.parse(json);

                if (resultObject instanceof JSONArray) {
                    JSONArray array=(JSONArray)resultObject;
                    for (Object object : array) {
                        JSONObject obj =(JSONObject)object;
                        System.out.println(obj.get("example"));
                        System.out.println(obj.get("fr"));
                    }

                }else if (resultObject instanceof JSONObject) {
                    JSONObject obj =(JSONObject)resultObject;
                    System.out.println(obj.get("example"));
                    System.out.println(obj.get("fr"));
                }

            } catch (Exception e) {
                // TODO: handle exception
            }*/

        } catch (Exception ex) {
            log.error("HttpGetResource" ,ex);
        }
        return null;
    }
}
