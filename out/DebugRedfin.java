import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class DebugRedfin {
  public static void main(String[] args) throws Exception {
    String u = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz";
    HttpClient c = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    HttpRequest req = HttpRequest.newBuilder().uri(URI.create(u)).timeout(Duration.ofMinutes(5)).GET().build();
    HttpResponse<InputStream> r = c.send(req, HttpResponse.BodyHandlers.ofInputStream());
    try (GZIPInputStream gis = new GZIPInputStream(r.body()); BufferedReader br = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8))) {
      String[] h = br.readLine().split("\\t", -1);
      Map<String,Integer> idx = new HashMap<>();
      for (int i=0;i<h.length;i++) idx.put(h[i].replace("\"", ""), i);
      int iState = idx.get("STATE_CODE");
      int iRegion = idx.get("REGION");
      int iPType = idx.get("PROPERTY_TYPE");
      int iEnd = idx.get("PERIOD_END");
      int iPrice = idx.get("MEDIAN_SALE_PRICE");
      int iDur = idx.get("PERIOD_DURATION");
      int wa=0;
      int sno=0;
      String line;
      while ((line = br.readLine()) != null) {
        String[] p = line.split("\\t", -1);
        String st = p[iState].replace("\"", "");
        String region = p[iRegion].replace("\"", "");
        if ("WA".equals(st)) {
          wa++;
          if (wa <= 3) {
            System.out.println("WA sample: " + region + " | " + p[iPType] + " | " + p[iEnd] + " | " + p[iPrice]);
          }
        }
        if (region.contains("Snohomish")) {
          sno++;
          System.out.println("SNO: " + region + " | st=" + st + " | type=" + p[iPType] + " | dur=" + p[iDur] + " | end=" + p[iEnd] + " | price=" + p[iPrice]);
        }
      }
      System.out.println("wa rows=" + wa + " sno rows=" + sno);
    }
  }
}
